// Package realtime transports committed farm patches between farmsvr and all
// gatesvr instances. Redis Pub/Sub is intentionally best effort; farm_version
// and Snapshot recovery, not Redis durability, preserve correctness.
package realtime

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/photon/farm-server/server/internal/farm/application"
	"github.com/photon/farm-server/server/internal/farm/domain"
	"github.com/photon/farm-server/server/pkg/observability"
	"github.com/redis/go-redis/v9"
)

const controlChannel = "farm:events:control"

type Message struct {
	FarmID      int64              `json:"farm_id"`
	EventID     string             `json:"event_id"`
	FarmVersion int64              `json:"farm_version"`
	Patch       domain.Patch       `json:"patch"`
	ActorUserID int64              `json:"actor_user_id"`
	CommandType domain.CommandType `json:"command_type,omitempty"`
	TraceID     string             `json:"trace_id,omitempty"`
}

func Channel(farmID int64) string { return "farm:events:" + strconv.FormatInt(farmID, 10) }

type Publisher struct{ rdb *redis.Client }

func NewPublisher(rdb *redis.Client) *Publisher { return &Publisher{rdb: rdb} }
func (p *Publisher) Publish(ctx context.Context, message Message) error {
	raw, err := json.Marshal(message)
	if err != nil {
		return err
	}
	return p.rdb.Publish(ctx, Channel(message.FarmID), raw).Err()
}

type EventPublisher interface {
	Publish(context.Context, Message) error
}

// Subscriber owns exactly one Pub/Sub connection per gatesvr and dynamically
// subscribes on first local viewer, with delayed unsubscribe after the last.
type Subscriber struct {
	rdb       *redis.Client
	delay     time.Duration
	mu        sync.Mutex
	pubsub    *redis.PubSub
	started   bool
	refs      map[int64]int
	timers    map[int64]*time.Timer
	onMessage func(Message)
	observer  Observer
}

type Observer interface {
	FarmPubSub(direction, result string)
}

func (s *Subscriber) WithObserver(observer Observer) *Subscriber { s.observer = observer; return s }

func NewSubscriber(rdb *redis.Client, delay time.Duration) *Subscriber {
	return &Subscriber{rdb: rdb, delay: delay, refs: make(map[int64]int), timers: make(map[int64]*time.Timer)}
}

func (s *Subscriber) Start(ctx context.Context, onMessage func(Message)) error {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return nil
	}
	s.started = true
	s.onMessage = onMessage
	s.mu.Unlock()
	pubsub, err := s.open(ctx)
	if err != nil {
		s.mu.Lock()
		s.started = false
		s.mu.Unlock()
		return fmt.Errorf("start farm event subscription: %w", err)
	}
	go s.run(ctx, pubsub)
	return nil
}

func (s *Subscriber) channelsLocked() []string {
	channels := make([]string, 0, len(s.refs)+1)
	channels = append(channels, controlChannel)
	for farmID, refs := range s.refs {
		if refs > 0 {
			channels = append(channels, Channel(farmID))
		}
	}
	return channels
}

func (s *Subscriber) open(ctx context.Context) (*redis.PubSub, error) {
	// Install the connection under the same lock used by Subscribe. A viewer
	// arriving before this point is included in channelsLocked; one arriving
	// after it dynamically subscribes on this newly installed connection.
	s.mu.Lock()
	pubsub := s.rdb.Subscribe(ctx, s.channelsLocked()...)
	s.pubsub = pubsub
	s.mu.Unlock()
	if _, err := pubsub.Receive(ctx); err != nil {
		s.mu.Lock()
		if s.pubsub == pubsub {
			s.pubsub = nil
		}
		s.mu.Unlock()
		_ = pubsub.Close()
		return nil, err
	}
	return pubsub, nil
}

func (s *Subscriber) run(ctx context.Context, pubsub *redis.PubSub) {
	defer func() {
		_ = pubsub.Close()
		s.mu.Lock()
		if s.pubsub == pubsub {
			s.pubsub = nil
		}
		s.started = false
		s.mu.Unlock()
	}()
	for {
		msg, err := pubsub.ReceiveMessage(ctx)
		if err == nil {
			s.handleMessage(msg)
			continue
		}
		if ctx.Err() != nil {
			return
		}
		_ = pubsub.Close()
		s.mu.Lock()
		if s.pubsub == pubsub {
			s.pubsub = nil
		}
		s.mu.Unlock()
		if s.observer != nil {
			s.observer.FarmPubSub("subscribe", "reconnect")
		}
		for ctx.Err() == nil {
			next, reconnectErr := s.open(ctx)
			if reconnectErr == nil {
				pubsub = next
				break
			}
			if s.observer != nil {
				s.observer.FarmPubSub("subscribe", "reconnect_error")
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
}

func (s *Subscriber) handleMessage(msg *redis.Message) {
	if msg == nil || msg.Channel == controlChannel {
		return
	}
	var event Message
	if json.Unmarshal([]byte(msg.Payload), &event) == nil && s.onMessage != nil {
		if s.observer != nil {
			s.observer.FarmPubSub("subscribe", "ok")
		}
		s.onMessage(event)
	} else if s.observer != nil {
		s.observer.FarmPubSub("subscribe", "invalid")
	}
}

func (s *Subscriber) Subscribe(farmID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if timer := s.timers[farmID]; timer != nil {
		timer.Stop()
		delete(s.timers, farmID)
	}
	s.refs[farmID]++
	if s.refs[farmID] == 1 && s.pubsub != nil {
		_ = s.pubsub.Subscribe(context.Background(), Channel(farmID))
	}
}

func (s *Subscriber) Unsubscribe(farmID int64) {
	s.mu.Lock()
	if s.refs[farmID] > 1 {
		s.refs[farmID]--
		s.mu.Unlock()
		return
	}
	delete(s.refs, farmID)
	if s.timers[farmID] != nil {
		s.mu.Unlock()
		return
	}
	delay := s.delay
	if delay <= 0 {
		delay = 5 * time.Second
	}
	s.timers[farmID] = time.AfterFunc(delay, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		delete(s.timers, farmID)
		if s.refs[farmID] == 0 && s.pubsub != nil {
			_ = s.pubsub.Unsubscribe(context.Background(), Channel(farmID))
		}
	})
	s.mu.Unlock()
}

type CommandSubmitter interface {
	Submit(context.Context, domain.Command) (application.CommitResult, error)
}

// PublishingSubmitter publishes only after the authoritative commit succeeds.
// Publish failure is logged and never changes the successful command result.
type PublishingSubmitter struct {
	next      CommandSubmitter
	publisher EventPublisher
	log       *slog.Logger
	observer  Observer
}

func (s *PublishingSubmitter) WithObserver(observer Observer) *PublishingSubmitter {
	s.observer = observer
	return s
}

func NewPublishingSubmitter(next CommandSubmitter, publisher EventPublisher, log *slog.Logger) *PublishingSubmitter {
	return &PublishingSubmitter{next: next, publisher: publisher, log: log}
}
func (s *PublishingSubmitter) Submit(ctx context.Context, cmd domain.Command) (application.CommitResult, error) {
	result, err := s.next.Submit(ctx, cmd)
	if err != nil || result.Replayed {
		return result, err
	}
	message := Message{FarmID: cmd.FarmID, EventID: result.EventID, FarmVersion: result.NewVersion, Patch: result.Patch, ActorUserID: cmd.ActorUser, CommandType: cmd.Type, TraceID: observability.TraceID(ctx)}
	publishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 100*time.Millisecond)
	defer cancel()
	if err = s.publisher.Publish(publishCtx, message); err != nil {
		if s.observer != nil {
			s.observer.FarmPubSub("publish", "error")
		}
		s.log.Warn("farm patch publish failed", slog.Int64("farm_id", cmd.FarmID), slog.String("event_id", result.EventID), slog.String("error", err.Error()))
	}
	if err == nil && s.observer != nil {
		s.observer.FarmPubSub("publish", "ok")
	}
	return result, nil
}
