// Package realtime delivers best-effort mailbox badge invalidations to the
// gatesvr instance currently owning a user's WebSocket connection.
package realtime

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	maildomain "github.com/photon/farm-server/server/internal/mail/domain"
	"github.com/redis/go-redis/v9"
)

const channelPrefix = "mailbox:changed:instance:"

type Message struct {
	UserID      int64 `json:"user_id"`
	UnreadCount int64 `json:"unread_count"`
	Version     int64 `json:"version"`
}

func Channel(instanceID string) string { return channelPrefix + instanceID }

type Observer interface {
	MailboxPublishFailure()
}

// Publisher resolves the user's current gatesvr from the existing ConnStore
// key. Offline users need no durable notification because MySQL is authoritative.
type Publisher struct {
	rdb      *redis.Client
	observer Observer
}

func NewPublisher(rdb *redis.Client) *Publisher { return &Publisher{rdb: rdb} }

func (p *Publisher) WithObserver(observer Observer) *Publisher {
	p.observer = observer
	return p
}

func (p *Publisher) failed(err error) error {
	if p.observer != nil {
		p.observer.MailboxPublishFailure()
	}
	return err
}

func (p *Publisher) NotifyMailboxChanged(ctx context.Context, summary maildomain.MailboxSummary) error {
	target, err := p.rdb.Get(ctx, "ws:conn:"+strconv.FormatInt(summary.UserID, 10)).Result()
	if err == redis.Nil {
		return nil
	}
	if err != nil {
		return p.failed(fmt.Errorf("resolve mailbox websocket target: %w", err))
	}
	instanceID, _, ok := strings.Cut(target, ":")
	if !ok || instanceID == "" {
		return nil
	}
	raw, err := json.Marshal(Message{UserID: summary.UserID, UnreadCount: summary.UnreadCount, Version: summary.Version})
	if err != nil {
		return p.failed(err)
	}
	if err := p.rdb.Publish(ctx, Channel(instanceID), raw).Err(); err != nil {
		return p.failed(err)
	}
	return nil
}

type Subscriber struct {
	rdb        *redis.Client
	instanceID string
}

func NewSubscriber(rdb *redis.Client, instanceID string) *Subscriber {
	return &Subscriber{rdb: rdb, instanceID: instanceID}
}

// Start installs one shared subscription per gatesvr instance. It reconnects
// after transient Redis failures; clients reconcile summary after any gap.
func (s *Subscriber) Start(ctx context.Context, onMessage func(Message)) error {
	pubsub, err := s.open(ctx)
	if err != nil {
		return err
	}
	go s.run(ctx, pubsub, onMessage)
	return nil
}

func (s *Subscriber) open(ctx context.Context) (*redis.PubSub, error) {
	pubsub := s.rdb.Subscribe(ctx, Channel(s.instanceID))
	if _, err := pubsub.Receive(ctx); err != nil {
		_ = pubsub.Close()
		return nil, fmt.Errorf("subscribe mailbox changes: %w", err)
	}
	return pubsub, nil
}

func (s *Subscriber) run(ctx context.Context, pubsub *redis.PubSub, onMessage func(Message)) {
	defer func() { _ = pubsub.Close() }()
	for ctx.Err() == nil {
		msg, err := pubsub.ReceiveMessage(ctx)
		if err == nil {
			var event Message
			if json.Unmarshal([]byte(msg.Payload), &event) == nil && event.UserID > 0 && onMessage != nil {
				onMessage(event)
			}
			continue
		}
		if ctx.Err() != nil {
			return
		}
		_ = pubsub.Close()
		for ctx.Err() == nil {
			next, openErr := s.open(ctx)
			if openErr == nil {
				pubsub = next
				break
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
}

var _ maildomain.MailboxNotifier = (*Publisher)(nil)
