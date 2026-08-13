// Package infrastructure 提供幂等 Kafka 事件投影的 EventConsumer。
// EventProjector 是端口接口；LogProjector 是 P0 日志实现。
// KafkaConsumer 使用 kafka-go kafka.Reader 实现 at-least-once 消费 + ConsumerDedup 幂等。
package infrastructure

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	kafka "github.com/segmentio/kafka-go"

	farmv1 "github.com/photon/farm-server/server/contracts/events/farmv1"
	"github.com/photon/farm-server/server/pkg/clock"
	"github.com/photon/farm-server/server/pkg/observability"
)

// ── EventProjector 端口 ───────────────────────────────────────────────────────

// EventProjector 处理单个已反序列化的农场领域事件。
// 实现必须幂等：消费者仅对每个 event_id+consumer_name 调用一次 Handle
// （由 consumed_events 去重表保证）。
type EventProjector interface {
	// Name 返回用于去重的稳定消费者标识（如 "stats-projector"）。
	Name() string
	// Handle 处理事件。返回错误会导致事件重新排队。
	Handle(ctx context.Context, env farmv1.EventEnvelope) error
}

// ── LogProjector ─────────────────────────────────────────────────────────────

// LogProjector 是 P0 EventProjector 实现：仅记录每条事件日志。
// 在路线 6 中替换或补充为真实 Projector（如 AnalyticsProjector）。
type LogProjector struct {
	log *slog.Logger
}

// NewLogProjector 构造一个 LogProjector。
func NewLogProjector(log *slog.Logger) *LogProjector {
	return &LogProjector{log: log}
}

func (p *LogProjector) Name() string { return "log-projector" }

func (p *LogProjector) Handle(_ context.Context, env farmv1.EventEnvelope) error {
	p.log.Info("farm event received",
		slog.String("trace_id", env.TraceID),
		slog.String("event_id", env.EventID),
		slog.String("correlation_id", env.CorrelationID),
		slog.String("causation_id", env.CausationID),
		slog.String("event_type", string(env.EventType)),
		slog.String("aggregate_id", env.AggregateID),
		slog.Time("occurred_at", env.OccurredAt),
	)
	return nil
}

var _ EventProjector = (*LogProjector)(nil)

// ── ConsumerDedup ─────────────────────────────────────────────────────────────

// ConsumerDedup 使用 consumed_events 表检查和记录事件消费。
// 强制执行 event_id + consumer_name 幂等保证（见 CONSTITUTION §1.4）。
type ConsumerDedup struct {
	db *sql.DB
}

// ErrInvalidEventEnvelope marks a permanently malformed message. Retrying the
// same bytes cannot repair invalid JSON or a missing event identity.
var ErrInvalidEventEnvelope = errors.New("invalid event envelope")

// NewConsumerDedup 使用给定 DB 构造 ConsumerDedup。
func NewConsumerDedup(db *sql.DB) *ConsumerDedup {
	return &ConsumerDedup{db: db}
}

// ProcessOnce 仅在 (eventID, consumerName) 未见过时调用 projector.Handle。
// 成功后记录到 consumed_events 表。
func (d *ConsumerDedup) ProcessOnce(ctx context.Context, raw []byte, projector EventProjector, now time.Time) error {
	var env farmv1.EventEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return fmt.Errorf("%w: unmarshal: %v", ErrInvalidEventEnvelope, err)
	}
	if env.EventID == "" {
		return fmt.Errorf("%w: event_id is required", ErrInvalidEventEnvelope)
	}

	seen, err := d.alreadyConsumed(ctx, env.EventID, projector.Name())
	if err != nil {
		return fmt.Errorf("check_dedup: %w", err)
	}
	if seen {
		return nil // 幂等跳过
	}

	if err := projector.Handle(ctx, env); err != nil {
		return fmt.Errorf("projector_handle: %w", err)
	}

	return d.markConsumed(ctx, env.EventID, projector.Name(), now)
}

func (d *ConsumerDedup) alreadyConsumed(ctx context.Context, eventID, consumerName string) (bool, error) {
	const q = `SELECT 1 FROM consumed_events WHERE event_id = ? AND consumer_name = ? LIMIT 1`
	var dummy int
	err := d.db.QueryRowContext(ctx, q, eventID, consumerName).Scan(&dummy)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (d *ConsumerDedup) markConsumed(ctx context.Context, eventID, consumerName string, now time.Time) error {
	const q = `INSERT INTO consumed_events (event_id, consumer_name, processed_at) VALUES (?, ?, ?)`
	_, err := d.db.ExecContext(ctx, q, eventID, consumerName, now)
	return err
}

// ── KafkaConsumer ─────────────────────────────────────────────────────────────

// KafkaConsumer 订阅 Kafka topic，对每条消息执行 ConsumerDedup.ProcessOnce，
// 实现 at-least-once 消费 + 幂等投影语义。
type KafkaConsumer struct {
	brokers      string
	topic        string
	groupID      string
	dedup        *ConsumerDedup
	resolveDedup ConsumerDedupResolver
	projector    EventProjector
	log          *slog.Logger
	clk          clock.Clock
	observer     KafkaObserver
	failures     consumerFailureRecorder
	maxTries     int
	retryMin     time.Duration
	retryMax     time.Duration
}

// ConsumerDedupResolver selects the authoritative consumed_events store for a
// message. Multi-shard workers use it to keep consumer dedup co-located with
// the aggregate being projected; single-shard callers keep the legacy dedup.
type ConsumerDedupResolver func(context.Context, []byte) (*ConsumerDedup, error)

const (
	defaultKafkaConsumerMaxTries = 5
	defaultKafkaConsumerRetryMin = 250 * time.Millisecond
	defaultKafkaConsumerRetryMax = 4 * time.Second
)

type kafkaMessageReader interface {
	FetchMessage(context.Context) (kafka.Message, error)
	CommitMessages(context.Context, ...kafka.Message) error
}

type KafkaObserver interface {
	KafkaConsumed(group, result string)
	KafkaLag(group string, partition int, lag int64)
}

// NewKafkaConsumer 构造 KafkaConsumer。
func NewKafkaConsumer(brokers, topic, groupID string, dedup *ConsumerDedup, projector EventProjector, log *slog.Logger, clk clock.Clock) *KafkaConsumer {
	c := &KafkaConsumer{
		brokers:   brokers,
		topic:     topic,
		groupID:   groupID,
		dedup:     dedup,
		projector: projector,
		log:       log,
		clk:       clk,
		maxTries:  defaultKafkaConsumerMaxTries,
		retryMin:  defaultKafkaConsumerRetryMin,
		retryMax:  defaultKafkaConsumerRetryMax,
	}
	if dedup != nil {
		c.failures = NewConsumerFailureStore(dedup.db)
	}
	return c
}

func (c *KafkaConsumer) WithObserver(o KafkaObserver) *KafkaConsumer { c.observer = o; return c }

// WithDedupResolver enables shard-aware idempotency. The resolver is invoked
// immediately before processing each message and must return a non-nil store.
func (c *KafkaConsumer) WithDedupResolver(resolver ConsumerDedupResolver) *KafkaConsumer {
	c.resolveDedup = resolver
	return c
}

// WithFailurePolicy configures total processing attempts and exponential retry
// bounds. Values outside the valid range retain the safe defaults.
func (c *KafkaConsumer) WithFailurePolicy(maxTries int, retryMin, retryMax time.Duration) *KafkaConsumer {
	if maxTries > 0 {
		c.maxTries = maxTries
	}
	if retryMin > 0 && retryMax >= retryMin {
		c.retryMin = retryMin
		c.retryMax = retryMax
	}
	return c
}

// Run 阻塞执行消费循环直到 ctx 取消。
// 消费流程：FetchMessage → ConsumerDedup.ProcessOnce → CommitMessages。
// ProcessOnce 瞬时失败时停留在当前消息并有限重试，不再次 Fetch，避免
// Reader 的本地读取位置越过失败消息。永久错误或重试耗尽后，必须先把
// 原消息写入 consumer_failed_events，之后才提交 offset 并读取下一条。
// ConsumerDedup 保证重试和提交失败后的幂等（event_id + consumer_name 唯一键）。
func (c *KafkaConsumer) Run(ctx context.Context) error {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     splitBrokers(c.brokers),
		Topic:       c.topic,
		GroupID:     c.groupID,
		MinBytes:    1,                // 有消息即返回，降低延迟
		MaxBytes:    10 << 20,         // 10MB 上限
		StartOffset: kafka.LastOffset, // 新消费者组从最新 offset 开始
	})
	defer r.Close()

	c.log.Info("KafkaConsumer 已启动",
		slog.String("brokers", c.brokers),
		slog.String("topic", c.topic),
		slog.String("group_id", c.groupID),
	)
	return c.run(ctx, r)
}

func (c *KafkaConsumer) run(ctx context.Context, r kafkaMessageReader) error {
	for {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil // ctx 取消，正常退出
			}
			c.log.Error("消息获取失败",
				slog.String("topic", c.topic),
				slog.String("error", err.Error()),
			)
			continue
		}
		if c.observer != nil {
			c.observer.KafkaLag(c.groupID, m.Partition, m.HighWaterMark-m.Offset-1)
		}

		var envelope farmv1.EventEnvelope
		_ = json.Unmarshal(m.Value, &envelope)
		eventCtx := observability.ContextWithTraceID(ctx, envelope.TraceID)
		attempts := 0
		firstFailedAt := time.Time{}
		parked := false
		for {
			attempts++
			dedup := c.dedup
			var processErr error
			if c.resolveDedup != nil {
				dedup, processErr = c.resolveDedup(eventCtx, m.Value)
				if processErr != nil {
					processErr = fmt.Errorf("resolve consumer dedup: %w", processErr)
				}
			}
			if processErr == nil && dedup == nil {
				processErr = fmt.Errorf("kafka consumer %q has no consumer dedup store", c.groupID)
			}
			if processErr == nil {
				processErr = dedup.ProcessOnce(eventCtx, m.Value, c.projector, c.clk.NowUTC())
			}
			if processErr == nil {
				if c.observer != nil {
					c.observer.KafkaConsumed(c.groupID, "ok")
				}
				break
			}

			failedAt := c.clk.NowUTC()
			if firstFailedAt.IsZero() {
				firstFailedAt = failedAt
			}
			if c.observer != nil {
				c.observer.KafkaConsumed(c.groupID, "error")
			}

			permanent := errors.Is(processErr, ErrInvalidEventEnvelope)
			if permanent || attempts >= c.maxTries {
				failure := ConsumerFailure{
					ConsumerGroup: c.groupID,
					ConsumerName:  c.projector.Name(),
					Topic:         m.Topic,
					Partition:     m.Partition,
					Offset:        m.Offset,
					Key:           m.Key,
					EventID:       envelope.EventID,
					Payload:       m.Value,
					Attempts:      attempts,
					LastError:     processErr.Error(),
					FirstFailedAt: firstFailedAt,
					LastFailedAt:  failedAt,
				}
				failureStore := c.failures
				// Preserve any explicitly injected recorder (including tests and
				// special deployments). Only shard-aware consumers replace the
				// default recorder with the resolved aggregate shard.
				if c.resolveDedup != nil && dedup != nil && dedup.db != nil {
					failureStore = NewConsumerFailureStore(dedup.db)
				}
				if err := c.recordFailure(ctx, failureStore, failure); err != nil {
					if ctx.Err() != nil {
						return nil
					}
					return err
				}
				parked = true
				if c.observer != nil {
					c.observer.KafkaConsumed(c.groupID, "parked")
				}
				c.log.Error("事件已持久化到失败事件表，提交原 offset",
					slog.String("topic", m.Topic),
					slog.Int("partition", m.Partition),
					slog.Int64("offset", m.Offset),
					slog.Int("attempts", attempts),
					slog.Bool("permanent", permanent),
					slog.String("error", processErr.Error()),
				)
				break
			}

			c.log.Error("事件处理失败，不提交 offset（退避后重试当前消息）",
				slog.String("topic", m.Topic),
				slog.Int("partition", m.Partition),
				slog.Int64("offset", m.Offset),
				slog.Int("attempt", attempts),
				slog.String("error", processErr.Error()),
			)
			if !waitKafkaConsumerRetry(ctx, c.retryDelay(attempts)) {
				return nil
			}
		}

		if err := r.CommitMessages(ctx, m); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			// 正常消息由 ConsumerDedup 保证重投幂等；失败消息已经可靠停放。
			c.log.Warn("提交 offset 失败（已幂等处理，不影响正确性）",
				slog.String("topic", m.Topic),
				slog.Int64("offset", m.Offset),
				slog.Bool("parked", parked),
				slog.String("error", err.Error()),
			)
		}
	}
}

func (c *KafkaConsumer) recordFailure(ctx context.Context, recorder consumerFailureRecorder, failure ConsumerFailure) error {
	if recorder == nil {
		return errors.New("kafka consumer failure recorder is not configured")
	}
	for storeAttempt := 1; ; storeAttempt++ {
		if err := recorder.Record(ctx, failure); err == nil {
			return nil
		} else if ctx.Err() != nil {
			return ctx.Err()
		} else {
			c.log.Error("失败事件落库失败，不提交 Kafka offset",
				slog.String("topic", failure.Topic),
				slog.Int("partition", failure.Partition),
				slog.Int64("offset", failure.Offset),
				slog.Int("store_attempt", storeAttempt),
				slog.String("error", err.Error()),
			)
		}
		if !waitKafkaConsumerRetry(ctx, c.retryDelay(storeAttempt)) {
			return ctx.Err()
		}
	}
}

func (c *KafkaConsumer) retryDelay(attempt int) time.Duration {
	delay := c.retryMin
	for current := 1; current < attempt && delay < c.retryMax; current++ {
		if delay > c.retryMax/2 {
			return c.retryMax
		}
		delay *= 2
	}
	if delay > c.retryMax {
		return c.retryMax
	}
	return delay
}

func waitKafkaConsumerRetry(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
