// Package infrastructure 提供 Outbox Relay 使用的 EventPublisher 实现。
// EventPublisher 接口将 OutboxRelay 与具体消息总线解耦。
// 内存驱动：MemPublisher（零外部依赖，适合测试和本地开发）。
// Kafka 驱动：KafkaPublisher（使用 github.com/segmentio/kafka-go）。
package infrastructure

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

// EventPublisher 将序列化后的事件发布到消息总线。
type EventPublisher interface {
	// Publish 将 payload 发送到 topic，使用 partitionKey 进行路由。
	Publish(ctx context.Context, topic, partitionKey string, payload []byte) error
	// Close 释放底层资源。
	Close() error
}

// BatchEventPublisher is an optional fast path used by the Outbox Relay. The
// returned slice is aligned with messages; a nil entry means that message was
// durably accepted by the broker. Implementations must not return a shorter
// slice.
type BatchEventPublisher interface {
	PublishBatch(ctx context.Context, messages []MemMessage) []error
}

// ── MemPublisher ──────────────────────────────────────────────────────────────

// MemMessage 是一条已捕获的发布调用记录，供测试断言使用。
type MemMessage struct {
	Topic        string
	PartitionKey string
	Payload      []byte
}

// MemPublisher 是线程安全的内存 EventPublisher。
// 用于 memory 事件总线驱动和单元测试。
type MemPublisher struct {
	mu   sync.Mutex
	msgs []MemMessage
	err  error // 若设置，Publish 返回此错误
}

// NewMemPublisher 构造一个空消息缓冲区的 MemPublisher。
func NewMemPublisher() *MemPublisher { return &MemPublisher{} }

// SetPublishError 配置 Publish 在每次调用时返回 err（测试辅助）。
func (p *MemPublisher) SetPublishError(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.err = err
}

// Publish 记录消息。若设置了 PublishError 则返回该错误。
func (p *MemPublisher) Publish(_ context.Context, topic, partitionKey string, payload []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return p.err
	}
	cp := make([]byte, len(payload))
	copy(cp, payload)
	p.msgs = append(p.msgs, MemMessage{Topic: topic, PartitionKey: partitionKey, Payload: cp})
	return nil
}

// Messages 返回所有已发布消息的快照。
func (p *MemPublisher) Messages() []MemMessage {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]MemMessage, len(p.msgs))
	copy(out, p.msgs)
	return out
}

// Drain 取出并清空已缓冲的消息（memory 模式下供 workersvr 消费投影）。
func (p *MemPublisher) Drain() []MemMessage {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := p.msgs
	p.msgs = nil
	return out
}

// Close 对 MemPublisher 为空操作。
func (p *MemPublisher) Close() error { return nil }

var _ EventPublisher = (*MemPublisher)(nil)

// ── KafkaPublisher ────────────────────────────────────────────────────────────

// KafkaPublisher 使用 kafka-go kafka.Writer 向 Kafka 发布事件。
// 每条消息在调用侧指定 Topic，Writer 不绑定单一 Topic（多 Topic 模式）。
type KafkaPublisher struct {
	writer *kafka.Writer
}

// NewKafkaPublisher 使用逗号分隔的 brokers（host:port）构造 KafkaPublisher。
// RequiredAcks=RequireOne：leader 写入确认，兼顾可用性与可靠性。
// Balancer=Hash：按 partitionKey 哈希路由，保证同一 farm_id 有序。
func NewKafkaPublisher(brokers string) EventPublisher {
	return &KafkaPublisher{
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(splitBrokers(brokers)...),
			Balancer:               &kafka.Hash{},
			RequiredAcks:           kafka.RequireOne,
			AllowAutoTopicCreation: true,
			BatchSize:              1000,
			BatchTimeout:           5 * time.Millisecond,
		},
	}
}

func (p *KafkaPublisher) PublishBatch(ctx context.Context, messages []MemMessage) []error {
	result := make([]error, len(messages))
	if len(messages) == 0 {
		return result
	}
	kafkaMessages := make([]kafka.Message, len(messages))
	for i, message := range messages {
		kafkaMessages[i] = kafka.Message{
			Topic: message.Topic,
			Key:   []byte(message.PartitionKey),
			Value: message.Payload,
		}
	}
	err := p.writer.WriteMessages(ctx, kafkaMessages...)
	if err == nil {
		return result
	}
	var writeErrors kafka.WriteErrors
	if errors.As(err, &writeErrors) && len(writeErrors) == len(result) {
		copy(result, writeErrors)
		return result
	}
	for i := range result {
		result[i] = err
	}
	return result
}

// Publish 同步写入一条消息；ctx 超时/取消会中止等待。
func (p *KafkaPublisher) Publish(ctx context.Context, topic, partitionKey string, payload []byte) error {
	return p.writer.WriteMessages(ctx, kafka.Message{
		Topic: topic,
		Key:   []byte(partitionKey),
		Value: payload,
	})
}

// Close 刷新并关闭底层 Writer。
func (p *KafkaPublisher) Close() error { return p.writer.Close() }

var _ EventPublisher = (*KafkaPublisher)(nil)
var _ BatchEventPublisher = (*KafkaPublisher)(nil)

// splitBrokers 将逗号分隔的 "host:port,host:port" 解析为字符串切片。
// 同包内 event_consumer.go 也使用此函数。
func splitBrokers(brokers string) []string {
	parts := strings.Split(brokers, ",")
	out := make([]string, 0, len(parts))
	for _, b := range parts {
		if b = strings.TrimSpace(b); b != "" {
			out = append(out, b)
		}
	}
	return out
}
