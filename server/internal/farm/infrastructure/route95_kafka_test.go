package infrastructure

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

// TestRoute95ExternalKafkaRoundTrip publishes through the production
// KafkaPublisher and consumes from an isolated test topic. The fixed topic and
// group avoid creating unbounded external metadata; each run uses a unique key.
func TestRoute95ExternalKafkaRoundTrip(t *testing.T) {
	brokers := os.Getenv("ROUTE95_KAFKA_BROKERS")
	if brokers == "" {
		t.Skip("ROUTE95_KAFKA_BROKERS is not configured")
	}
	topic := os.Getenv("ROUTE95_KAFKA_TOPIC")
	if topic == "" {
		topic = "route95-e2e"
	}
	groupID := "route95-e2e-verifier"
	key := fmt.Sprintf("route95-%d", time.Now().UnixNano())
	payload := []byte(`{"event_type":"route95.e2e","schema_version":1}`)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	bootstrap := splitBrokers(brokers)
	if len(bootstrap) == 0 {
		t.Fatal("ROUTE95_KAFKA_BROKERS has no usable address")
	}
	conn, err := kafka.DialContext(ctx, "tcp", bootstrap[0])
	if err != nil {
		t.Fatalf("dial external Kafka: %v", err)
	}
	controller, err := conn.Controller()
	_ = conn.Close()
	if err != nil {
		t.Fatalf("discover Kafka controller: %v", err)
	}
	controllerConn, err := kafka.DialContext(ctx, "tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		t.Fatalf("dial Kafka controller: %v", err)
	}
	if err := controllerConn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		_ = controllerConn.Close()
		t.Fatalf("set Kafka controller deadline: %v", err)
	}
	err = controllerConn.CreateTopics(kafka.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1})
	_ = controllerConn.Close()
	if err != nil {
		t.Fatalf("ensure Kafka test topic: %v", err)
	}
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     splitBrokers(brokers),
		Topic:       topic,
		GroupID:     groupID,
		MinBytes:    1,
		MaxBytes:    1 << 20,
		StartOffset: kafka.LastOffset,
	})
	defer reader.Close()
	received := make(chan kafka.Message, 1)
	readErr := make(chan error, 1)
	go func() {
		for {
			message, err := reader.ReadMessage(ctx)
			if err != nil {
				readErr <- err
				return
			}
			if string(message.Key) == key {
				received <- message
				return
			}
		}
	}()
	// Wait for an actual group assignment before publishing. Kafka's default
	// initial rebalance delay is commonly three seconds, so a fixed one-second
	// sleep races with StartOffset=Last and can skip the target message.
	assignmentDeadline := time.Now().Add(10 * time.Second)
	assigned := false
	for !assigned && time.Now().Before(assignmentDeadline) {
		assigned = reader.Stats().Rebalances > 0
		if assigned {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !assigned {
		t.Fatal("Kafka reader did not receive a group assignment")
	}
	publisher := NewKafkaPublisher(brokers)
	defer publisher.Close()
	if err := publisher.Publish(ctx, topic, key, payload); err != nil {
		t.Fatalf("publish external Kafka message: %v", err)
	}

	select {
	case message := <-received:
		if string(message.Value) != string(payload) {
			t.Fatalf("Kafka payload mismatch: %q", message.Value)
		}
		t.Logf("external Kafka round trip verified topic=%s partition=%d offset=%d", topic, message.Partition, message.Offset)
	case err := <-readErr:
		if err == context.DeadlineExceeded || err == context.Canceled {
			t.Fatalf("consume external Kafka message: %v", err)
		}
		t.Fatalf("consume external Kafka message: %v", err)
	case <-ctx.Done():
		t.Fatalf("external Kafka round trip timed out: %v", ctx.Err())
	}
}
