package infrastructure

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	farmevents "github.com/photon/farm-server/server/contracts/events/farmv1"
	socialevents "github.com/photon/farm-server/server/contracts/events/socialv1"
	"github.com/photon/farm-server/server/pkg/id"
	"github.com/photon/farm-server/server/pkg/shard"
)

// TestRoute11ExternalTwoShardFriendSaga verifies the durable request -> remote
// edge -> ACK -> source activation sequence against two disposable MySQL
// schemas. It also replays the remote request and proves there is one ACK only.
func TestRoute11ExternalTwoShardFriendSaga(t *testing.T) {
	dsns, err := route11SocialExternalShardDSNs(os.Getenv("ROUTE11_SHARD_DSNS"))
	if err != nil {
		t.Skipf("ROUTE11_SHARD_DSNS is required for external two-shard gate: %v", err)
	}
	router, err := shard.NewRouter([]string{"a", "b"})
	if err != nil {
		t.Fatal(err)
	}
	dbs := make(map[string]*sql.DB, len(dsns))
	for name, dsn := range dsns {
		db, openErr := sql.Open("mysql", dsn)
		if openErr != nil {
			t.Fatalf("open shard %s: %v", name, openErr)
		}
		db.SetMaxOpenConns(2)
		db.SetMaxIdleConns(2)
		dbs[name] = db
		t.Cleanup(func() { _ = db.Close() })
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()

	// Even -> a, odd -> b. The initiating source is b and target is a.
	targetID := time.Now().UnixNano() & 0x3ffffffffffffffe
	if targetID <= 0 {
		targetID = 2
	}
	sourceID := targetID + 1
	sourceShard, _ := router.ShardForUserID(sourceID)
	targetShard, _ := router.ShardForUserID(targetID)
	if sourceShard != "b" || targetShard != "a" {
		t.Fatalf("unexpected routes source=%s target=%s", sourceShard, targetShard)
	}
	t.Cleanup(func() {
		for _, db := range dbs {
			_, _ = db.ExecContext(context.Background(), "DELETE FROM friendship_edges WHERE user_id IN (?, ?) OR friend_user_id IN (?, ?)", sourceID, targetID, sourceID, targetID)
			_, _ = db.ExecContext(context.Background(), "DELETE FROM outbox_events WHERE aggregate_type='social' AND aggregate_id IN (?, ?)", sourceID, targetID)
		}
	})

	sourceEventID := id.NewV7()
	sagas := map[string]*FriendEdgeSaga{"a": NewFriendEdgeSaga(dbs["a"]), "b": NewFriendEdgeSaga(dbs["b"])}
	if err := sagas[sourceShard].Begin(ctx, sourceID, targetID, sourceEventID); err != nil {
		t.Fatalf("begin source edge: %v", err)
	}
	projector, err := NewFriendEdgeProjector(router, sagas)
	if err != nil {
		t.Fatal(err)
	}
	request := farmevents.EventEnvelope{
		EventID: sourceEventID, EventType: farmevents.EventType(socialevents.EventTypeFriendEdgeRequested),
		Payload: socialevents.FriendEdgeRequestedPayload{SourceEventID: sourceEventID, SourceUserID: fmt.Sprint(sourceID), TargetUserID: fmt.Sprint(targetID), RequestedAt: time.Now().UTC()},
	}
	if err := projector.Handle(ctx, request); err != nil {
		t.Fatalf("apply remote request: %v", err)
	}
	if err := projector.Handle(ctx, request); err != nil {
		t.Fatalf("replay remote request: %v", err)
	}

	var targetState string
	if err := dbs[targetShard].QueryRowContext(ctx, "SELECT state FROM friendship_edges WHERE user_id=? AND friend_user_id=?", targetID, sourceID).Scan(&targetState); err != nil {
		t.Fatal(err)
	}
	var ackRows int
	if err := dbs[targetShard].QueryRowContext(ctx, `SELECT COUNT(*)
		FROM outbox_events WHERE event_type=? AND JSON_UNQUOTE(JSON_EXTRACT(payload, '$.correlation_id'))=?`,
		string(socialevents.EventTypeFriendEdgeApplied), sourceEventID).Scan(&ackRows); err != nil {
		t.Fatal(err)
	}
	if targetState != "ACTIVE" || ackRows != 1 {
		t.Fatalf("remote edge/ACK invariant state=%s ack_rows=%d", targetState, ackRows)
	}
	var ackRaw []byte
	if err := dbs[targetShard].QueryRowContext(ctx, `SELECT payload FROM outbox_events
		WHERE event_type=? AND JSON_UNQUOTE(JSON_EXTRACT(payload, '$.correlation_id'))=? LIMIT 1`,
		string(socialevents.EventTypeFriendEdgeApplied), sourceEventID).Scan(&ackRaw); err != nil {
		t.Fatal(err)
	}
	var ack farmevents.EventEnvelope
	if err := json.Unmarshal(ackRaw, &ack); err != nil {
		t.Fatalf("decode durable ACK: %v", err)
	}
	if err := projector.Handle(ctx, ack); err != nil {
		t.Fatalf("confirm source edge: %v", err)
	}
	var sourceState string
	if err := dbs[sourceShard].QueryRowContext(ctx, "SELECT state FROM friendship_edges WHERE user_id=? AND friend_user_id=?", sourceID, targetID).Scan(&sourceState); err != nil {
		t.Fatal(err)
	}
	if sourceState != "ACTIVE" {
		t.Fatalf("source state=%s, want ACTIVE", sourceState)
	}
	t.Logf("friend saga verified source=%d/%s target=%d/%s source_event=%s", sourceID, sourceShard, targetID, targetShard, sourceEventID)
}

func route11SocialExternalShardDSNs(raw string) (map[string]string, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, fmt.Errorf("empty")
	}
	result := make(map[string]string, 2)
	for _, entry := range strings.Split(raw, ",") {
		name, dsn, ok := strings.Cut(strings.TrimSpace(entry), "=")
		name, dsn = strings.TrimSpace(name), strings.TrimSpace(dsn)
		if !ok || (name != "a" && name != "b") || dsn == "" {
			return nil, fmt.Errorf("expected a=<dsn>,b=<dsn>")
		}
		if _, duplicate := result[name]; duplicate {
			return nil, fmt.Errorf("duplicate shard %q", name)
		}
		result[name] = dsn
	}
	if len(result) != 2 || result["a"] == "" || result["b"] == "" {
		return nil, fmt.Errorf("both a and b are required")
	}
	return result, nil
}

func TestRoute11ExternalFriendCompensatesOnlyDeadOutbox(t *testing.T) {
	dsns, err := route11SocialExternalShardDSNs(os.Getenv("ROUTE11_SHARD_DSNS"))
	if err != nil {
		t.Skipf("ROUTE11_SHARD_DSNS is required for external two-shard gate: %v", err)
	}
	router, err := shard.NewRouter([]string{"a", "b"})
	if err != nil {
		t.Fatal(err)
	}
	dbs := make(map[string]*sql.DB, len(dsns))
	for name, dsn := range dsns {
		db, openErr := sql.Open("mysql", dsn)
		if openErr != nil {
			t.Fatalf("open shard %s: %v", name, openErr)
		}
		dbs[name] = db
		t.Cleanup(func() { _ = db.Close() })
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	targetID := time.Now().UnixNano() & 0x3ffffffffffffffe
	if targetID <= 0 {
		targetID = 2
	}
	sourceID := targetID + 1
	sourceShard, _ := router.ShardForUserID(sourceID)
	if sourceShard != "b" {
		t.Fatalf("source route=%s, want b", sourceShard)
	}
	sourceDB := dbs[sourceShard]
	t.Cleanup(func() {
		for _, db := range dbs {
			_, _ = db.ExecContext(context.Background(), "DELETE FROM friendship_edges WHERE user_id IN (?, ?) OR friend_user_id IN (?, ?)", sourceID, targetID, sourceID, targetID)
			_, _ = db.ExecContext(context.Background(), "DELETE FROM outbox_events WHERE aggregate_type='social' AND aggregate_id IN (?, ?)", sourceID, targetID)
		}
	})

	eventID := NewSourceEventID()
	saga := NewFriendEdgeSaga(sourceDB)
	if err := saga.Begin(ctx, sourceID, targetID, eventID); err != nil {
		t.Fatalf("begin pending friend edge: %v", err)
	}
	compensator := NewFriendEdgeCompensator(sourceDB)
	if n, err := compensator.CompensateDeadRequests(ctx, 10); err != nil || n != 0 {
		t.Fatalf("pending outbox must not be compensated: n=%d err=%v", n, err)
	}
	if _, err := sourceDB.ExecContext(ctx, "UPDATE outbox_events SET status='DEAD' WHERE event_id=?", eventID); err != nil {
		t.Fatalf("inject terminal outbox failure: %v", err)
	}
	if n, err := compensator.CompensateDeadRequests(ctx, 10); err != nil || n != 1 {
		t.Fatalf("compensate terminal outbox failure: n=%d err=%v", n, err)
	}
	if n, err := compensator.CompensateDeadRequests(ctx, 10); err != nil || n != 0 {
		t.Fatalf("compensation must be idempotent: n=%d err=%v", n, err)
	}
	var state string
	if err := sourceDB.QueryRowContext(ctx, "SELECT state FROM friendship_edges WHERE user_id=? AND friend_user_id=?", sourceID, targetID).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "FAILED" {
		t.Fatalf("source state=%s, want FAILED", state)
	}
	t.Logf("dead outbox compensation verified source=%d event=%s", sourceID, eventID)
}
