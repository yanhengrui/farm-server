package infrastructure

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/photon/farm-server/server/internal/farm/application"
	"github.com/photon/farm-server/server/internal/farm/domain"
	"github.com/photon/farm-server/server/pkg/clock"
	"github.com/photon/farm-server/server/pkg/errcode"
)

func TestRoute95MySQLUnavailableFailsWithinDeadline(t *testing.T) {
	db, err := sql.Open("mysql", "route95:route95@tcp(127.0.0.1:1)/route95?timeout=50ms&readTimeout=50ms&writeTimeout=50ms")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	committer := NewMySQLCommitter(db, clock.System{})
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err = committer.CommitFarmCommand(ctx, application.CommitRequest{Command: domain.Command{
		CmdID: "route95-mysql-down", FarmID: 95, ActorUser: 95, Type: domain.CmdPlant,
	}})
	if err == nil {
		t.Fatal("commit unexpectedly succeeded without MySQL")
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("MySQL outage was not bounded: %s", elapsed)
	}
}

// TestRoute95ExternalOutboxPublished verifies that a command sent through the
// public E2E stack was relayed to Kafka and processed by at least one durable
// idempotent consumer. It is read-only and keyed by ROUTE95_FARM_ID.
func TestRoute95ExternalOutboxPublished(t *testing.T) {
	dsn := os.Getenv("ROUTE95_MYSQL_DSN")
	farmIDText := os.Getenv("ROUTE95_FARM_ID")
	if dsn == "" || farmIDText == "" {
		t.Skip("ROUTE95_MYSQL_DSN and ROUTE95_FARM_ID are required")
	}
	farmID, err := strconv.ParseInt(farmIDText, 10, 64)
	if err != nil || farmID <= 0 {
		t.Fatalf("invalid ROUTE95_FARM_ID=%q", farmIDText)
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	var eventID, status string
	var consumed int
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		err = db.QueryRowContext(ctx, `SELECT event_id, status FROM outbox_events
			WHERE aggregate_type = 'farm' AND aggregate_id = ?
			ORDER BY outbox_id DESC LIMIT 1`, farmID).Scan(&eventID, &status)
		if err == nil && status == "PUBLISHED" {
			if err = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM consumed_events WHERE event_id = ?`, eventID).Scan(&consumed); err == nil && consumed > 0 {
				t.Logf("external Outbox published and consumed event=%s consumers=%d", eventID, consumed)
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("Outbox did not converge: event=%s status=%s consumed=%d query_err=%v", eventID, status, consumed, err)
}

// TestRoute95ExternalOutboxBacklogVisible is the Kafka-outage checkpoint: the
// authoritative command has succeeded while Relay has retained a retryable,
// observable PENDING row with an incremented retry counter.
func TestRoute95ExternalOutboxBacklogVisible(t *testing.T) {
	dsn := os.Getenv("ROUTE95_MYSQL_DSN")
	farmIDText := os.Getenv("ROUTE95_FARM_ID")
	if dsn == "" || farmIDText == "" {
		t.Skip("ROUTE95_MYSQL_DSN and ROUTE95_FARM_ID are required")
	}
	farmID, err := strconv.ParseInt(farmIDText, 10, 64)
	if err != nil || farmID <= 0 {
		t.Fatalf("invalid ROUTE95_FARM_ID=%q", farmIDText)
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()

	var eventID, status string
	var retryCount int
	var lastError sql.NullString
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		err = db.QueryRowContext(ctx, `SELECT event_id, status, retry_count, last_error FROM outbox_events
			WHERE aggregate_type = 'farm' AND aggregate_id = ?
			ORDER BY outbox_id DESC LIMIT 1`, farmID).Scan(&eventID, &status, &retryCount, &lastError)
		if err == nil && status == "PENDING" && retryCount > 0 && lastError.Valid {
			t.Logf("external Outbox backlog visible event=%s retry_count=%d", eventID, retryCount)
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("Outbox backlog not visible: event=%s status=%s retry_count=%d last_error=%t query_err=%v",
		eventID, status, retryCount, lastError.Valid, err)
}

// TestRoute95ExternalMySQLACKLossAndFence is enabled by ROUTE95_MYSQL_DSN and
// expects a disposable, fully migrated database. It is the real-MySQL gate for
// ACK loss/idempotency, outbox atomicity, persistent route fencing, and recovery.
func TestRoute95ExternalMySQLACKLossAndFence(t *testing.T) {
	dsn := os.Getenv("ROUTE95_MYSQL_DSN")
	if dsn == "" {
		t.Skip("ROUTE95_MYSQL_DSN is not configured")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping external MySQL: %v", err)
	}

	farmID := time.Now().UnixNano() & 0x3fffffffffffffff
	now := time.Now().UTC()
	snapshot := domain.Snapshot{FarmID: farmID, OwnerID: farmID, RouteEpoch: 1, Plots: make(map[int32]domain.Plot)}
	raw, err := marshalSnapshot(&snapshot)
	if err != nil {
		t.Fatal(err)
	}
	cleanup := func() {
		_, _ = db.ExecContext(context.Background(), "DELETE FROM outbox_events WHERE aggregate_id = ?", farmID)
		_, _ = db.ExecContext(context.Background(), "DELETE FROM cmd_receipts WHERE user_id = ?", farmID)
		_, _ = db.ExecContext(context.Background(), "DELETE FROM inventory_items WHERE user_id = ?", farmID)
		_, _ = db.ExecContext(context.Background(), "DELETE FROM farm_snapshots WHERE farm_id = ?", farmID)
	}
	t.Cleanup(cleanup)
	if _, err := db.ExecContext(ctx, `INSERT INTO farm_snapshots
		(farm_id, owner_user_id, version, snapshot, route_epoch, created_at, updated_at)
		VALUES (?, ?, 0, ?, 1, ?, ?)`, farmID, farmID, raw, now, now); err != nil {
		t.Fatalf("seed farm snapshot (is schema fully migrated?): %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO inventory_items
		(user_id, item_type, item_id, quantity, row_version, created_at, updated_at)
		VALUES (?, 'SEED', ?, 2, 1, ?, ?)`, farmID, cropIDToItemID("WHEAT"), now, now); err != nil {
		t.Fatalf("seed inventory: %v", err)
	}

	committer := NewMySQLCommitter(db, clock.System{})
	command := domain.Command{
		CmdID: "route95-ack-loss", FarmID: farmID, ActorUser: farmID,
		Type: domain.CmdPlant, PlotID: 1, CropID: "WHEAT", RouteEpoch: 1,
	}
	first, err := committer.CommitFarmCommand(ctx, application.CommitRequest{Command: command})
	if err != nil {
		t.Fatalf("first commit: %v", err)
	}
	// Simulate a lost ACK by discarding first. The identical durable key must
	// return the original version without another inventory/outbox mutation.
	replayed, err := committer.CommitFarmCommand(ctx, application.CommitRequest{Command: command})
	if err != nil || !replayed.Replayed || replayed.NewVersion != first.NewVersion {
		t.Fatalf("ACK-loss replay: first=%+v replay=%+v err=%v", first, replayed, err)
	}
	var version, seedQuantity, receipts, outbox int64
	if err := db.QueryRowContext(ctx, "SELECT version FROM farm_snapshots WHERE farm_id = ?", farmID).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, "SELECT quantity FROM inventory_items WHERE user_id = ? AND item_type = 'SEED' AND item_id = ?", farmID, cropIDToItemID("WHEAT")).Scan(&seedQuantity); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM cmd_receipts WHERE user_id = ? AND cmd_id = ?", farmID, command.CmdID).Scan(&receipts); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = ?", farmID).Scan(&outbox); err != nil {
		t.Fatal(err)
	}
	if version != 1 || seedQuantity != 1 || receipts != 1 || outbox != 1 {
		t.Fatalf("non-idempotent state: version=%d seeds=%d receipts=%d outbox=%d", version, seedQuantity, receipts, outbox)
	}

	if err := committer.AdvanceRouteEpoch(ctx, farmID, 2); err != nil {
		t.Fatalf("advance route epoch: %v", err)
	}
	stale := command
	stale.CmdID = "route95-stale-owner"
	stale.PlotID = 2
	_, err = committer.CommitFarmCommand(ctx, application.CommitRequest{Command: stale})
	var coded *errcode.Error
	if !errors.As(err, &coded) || coded.Code != errcode.RoutingFenced {
		t.Fatalf("old owner error=%v, want RoutingFenced", err)
	}
	current := stale
	current.CmdID = "route95-current-owner"
	current.RouteEpoch = 2
	current.BaseVersion = 1
	if result, err := committer.CommitFarmCommand(ctx, application.CommitRequest{Command: current}); err != nil || result.NewVersion != 2 {
		t.Fatalf("new owner commit: result=%+v err=%v", result, err)
	}
	t.Logf("farm=%d ACK-loss replay and route fence verified", farmID)
}

func Example_route95ExternalMySQL() {
	fmt.Println("ROUTE95_MYSQL_DSN='user:pass@tcp(host:3306)/db?parseTime=true' go test ./internal/farm/infrastructure -run TestRoute95ExternalMySQL -v")
	// Output:
	// ROUTE95_MYSQL_DSN='user:pass@tcp(host:3306)/db?parseTime=true' go test ./internal/farm/infrastructure -run TestRoute95ExternalMySQL -v
}
