package infrastructure

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	farmevents "github.com/photon/farm-server/server/contracts/events/farmv1"
	"github.com/photon/farm-server/server/pkg/id"
	"github.com/photon/farm-server/server/pkg/shard"
)

// TestRoute11ExternalTwoShardCrossSteal is an opt-in, disposable-schema gate.
// It intentionally uses only ROUTE11_SHARD_DSNS and never discovers a running
// service DSN.  Each run creates IDs unique to the process and deletes only
// those rows in its Cleanup, so it is safe for the dedicated c2shard schemas.
func TestRoute11ExternalTwoShardCrossSteal(t *testing.T) {
	dsns, err := route11ExternalShardDSNs(os.Getenv("ROUTE11_SHARD_DSNS"))
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
	for name, db := range dbs {
		if pingErr := db.PingContext(ctx); pingErr != nil {
			t.Fatalf("ping shard %s: %v", name, pingErr)
		}
	}

	// Lexically sorted {a,b}: even IDs route to a, odd IDs route to b.
	ownerID := time.Now().UnixNano() & 0x3ffffffffffffffe
	if ownerID <= 0 {
		ownerID = 2
	}
	actorID := ownerID + 1
	ownerShard, err := router.ShardForUserID(ownerID)
	if err != nil || ownerShard != "a" {
		t.Fatalf("owner route=%q err=%v, want a", ownerShard, err)
	}
	actorShard, err := router.ShardForUserID(actorID)
	if err != nil || actorShard != "b" {
		t.Fatalf("actor route=%q err=%v, want b", actorShard, err)
	}
	t.Cleanup(func() {
		for _, db := range dbs {
			_, _ = db.ExecContext(context.Background(), "DELETE FROM inventory_items WHERE user_id IN (?, ?)", ownerID, actorID)
			_, _ = db.ExecContext(context.Background(), "DELETE FROM economy_transactions WHERE user_id IN (?, ?)", ownerID, actorID)
		}
	})

	projector, err := NewCrossShardStealProjector(router, dbs)
	if err != nil {
		t.Fatal(err)
	}
	eventID := id.NewV7()
	env := farmevents.EventEnvelope{
		EventID:   eventID,
		EventType: farmevents.EventTypeFarmStolen,
		Payload: farmevents.FarmStolenPayload{
			OwnerUserID:  fmt.Sprint(ownerID),
			ActorUserID:  fmt.Sprint(actorID),
			CropID:       "WHEAT",
			StolenAmount: 3,
			StolenAt:     time.Now().UTC(),
		},
	}
	if err := projector.Handle(ctx, env); err != nil {
		t.Fatalf("first cross-shard credit: %v", err)
	}
	if err := projector.Handle(ctx, env); err != nil {
		t.Fatalf("idempotent replay: %v", err)
	}

	var quantity, ledgerRows, wrongShardRows int64
	if err := dbs[actorShard].QueryRowContext(ctx, `SELECT COALESCE(MAX(quantity), 0)
		FROM inventory_items WHERE user_id = ? AND item_type = 'CROP' AND item_id = ?`, actorID, cropIDToItemID("WHEAT")).Scan(&quantity); err != nil {
		t.Fatal(err)
	}
	if err := dbs[actorShard].QueryRowContext(ctx, `SELECT COUNT(*)
		FROM economy_transactions WHERE user_id = ? AND biz_type = 'CROSS_SHARD_STEAL'`, actorID).Scan(&ledgerRows); err != nil {
		t.Fatal(err)
	}
	if err := dbs[ownerShard].QueryRowContext(ctx, `SELECT COUNT(*)
		FROM inventory_items WHERE user_id = ? AND item_type = 'CROP' AND item_id = ?`, actorID, cropIDToItemID("WHEAT")).Scan(&wrongShardRows); err != nil {
		t.Fatal(err)
	}
	if quantity != 3 || ledgerRows != 1 || wrongShardRows != 0 {
		t.Fatalf("cross-shard credit violated: quantity=%d ledger_rows=%d wrong_shard_rows=%d", quantity, ledgerRows, wrongShardRows)
	}
	t.Logf("cross-shard actor credit verified owner=%d/%s actor=%d/%s event=%s", ownerID, ownerShard, actorID, actorShard, eventID)
}

func route11ExternalShardDSNs(raw string) (map[string]string, error) {
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
