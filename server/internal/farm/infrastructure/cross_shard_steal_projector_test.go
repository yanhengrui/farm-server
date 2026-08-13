package infrastructure

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	farmevents "github.com/photon/farm-server/server/contracts/events/farmv1"
	"github.com/photon/farm-server/server/pkg/id"
	"github.com/photon/farm-server/server/pkg/shard"
)

func TestCrossShardStealDedupResolverRoutesToActor(t *testing.T) {
	router, err := shard.NewRouter([]string{"shard-0", "shard-1"})
	if err != nil {
		t.Fatal(err)
	}
	primary := NewConsumerDedup(nil)
	shard0 := NewConsumerDedup(nil)
	shard1 := NewConsumerDedup(nil)
	resolver, err := NewCrossShardStealDedupResolver(router, map[string]*ConsumerDedup{
		"shard-0": shard0,
		"shard-1": shard1,
	}, primary)
	if err != nil {
		t.Fatal(err)
	}
	got, err := resolver(context.Background(), []byte(`{"event_type":"farm.stolen.v1","payload":{"owner_user_id":"2","actor_user_id":"3"}}`))
	if err != nil {
		t.Fatal(err)
	}
	if got != shard1 {
		t.Fatal("stolen asset event did not use actor shard dedup")
	}
}

func TestCrossShardStealProjectorSkipsSameShard(t *testing.T) {
	router, _ := shard.NewRouter([]string{"shard-0", "shard-1"})
	p, err := NewCrossShardStealProjector(router, map[string]*sql.DB{"shard-0": &sql.DB{}, "shard-1": &sql.DB{}})
	if err != nil {
		t.Fatal(err)
	}
	err = p.Handle(context.Background(), farmevents.EventEnvelope{
		EventID:   id.NewV7(),
		EventType: farmevents.EventTypeFarmStolen,
		Payload:   farmevents.FarmStolenPayload{OwnerUserID: "2", ActorUserID: "4", CropID: "wheat", StolenAmount: 1, StolenAt: time.Now()},
	})
	if err != nil {
		t.Fatalf("same-shard event should not require remote credit: %v", err)
	}
}

func TestCrossShardStealInventoryFailureRollsBackLedgerAndRetryCredits(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	actorID := int64(43)
	eventID := "019febfce09c776d896cbc95a03653c0"
	injected := errors.New("injected inventory failure")

	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO economy_transactions`).
		WithArgs(uint64(actorID), eventID, "WHEAT", int64(3), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO inventory_items`).
		WithArgs(uint64(actorID), "CROP", uint64(cropIDToItemID("WHEAT")), int64(3), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnError(injected)
	mock.ExpectRollback()

	if err := creditCrossShardSteal(t.Context(), db, actorID, "WHEAT", 3, eventID); !errors.Is(err, injected) {
		t.Fatalf("first credit error=%v, want injected inventory failure", err)
	}

	// The ledger insert must be attempted again. If it had escaped the first
	// transaction, a real database would return duplicate-key here and skip the
	// inventory credit.
	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO economy_transactions`).
		WithArgs(uint64(actorID), eventID, "WHEAT", int64(3), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO inventory_items`).
		WithArgs(uint64(actorID), "CROP", uint64(cropIDToItemID("WHEAT")), int64(3), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	if err := creditCrossShardSteal(t.Context(), db, actorID, "WHEAT", 3, eventID); err != nil {
		t.Fatalf("retry credit: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
