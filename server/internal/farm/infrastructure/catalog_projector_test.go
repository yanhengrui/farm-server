package infrastructure

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	farmevents "github.com/photon/farm-server/server/contracts/events/farmv1"
)

func TestCatalogProjectorUnlocksEachConfiguredCrop(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	p := NewCatalogProjector(db, slog.New(slog.NewTextHandler(io.Discard, nil)))

	for _, tc := range []struct{ cropID, catalogKey string }{
		{"WHEAT", "crop_WHEAT"},
		{"CARROT", "crop_CARROT"},
		{"TOMATO", "crop_TOMATO"},
	} {
		mock.ExpectExec(`INSERT IGNORE INTO catalog_unlocks`).
			WithArgs(uint64(42), tc.catalogKey, sqlmock.AnyArg()).
			WillReturnResult(sqlmock.NewResult(1, 1))
		err := p.Handle(context.Background(), farmevents.EventEnvelope{
			EventID: "event-" + tc.cropID, EventType: farmevents.EventTypeFarmHarvested,
			Payload: farmevents.FarmHarvestedPayload{OwnerUserID: "42", CropID: tc.cropID},
		})
		if err != nil {
			t.Fatalf("handle %s: %v", tc.cropID, err)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestCatalogProjectorSkipsUnknownCrop(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	p := NewCatalogProjector(db, slog.New(slog.NewTextHandler(io.Discard, nil)))
	err = p.Handle(context.Background(), farmevents.EventEnvelope{
		EventID: "event-unknown", EventType: farmevents.EventTypeFarmHarvested,
		Payload: farmevents.FarmHarvestedPayload{OwnerUserID: "42", CropID: "UNKNOWN"},
	})
	if err != nil {
		t.Fatalf("unknown crop: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
