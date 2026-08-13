package infrastructure_test

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	cataloginfra "github.com/photon/farm-server/server/internal/catalog/infrastructure"
)

const listCatalogQuery = `
		SELECT catalog_key, unlocked_at
		FROM catalog_unlocks
		WHERE user_id = ?
		ORDER BY catalog_key`

func TestListCatalogUnlocksEmpty(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery(regexp.QuoteMeta(listCatalogQuery)).WithArgs(uint64(42)).
		WillReturnRows(sqlmock.NewRows([]string{"catalog_key", "unlocked_at"}))

	items, err := cataloginfra.NewMySQLCatalogService(db).ListCatalogUnlocks(context.Background(), 42)
	if err != nil {
		t.Fatal(err)
	}
	if items == nil || len(items) != 0 {
		t.Fatalf("items=%#v, want non-nil empty slice", items)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestListCatalogUnlocksMultipleAndTime(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	wheatAt := time.Date(2026, 8, 3, 9, 10, 11, 123000000, time.UTC)
	cornAt := wheatAt.Add(time.Minute)
	mock.ExpectQuery(regexp.QuoteMeta(listCatalogQuery)).WithArgs(uint64(42)).
		WillReturnRows(sqlmock.NewRows([]string{"catalog_key", "unlocked_at"}).
			AddRow("crop_CORN", cornAt).
			AddRow("crop_WHEAT", wheatAt))

	items, err := cataloginfra.NewMySQLCatalogService(db).ListCatalogUnlocks(context.Background(), 42)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 || items[0].CatalogKey != "crop_CORN" || !items[1].UnlockedAt.Equal(wheatAt) {
		t.Fatalf("unexpected items: %+v", items)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
