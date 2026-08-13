package infrastructure_test

import (
	"context"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	economyinfra "github.com/photon/farm-server/server/internal/economy/infrastructure"
)

func TestGetPlayerAssets(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT coin_balance FROM wallets WHERE user_id = ?`)).WithArgs(uint64(42)).
		WillReturnRows(sqlmock.NewRows([]string{"coin_balance"}).AddRow(321))
	mock.ExpectQuery(`SELECT item_type, item_id, quantity\s+FROM inventory_items`).WithArgs(uint64(42)).
		WillReturnRows(sqlmock.NewRows([]string{"item_type", "item_id", "quantity"}).
			AddRow("CROP", 1, 2).AddRow("SEED", 1, 5))
	mock.ExpectCommit()

	assets, err := economyinfra.NewMySQLAssetService(db).GetPlayerAssets(context.Background(), 42)
	if err != nil {
		t.Fatal(err)
	}
	if assets.CoinBalance != 321 || len(assets.Inventory) != 2 || assets.Inventory[0].ItemType != "CROP" {
		t.Fatalf("assets=%+v", assets)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
