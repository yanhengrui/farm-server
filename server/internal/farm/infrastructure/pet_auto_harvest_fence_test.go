package infrastructure

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/photon/farm-server/server/pkg/errcode"
)

func TestCheckPetAutoHarvestEnabledRejectsDisabled(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT auto_harvest_enabled[\s\S]+FROM player_pets`).WithArgs(uint64(42)).
		WillReturnRows(sqlmock.NewRows([]string{"auto_harvest_enabled"}).AddRow(false))
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback() //nolint:errcheck
	err = checkPetAutoHarvestEnabled(context.Background(), tx, 42)
	var domainErr *errcode.Error
	if !errors.As(err, &domainErr) || domainErr.Code != errcode.PetAutoDisabled {
		t.Fatalf("err=%v", err)
	}
}
