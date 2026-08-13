package infrastructure_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	petinfra "github.com/photon/farm-server/server/internal/pet/infrastructure"
	"github.com/photon/farm-server/server/pkg/errcode"
)

func TestSetAutoHarvestNotOwned(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT 1 FROM farm_snapshots`).WithArgs(uint64(42)).
		WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
	mock.ExpectQuery(`SELECT status, auto_harvest_enabled FROM player_pets`).WithArgs(uint64(42)).
		WillReturnError(sql.ErrNoRows)
	err = petinfra.NewMySQLPetService(db).SetAutoHarvest(context.Background(), 42, false)
	var domainErr *errcode.Error
	if !errors.As(err, &domainErr) || domainErr.Code != errcode.PetNotOwned {
		t.Fatalf("err=%v", err)
	}
}

func TestSetAutoHarvestDisableClearsSchedule(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT 1 FROM farm_snapshots`).WithArgs(uint64(42)).
		WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
	mock.ExpectQuery(`SELECT status, auto_harvest_enabled FROM player_pets`).WithArgs(uint64(42)).
		WillReturnRows(sqlmock.NewRows([]string{"status", "auto_harvest_enabled"}).AddRow("ACTIVE", true))
	mock.ExpectExec(`UPDATE player_pets SET auto_harvest_enabled`).WithArgs(false, uint64(42)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`UPDATE farm_snapshots SET next_pet_action_at`).WithArgs(nil, uint64(42)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	if err := petinfra.NewMySQLPetService(db).SetAutoHarvest(context.Background(), 42, false); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestSetAutoHarvestEnableSchedulesFreshInterval(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT 1 FROM farm_snapshots`).WithArgs(uint64(42)).
		WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
	mock.ExpectQuery(`SELECT status, auto_harvest_enabled FROM player_pets`).WithArgs(uint64(42)).
		WillReturnRows(sqlmock.NewRows([]string{"status", "auto_harvest_enabled"}).AddRow("ACTIVE", false))
	mock.ExpectExec(`UPDATE player_pets SET auto_harvest_enabled`).WithArgs(true, uint64(42)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`UPDATE farm_snapshots SET next_pet_action_at`).WithArgs(sqlmock.AnyArg(), uint64(42)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	if err := petinfra.NewMySQLPetService(db).SetAutoHarvest(context.Background(), 42, true); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
