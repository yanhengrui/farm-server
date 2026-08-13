package infrastructure_test

import (
	"context"
	"errors"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"

	petinfra "github.com/photon/farm-server/server/internal/pet/infrastructure"
	"github.com/photon/farm-server/server/pkg/errcode"
)

func assertPetErrCode(t *testing.T, err error, want errcode.Code) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error %s, got nil", want)
	}
	var e *errcode.Error
	if !errors.As(err, &e) {
		t.Fatalf("expected *errcode.Error, got %T: %v", err, err)
	}
	if e.Code != want {
		t.Errorf("expected code %s, got %s", want, e.Code)
	}
}

func TestBuyPet_AlreadyHasPet(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT 1 FROM farm_snapshots`).
		WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
	mock.ExpectQuery(`SELECT 1 FROM player_pets`).
		WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
	mock.ExpectRollback()

	svc := petinfra.NewMySQLPetService(db)
	err = svc.BuyPet(context.Background(), 42)
	assertPetErrCode(t, err, errcode.PetAlreadyEquipped)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestBuyPet_InsufficientBalance(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT 1 FROM farm_snapshots`).
		WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
	mock.ExpectQuery(`SELECT 1 FROM player_pets`).
		WillReturnRows(sqlmock.NewRows([]string{"1"}))
	mock.ExpectQuery(`SELECT coin_balance FROM wallets`).
		WillReturnRows(sqlmock.NewRows([]string{"coin_balance"}).AddRow(100))
	mock.ExpectRollback()

	svc := petinfra.NewMySQLPetService(db)
	err = svc.BuyPet(context.Background(), 42)
	assertPetErrCode(t, err, errcode.EconomyInsufficient)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestBuyPet_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT 1 FROM farm_snapshots`).
		WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
	mock.ExpectQuery(`SELECT 1 FROM player_pets`).
		WillReturnRows(sqlmock.NewRows([]string{"1"}))
	mock.ExpectQuery(`SELECT coin_balance FROM wallets`).
		WillReturnRows(sqlmock.NewRows([]string{"coin_balance"}).AddRow(500))
	mock.ExpectExec(`UPDATE wallets`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO player_pets`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`UPDATE farm_snapshots`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	svc := petinfra.NewMySQLPetService(db)
	if err := svc.BuyPet(context.Background(), 42); err != nil {
		t.Fatalf("BuyPet: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
