package infrastructure

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/photon/farm-server/server/pkg/shard"
)

func TestShardedAccountServiceFindsLegacyDeviceBeforeHashRoute(t *testing.T) {
	db0, mock0, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db0.Close()
	db1, mock1, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db1.Close()
	deviceID := "legacy-device-on-other-shard"
	mock0.ExpectQuery(`SELECT user_id FROM auth_identities`).WithArgs(deviceID).
		WillReturnRows(sqlmock.NewRows([]string{"user_id"}))
	mock1.ExpectQuery(`SELECT user_id FROM auth_identities`).WithArgs(deviceID).
		WillReturnRows(sqlmock.NewRows([]string{"user_id"}).AddRow(99))
	router, err := shard.NewRouter([]string{"shard-1", "shard-0"})
	if err != nil {
		t.Fatal(err)
	}
	s0 := NewMySQLAccountService(db0, nil, nil, nil)
	s1 := NewMySQLAccountService(db1, nil, nil, nil)
	service, err := NewShardedAccountService(router, map[string]*MySQLAccountService{"shard-0": s0, "shard-1": s1})
	if err != nil {
		t.Fatal(err)
	}
	got, exists, err := service.serviceForExistingGuestDevice(context.Background(), deviceID)
	if err != nil || !exists || got != s1 {
		t.Fatalf("got service=%p exists=%v err=%v, want shard-1 true", got, exists, err)
	}
	if err := mock0.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	if err := mock1.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestLookupGuestUserIDNotFoundIsNotAnError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery(`SELECT user_id FROM auth_identities`).WithArgs("new-device").
		WillReturnRows(sqlmock.NewRows([]string{"user_id"}))
	service := NewMySQLAccountService(db, nil, nil, nil)
	userID, found, err := service.LookupGuestUserID(context.Background(), "new-device")
	if err != nil || found || userID != 0 {
		t.Fatalf("userID=%d found=%v err=%v", userID, found, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
