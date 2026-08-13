package infrastructure

import (
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/photon/farm-server/server/pkg/shard"
)

func expectRegisteredAccountLoad(mock sqlmock.Sqlmock, userID int64, displayName string, now time.Time) {
	mock.ExpectQuery(`SELECT user_id, COALESCE\(display_name,''\), account_type, status, farm_id, created_at`).
		WithArgs(uint64(userID)).
		WillReturnRows(sqlmock.NewRows([]string{"user_id", "display_name", "account_type", "status", "farm_id", "created_at"}).
			AddRow(userID, displayName, "REGISTERED", "ACTIVE", userID, now))
}

func TestRegisterWithIDGeneratorKeepsAccountOnSelectedShard(t *testing.T) {
	now := time.Date(2026, 8, 11, 10, 0, 1, 0, time.UTC)
	epoch := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	service, mock := newGuestLoginTestService(t, now)
	generator, err := shard.NewIDGenerator(epoch, 1, 3)
	if err != nil {
		t.Fatal(err)
	}
	expectedGenerator, err := shard.NewIDGenerator(epoch, 1, 3)
	if err != nil {
		t.Fatal(err)
	}
	expectedUserID, err := expectedGenerator.Next(now)
	if err != nil {
		t.Fatal(err)
	}
	service.WithIDGenerator(generator)

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT 1 FROM auth_identities`).WithArgs("sharded_farmer").
		WillReturnRows(sqlmock.NewRows([]string{"1"}))
	mock.ExpectExec(`INSERT INTO accounts \(user_id, farm_id`).
		WithArgs(uint64(expectedUserID), uint64(expectedUserID), "分片农场", now, now, now).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO auth_identities`).WithArgs(uint64(expectedUserID), "sharded_farmer", sqlmock.AnyArg(), now, now, now).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO farm_snapshots`).WithArgs(uint64(expectedUserID), uint64(expectedUserID), sqlmock.AnyArg(), now, now).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO wallets`).WithArgs(uint64(expectedUserID), initCoinBalance, now, now).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO inventory_items`).WithArgs(uint64(expectedUserID), uint64(initSeedItemID), initSeedQuantity, now, now).
		WillReturnResult(sqlmock.NewResult(1, 1))
	expectRegisteredAccountLoad(mock, expectedUserID, "分片农场", now)
	mock.ExpectCommit()

	result, err := service.Register(t.Context(), "sharded_farmer", "password-123", "分片农场")
	if err != nil {
		t.Fatal(err)
	}
	router, err := shard.NewRouter([]string{"shard-0", "shard-1"})
	if err != nil {
		t.Fatal(err)
	}
	name, err := router.ShardForUserID(result.Account.UserID)
	if err != nil || name != "shard-1" {
		t.Fatalf("registered user %d routed to %q: %v", result.Account.UserID, name, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRegisterCreatesLocalIdentityAndInitialFarm(t *testing.T) {
	now := time.Date(2026, 8, 11, 10, 0, 0, 0, time.UTC)
	service, mock := newGuestLoginTestService(t, now)
	const userID = int64(8123)

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT 1 FROM auth_identities`).WithArgs("farmer_01").
		WillReturnRows(sqlmock.NewRows([]string{"1"}))
	mock.ExpectExec(`INSERT INTO accounts`).WithArgs("麦芽糖", now, now, now).
		WillReturnResult(sqlmock.NewResult(userID, 1))
	mock.ExpectExec(`UPDATE accounts SET farm_id=\? WHERE user_id=\?`).WithArgs(uint64(userID), uint64(userID)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO auth_identities`).WithArgs(uint64(userID), "farmer_01", sqlmock.AnyArg(), now, now, now).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO farm_snapshots`).WithArgs(uint64(userID), uint64(userID), sqlmock.AnyArg(), now, now).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO wallets`).WithArgs(uint64(userID), initCoinBalance, now, now).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO inventory_items`).WithArgs(uint64(userID), uint64(initSeedItemID), initSeedQuantity, now, now).
		WillReturnResult(sqlmock.NewResult(1, 1))
	expectRegisteredAccountLoad(mock, userID, "麦芽糖", now)
	mock.ExpectCommit()

	result, err := service.Register(t.Context(), " Farmer_01 ", "password-123", " 麦芽糖 ")
	if err != nil {
		t.Fatal(err)
	}
	if result.Account.UserID != userID || result.Account.AccountType != "REGISTERED" || result.Session.RefreshToken == "" {
		t.Fatalf("result=%+v", result)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPasswordLoginRejectsWrongPasswordAndCreatesSessionForCorrectPassword(t *testing.T) {
	now := time.Date(2026, 8, 11, 10, 0, 0, 0, time.UTC)
	service, mock := newGuestLoginTestService(t, now)
	hash, err := hashPassword("password-123")
	if err != nil {
		t.Fatal(err)
	}
	const userID = int64(8123)

	mock.ExpectQuery(`SELECT ai.user_id, ai.credential_hash`).WithArgs("farmer_01").
		WillReturnRows(sqlmock.NewRows([]string{"user_id", "credential_hash", "identity_status", "account_status"}).
			AddRow(userID, hash, "ACTIVE", "ACTIVE"))
	if _, err := service.PasswordLogin(t.Context(), "farmer_01", "wrong-password"); err == nil {
		t.Fatal("wrong password was accepted")
	}

	mock.ExpectQuery(`SELECT ai.user_id, ai.credential_hash`).WithArgs("farmer_01").
		WillReturnRows(sqlmock.NewRows([]string{"user_id", "credential_hash", "identity_status", "account_status"}).
			AddRow(userID, hash, "ACTIVE", "ACTIVE"))
	mock.ExpectBegin()
	mock.ExpectExec(`UPDATE accounts SET last_login_at=\?, updated_at=\? WHERE user_id=\?`).WithArgs(now, now, uint64(userID)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	expectRegisteredAccountLoad(mock, userID, "麦芽糖", now)
	mock.ExpectCommit()
	result, err := service.PasswordLogin(t.Context(), "farmer_01", "password-123")
	if err != nil {
		t.Fatal(err)
	}
	if result.Session.SessionID == "" || result.Session.ExpiresAt.Sub(now) != 7*24*time.Hour {
		t.Fatalf("session=%+v", result.Session)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
