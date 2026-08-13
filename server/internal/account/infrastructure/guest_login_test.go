package infrastructure

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/alicebob/miniredis/v2"
	"github.com/photon/farm-server/server/pkg/clock"
	"github.com/photon/farm-server/server/pkg/errcode"
	"github.com/photon/farm-server/server/pkg/redisstore"
	"github.com/redis/go-redis/v9"
)

func newGuestLoginTestService(t *testing.T, now time.Time) (*MySQLAccountService, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	redisServer := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	return NewMySQLAccountService(
		db,
		clock.NewFixed(now),
		[]byte("guest-login-test-secret"),
		redisstore.NewRefreshStore(rdb),
	), mock
}

func expectGuestIdentityLookup(mock sqlmock.Sqlmock, deviceID string, userID *int64) {
	rows := sqlmock.NewRows([]string{"user_id"})
	if userID != nil {
		rows.AddRow(*userID)
	}
	mock.ExpectQuery(`SELECT user_id FROM auth_identities`).
		WithArgs(deviceID).
		WillReturnRows(rows)
}

func expectAccountLoad(mock sqlmock.Sqlmock, userID int64, displayName string, now time.Time) {
	mock.ExpectQuery(`SELECT user_id, COALESCE\(display_name,''\), account_type, status, farm_id, created_at`).
		WithArgs(uint64(userID)).
		WillReturnRows(sqlmock.NewRows([]string{
			"user_id", "display_name", "account_type", "status", "farm_id", "created_at",
		}).AddRow(userID, displayName, "GUEST", "ACTIVE", userID, now))
}

func expectNewGuestAccount(mock sqlmock.Sqlmock, userID int64, deviceID, displayName string, now time.Time) {
	mock.ExpectExec(`INSERT INTO accounts`).
		WithArgs(displayName, now, now).
		WillReturnResult(sqlmock.NewResult(userID, 1))
	mock.ExpectExec(`UPDATE accounts SET farm_id=\? WHERE user_id=\?`).
		WithArgs(uint64(userID), uint64(userID)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO auth_identities`).
		WithArgs(uint64(userID), deviceID, now, now).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO farm_snapshots`).
		WithArgs(uint64(userID), uint64(userID), sqlmock.AnyArg(), now, now).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO wallets`).
		WithArgs(uint64(userID), initCoinBalance, now, now).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO inventory_items`).
		WithArgs(uint64(userID), uint64(initSeedItemID), initSeedQuantity, now, now).
		WillReturnResult(sqlmock.NewResult(1, 1))
}

func TestGuestLoginCreatesAccountWithSubmittedDisplayName(t *testing.T) {
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	service, mock := newGuestLoginTestService(t, now)
	const (
		deviceID = "device-new-guest"
		userID   = int64(121347)
	)

	mock.ExpectBegin()
	expectGuestIdentityLookup(mock, deviceID, nil)
	expectNewGuestAccount(mock, userID, deviceID, "小麦糖", now)
	expectAccountLoad(mock, userID, "小麦糖", now)
	mock.ExpectCommit()

	result, err := service.GuestLogin(t.Context(), deviceID, "  小麦糖  ")
	if err != nil {
		t.Fatalf("GuestLogin: %v", err)
	}
	if result.Account.DisplayName != "小麦糖" {
		t.Fatalf("display_name=%q", result.Account.DisplayName)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestGuestLoginLegacyClientGetsGuestNameInsteadOfUserID(t *testing.T) {
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	service, mock := newGuestLoginTestService(t, now)
	const (
		deviceID    = "legacy-device"
		userID      = int64(121347)
		displayName = "Guest_legacy-d"
	)

	mock.ExpectBegin()
	expectGuestIdentityLookup(mock, deviceID, nil)
	expectNewGuestAccount(mock, userID, deviceID, displayName, now)
	expectAccountLoad(mock, userID, displayName, now)
	mock.ExpectCommit()

	result, err := service.GuestLogin(t.Context(), deviceID, "")
	if err != nil {
		t.Fatalf("GuestLogin: %v", err)
	}
	if result.Account.DisplayName != displayName {
		t.Fatalf("display_name=%q", result.Account.DisplayName)
	}
	if result.Account.DisplayName == "121347" {
		t.Fatal("normal guest display name must not use the user ID fallback")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestGuestLoginUpdatesExistingGuestWithNonEmptyDisplayName(t *testing.T) {
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	service, mock := newGuestLoginTestService(t, now)
	const (
		deviceID = "device-existing-guest"
		userID   = int64(121347)
	)

	mock.ExpectBegin()
	expectGuestIdentityLookup(mock, deviceID, ptrTo(userID))
	mock.ExpectExec(`(?s)UPDATE accounts.*SET display_name = \?, row_version = row_version \+ 1, updated_at = \?.*WHERE user_id = \? AND account_type = 'GUEST'`).
		WithArgs("Henry", now, uint64(userID)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	expectAccountLoad(mock, userID, "Henry", now)
	mock.ExpectCommit()

	result, err := service.GuestLogin(t.Context(), deviceID, " Henry ")
	if err != nil {
		t.Fatalf("GuestLogin: %v", err)
	}
	if result.Account.DisplayName != "Henry" {
		t.Fatalf("display_name=%q", result.Account.DisplayName)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestGuestLoginPreservesExistingNameWhenSubmittedNameIsBlank(t *testing.T) {
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	service, mock := newGuestLoginTestService(t, now)
	const (
		deviceID = "device-existing-guest"
		userID   = int64(121347)
	)

	mock.ExpectBegin()
	expectGuestIdentityLookup(mock, deviceID, ptrTo(userID))
	expectAccountLoad(mock, userID, "原昵称", now)
	mock.ExpectCommit()

	result, err := service.GuestLogin(t.Context(), deviceID, " \t ")
	if err != nil {
		t.Fatalf("GuestLogin: %v", err)
	}
	if result.Account.DisplayName != "原昵称" {
		t.Fatalf("display_name=%q", result.Account.DisplayName)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestGuestLoginRejectsDisplayNameLongerThanSchemaLimit(t *testing.T) {
	service := NewMySQLAccountService(nil, clock.NewFixed(time.Now()), nil, nil)
	_, err := service.GuestLogin(t.Context(), "device", strings.Repeat("麦", 65))
	if err == nil {
		t.Fatal("expected display_name validation error")
	}
	var coded *errcode.Error
	if !errors.As(err, &coded) || coded.Code != errcode.CommonInvalidArgument {
		t.Fatalf("expected COMMON_INVALID_ARGUMENT, got %v", err)
	}
}

func ptrTo[T any](value T) *T { return &value }
