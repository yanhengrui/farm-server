package worker

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/photon/farm-server/server/internal/farm/application"
	"github.com/photon/farm-server/server/internal/farm/domain"
)

type scannerFarmClient struct {
	calls int
	last  domain.Command
	err   error
}

func (c *scannerFarmClient) SubmitCommand(_ context.Context, cmd domain.Command) (application.CommitResult, error) {
	c.calls++
	c.last = cmd
	return application.CommitResult{}, c.err
}

func expectClaim(t *testing.T, mock sqlmock.Sqlmock, rows *sqlmock.Rows, batch int) {
	t.Helper()
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT fs\.farm_id[\s\S]+ORDER BY fs\.next_pet_action_at, fs\.farm_id[\s\S]+FOR UPDATE SKIP LOCKED`).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), batch).
		WillReturnRows(rows)
}

func expectLease(mock sqlmock.Sqlmock, farmID int64) {
	mock.ExpectExec(`UPDATE farm_snapshots SET pet_scan_lease_owner = \?, pet_scan_lease_until = \? WHERE farm_id = \?`).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), uint64(farmID)).
		WillReturnResult(sqlmock.NewResult(0, 1))
}

func expectLeaseRenew(mock sqlmock.Sqlmock, farmID int64) {
	mock.ExpectExec(`UPDATE farm_snapshots SET pet_scan_lease_until = \? WHERE farm_id = \? AND pet_scan_lease_owner = \? AND pet_scan_lease_until > \?`).
		WithArgs(sqlmock.AnyArg(), uint64(farmID), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
}

func TestPetScannerFiltersDisabledPetsAndUsesOrderedSkipLockedClaim(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	expectClaim(t, mock, sqlmock.NewRows([]string{"farm_id", "owner_user_id", "snapshot", "next_pet_action_at"}), 10)
	mock.ExpectCommit()
	client := &scannerFarmClient{}
	result, err := NewPetScanner(db, client, slog.Default()).Scan(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if result.Claimed != 0 || client.calls != 0 {
		t.Fatalf("result=%+v calls=%d", result, client.calls)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPetScannerPassesPersistedScheduleToAuthority(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	scheduledAt := time.Now().UTC().Add(-time.Second).Truncate(time.Millisecond)
	snapshot := []byte(`{"plots":{"3":{"status":"GROWING","mature_at":"2020-01-01T00:00:00Z"}}}`)
	expectClaim(t, mock, sqlmock.NewRows([]string{"farm_id", "owner_user_id", "snapshot", "next_pet_action_at"}).AddRow(int64(42), int64(42), snapshot, scheduledAt), 10)
	expectLease(mock, 42)
	mock.ExpectCommit()
	expectLeaseRenew(mock, 42)
	client := &scannerFarmClient{}
	result, err := NewPetScanner(db, client, slog.Default()).Scan(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if result.Harvested != 1 || client.calls != 1 || client.last.PlotID != 3 || !client.last.PetScheduledAt.Equal(scheduledAt) {
		t.Fatalf("result=%+v calls=%d command=%+v", result, client.calls, client.last)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPetScannerNoMaturePlotReschedulesWithLeaseCAS(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	scheduledAt := time.Now().UTC().Add(-time.Second).Truncate(time.Millisecond)
	snapshot := []byte(`{"plots":{"3":{"status":"GROWING","mature_at":"2099-01-01T00:00:00Z"}}}`)
	expectClaim(t, mock, sqlmock.NewRows([]string{"farm_id", "owner_user_id", "snapshot", "next_pet_action_at"}).AddRow(int64(42), int64(42), snapshot, scheduledAt), 10)
	expectLease(mock, 42)
	mock.ExpectCommit()
	expectLeaseRenew(mock, 42)
	mock.ExpectExec(`UPDATE farm_snapshots[\s\S]+pet_scan_lease_owner = NULL[\s\S]+pet_scan_lease_owner = \?[\s\S]+pet_scan_lease_until > \?`).
		WithArgs(sqlmock.AnyArg(), uint64(42), scheduledAt, sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))

	result, err := NewPetScanner(db, &scannerFarmClient{}, slog.Default()).Scan(context.Background(), 10)
	if err != nil || result.Rescheduled != 1 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPetScannerSubmissionFailureIsCountedAndLeaseIsRetained(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	scheduledAt := time.Now().UTC().Add(-time.Second).Truncate(time.Millisecond)
	snapshot := []byte(`{"plots":{"3":{"status":"GROWING","mature_at":"2020-01-01T00:00:00Z"}}}`)
	expectClaim(t, mock, sqlmock.NewRows([]string{"farm_id", "owner_user_id", "snapshot", "next_pet_action_at"}).AddRow(int64(42), int64(42), snapshot, scheduledAt), 10)
	expectLease(mock, 42)
	mock.ExpectCommit()
	expectLeaseRenew(mock, 42)
	result, err := NewPetScanner(db, &scannerFarmClient{err: errors.New("farm unavailable")}, slog.Default()).Scan(context.Background(), 10)
	if err != nil || result.SubmitFailed != 1 || result.Harvested != 0 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPetLeaseTokensAreUnique(t *testing.T) {
	seen := make(map[string]struct{}, 32)
	for i := 0; i < 32; i++ {
		token, err := newPetLeaseToken()
		if err != nil {
			t.Fatal(err)
		}
		if _, exists := seen[token]; exists {
			t.Fatalf("duplicate lease token %q", token)
		}
		seen[token] = struct{}{}
	}
}
