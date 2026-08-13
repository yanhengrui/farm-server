package infrastructure

import (
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestInsertFriendOutboxUsesNumericAggregateIDAndPairPartitionKey(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectBegin()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectQuery(`SELECT display_name FROM accounts`).WithArgs(uint64(84)).
		WillReturnRows(sqlmock.NewRows([]string{"display_name"}).AddRow("inviter"))
	mock.ExpectQuery(`SELECT display_name FROM accounts`).WithArgs(uint64(42)).
		WillReturnRows(sqlmock.NewRows([]string{"display_name"}).AddRow("accepter"))

	now := time.Date(2026, time.August, 10, 8, 0, 0, 0, time.UTC)
	mock.ExpectExec(`(?s)INSERT INTO outbox_events`).
		WithArgs(
			sqlmock.AnyArg(), "social", uint64(42), "42:84",
			"social.friend_accepted.v1", "1.0", sqlmock.AnyArg(),
			now, now, now,
		).
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := insertFriendOutbox(t.Context(), tx, 84, 42, 42, 84, now); err != nil {
		t.Fatalf("insertFriendOutbox: %v", err)
	}
	mock.ExpectRollback()
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestListFriendsUsesSingleJoinAndReturnsDisplayNames(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const query = `
		SELECT related.friend_id,
		       COALESCE(NULLIF(TRIM(a.display_name), ''), CAST(related.friend_id AS CHAR)) AS display_name
		FROM (
			SELECT f.friendship_id,
			       CASE WHEN f.user_id_a = ? THEN f.user_id_b ELSE f.user_id_a END AS friend_id
			FROM friendships AS f
			WHERE f.user_id_a = ? OR f.user_id_b = ?
		) AS related
		LEFT JOIN accounts AS a ON a.user_id = related.friend_id
		ORDER BY related.friendship_id
	`
	mock.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(uint64(42), uint64(42), uint64(42)).
		WillReturnRows(sqlmock.NewRows([]string{"user_id", "display_name"}).
			AddRow(uint64(121343), "小麦糖").
			AddRow(uint64(121342), "Henry"))

	service := NewMySQLSocialService(db, nil)
	friends, err := service.ListFriends(t.Context(), 42)
	if err != nil {
		t.Fatalf("ListFriends: %v", err)
	}
	if len(friends) != 2 || friends[0].UserID != 121343 || friends[0].DisplayName != "小麦糖" || friends[1].UserID != 121342 || friends[1].DisplayName != "Henry" {
		t.Fatalf("unexpected friends: %+v", friends)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expected exactly one JOIN query: %v", err)
	}
}

func TestListFriendsKeepsOrphanRelationshipWithIDFallback(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectQuery(`(?s)SELECT related\.friend_id,.*FROM \(.*friendships AS f.*\) AS related.*LEFT JOIN accounts AS a`).
		WithArgs(uint64(42), uint64(42), uint64(42)).
		WillReturnRows(sqlmock.NewRows([]string{"friend_id", "display_name"}).AddRow(uint64(121343), "121343"))

	service := NewMySQLSocialService(db, nil)
	friends, err := service.ListFriends(t.Context(), 42)
	if err != nil {
		t.Fatalf("ListFriends: %v", err)
	}
	if len(friends) != 1 || friends[0].DisplayName != "121343" {
		t.Fatalf("orphan friendship was not preserved: %+v", friends)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestLoadDisplayNameFallsBackForWhitespace(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectBegin()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectQuery(`SELECT display_name FROM accounts`).WithArgs(uint64(121343)).
		WillReturnRows(sqlmock.NewRows([]string{"display_name"}).AddRow(" \t "))
	if got := loadDisplayName(t.Context(), tx, 121343); got != "121343" {
		t.Fatalf("display name fallback=%q", got)
	}
	mock.ExpectRollback()
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
