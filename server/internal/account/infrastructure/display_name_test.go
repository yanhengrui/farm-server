package infrastructure

import (
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestLoadDisplayNameReturnsRequiredValue(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	query := regexp.QuoteMeta(`
		SELECT COALESCE(NULLIF(TRIM(display_name), ''), CAST(user_id AS CHAR))
		FROM accounts
		WHERE user_id = ?`)
	mock.ExpectQuery(query).WithArgs(uint64(121343)).
		WillReturnRows(sqlmock.NewRows([]string{"display_name"}).AddRow("小麦糖"))

	service := NewMySQLAccountService(db, nil, nil, nil)
	displayName, err := service.LoadDisplayName(t.Context(), 121343)
	if err != nil {
		t.Fatalf("LoadDisplayName: %v", err)
	}
	if displayName != "小麦糖" {
		t.Fatalf("display_name=%q", displayName)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestLoadDisplayNameRejectsEmptyResult(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectQuery(`(?s)SELECT COALESCE.*FROM accounts.*WHERE user_id = \?`).WithArgs(uint64(121343)).
		WillReturnRows(sqlmock.NewRows([]string{"display_name"}).AddRow(""))

	service := NewMySQLAccountService(db, nil, nil, nil)
	if _, err := service.LoadDisplayName(t.Context(), 121343); err == nil {
		t.Fatal("expected empty display name to be rejected")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestLoadDisplayNameFallsBackToUserIDWhenAccountIsMissing(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectQuery(`(?s)SELECT COALESCE.*FROM accounts.*WHERE user_id = \?`).WithArgs(uint64(121347)).
		WillReturnRows(sqlmock.NewRows([]string{"display_name"}))

	service := NewMySQLAccountService(db, nil, nil, nil)
	displayName, err := service.LoadDisplayName(t.Context(), 121347)
	if err != nil {
		t.Fatalf("LoadDisplayName: %v", err)
	}
	if displayName != "121347" {
		t.Fatalf("display_name=%q", displayName)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
