package infrastructure

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestLoadDisplayNamesFallsBackForWhitespace(t *testing.T) {
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
		WillReturnRows(sqlmock.NewRows([]string{"display_name"}).AddRow("   "))
	mock.ExpectQuery(`SELECT display_name FROM accounts`).WithArgs(uint64(121342)).
		WillReturnRows(sqlmock.NewRows([]string{"display_name"}).AddRow("Henry"))

	names := loadDisplayNames(t.Context(), tx, []int64{121343, 121342})
	if names[121343] != "121343" || names[121342] != "Henry" {
		t.Fatalf("unexpected names: %+v", names)
	}
	mock.ExpectRollback()
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
