package probe

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestMySQLReusesProvidedPool(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectPing()
	if err := MySQL(db)(context.Background()); err != nil {
		t.Fatalf("MySQL readiness: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestMySQLRejectsNilPool(t *testing.T) {
	if err := MySQL(nil)(context.Background()); err == nil {
		t.Fatal("expected nil pool error")
	}
}
