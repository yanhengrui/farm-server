package infrastructure

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	socialevents "github.com/photon/farm-server/server/contracts/events/socialv1"
)

func TestFriendEdgeSagaBeginRetryReusesPersistedSourceEventID(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const persistedID = "019febfce09c776d896cbc95a03653a0"
	const retryCandidateID = "019febfce09c776d896cbc95a03653a1"
	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO friendship_edges`).
		WithArgs(uint64(11), uint64(22), retryCandidateID, sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`SELECT state, source_event_id FROM friendship_edges`).
		WithArgs(uint64(11), uint64(22)).
		WillReturnRows(sqlmock.NewRows([]string{"state", "source_event_id"}).AddRow("PENDING", persistedID))
	mock.ExpectExec(`INSERT INTO outbox_events`).
		WithArgs(persistedID, uint64(11), "22", string(socialevents.EventTypeFriendEdgeRequested), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	if err := NewFriendEdgeSaga(db).BeginWithDisplayNames(t.Context(), 11, 22, retryCandidateID, "source", "target"); err != nil {
		t.Fatalf("retry Begin: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestFriendEdgeSagaApplyRemoteDuplicateDoesNotEmitSecondACK(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO friendship_edges`).
		WithArgs(uint64(2), uint64(1), "019febfce09c776d896cbc95a03653b0", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 0)) // existing ACTIVE edge: no mutation
	mock.ExpectCommit()

	saga := NewFriendEdgeSaga(db)
	if err := saga.ApplyRemote(t.Context(), 1, 2, "019febfce09c776d896cbc95a03653b0"); err != nil {
		t.Fatalf("apply duplicate remote edge: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("duplicate delivery must not insert another ACK: %v", err)
	}
}

func TestFriendEdgeSagaApplyRemoteTransitionEmitsOneACK(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO friendship_edges`).
		WithArgs(uint64(2), uint64(1), "019febfce09c776d896cbc95a03653b1", sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 2)) // PENDING -> ACTIVE
	mock.ExpectExec(`INSERT INTO outbox_events`).
		WithArgs(sqlmock.AnyArg(), uint64(2), "1", string(socialevents.EventTypeFriendEdgeApplied), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	saga := NewFriendEdgeSaga(db)
	if err := saga.ApplyRemote(t.Context(), 1, 2, "019febfce09c776d896cbc95a03653b1"); err != nil {
		t.Fatalf("apply pending remote edge: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestFriendEdgeSagaConfirmSourceEmitsFriendAcceptedAtomically(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const sourceEventID = "019febfce09c776d896cbc95a03653b2"
	mock.ExpectBegin()
	mock.ExpectExec(`UPDATE friendship_edges SET state='ACTIVE'`).
		WithArgs(sqlmock.AnyArg(), uint64(11), uint64(22), sourceEventID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`(?s)INSERT INTO outbox_events`).
		WithArgs(sqlmock.AnyArg(), "social", uint64(11), "11:22", string(socialevents.EventTypeFriendAccepted), "1.0", sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	if err := NewFriendEdgeSaga(db).ConfirmSourceWithDisplayNames(t.Context(), 11, 22, sourceEventID, "accepter", "inviter"); err != nil {
		t.Fatalf("ConfirmSourceWithDisplayNames: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
