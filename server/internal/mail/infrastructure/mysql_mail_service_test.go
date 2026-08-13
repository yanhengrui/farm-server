package infrastructure

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	maildomain "github.com/photon/farm-server/server/internal/mail/domain"
)

type recordingMailboxNotifier struct{ summaries []maildomain.MailboxSummary }

func (n *recordingMailboxNotifier) NotifyMailboxChanged(_ context.Context, summary maildomain.MailboxSummary) error {
	n.summaries = append(n.summaries, summary)
	return nil
}

func TestSendMailUpdatesSummaryBeforeNotification(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	notifier := &recordingMailboxNotifier{}
	service := NewMySQLMailService(db).WithNotifier(notifier)

	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO mails`).
		WithArgs(uint64(42), nil, "SYSTEM", "hello", "body", nil, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(9, 1))
	mock.ExpectQuery(`SELECT unread_count, version FROM mailbox_states`).
		WithArgs(uint64(42)).
		WillReturnRows(sqlmock.NewRows([]string{"unread_count", "version"}).AddRow(3, 8))
	mock.ExpectCommit()

	mailID, err := service.SendMail(t.Context(), maildomain.SendMailReq{UserID: 42, MailType: maildomain.MailTypeSystem, Title: "hello", Content: "body"})
	if err != nil || mailID != 9 {
		t.Fatalf("mailID=%d err=%v", mailID, err)
	}
	if len(notifier.summaries) != 1 || notifier.summaries[0].UnreadCount != 3 || notifier.summaries[0].Version != 8 {
		t.Fatalf("notifications=%+v", notifier.summaries)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestMarkReadDecrementsOnlyOnUnreadTransition(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	notifier := &recordingMailboxNotifier{}
	service := NewMySQLMailService(db).WithNotifier(notifier)

	mock.ExpectBegin()
	mock.ExpectExec(`UPDATE mails SET status='READ'`).
		WithArgs(sqlmock.AnyArg(), uint64(9), uint64(42)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`SELECT unread_count, version FROM mailbox_states`).
		WithArgs(uint64(42)).
		WillReturnRows(sqlmock.NewRows([]string{"unread_count", "version"}).AddRow(2, 9))
	mock.ExpectCommit()
	if err := service.MarkRead(t.Context(), 9, 42); err != nil {
		t.Fatal(err)
	}

	mock.ExpectBegin()
	mock.ExpectExec(`UPDATE mails SET status='READ'`).
		WithArgs(sqlmock.AnyArg(), uint64(9), uint64(42)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()
	if err := service.MarkRead(t.Context(), 9, 42); err != nil {
		t.Fatal(err)
	}
	if len(notifier.summaries) != 1 || notifier.summaries[0].UnreadCount != 2 {
		t.Fatalf("notifications=%+v", notifier.summaries)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
