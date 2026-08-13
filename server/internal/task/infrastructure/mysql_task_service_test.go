package infrastructure_test

import (
	"context"
	"errors"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"

	taskinfra "github.com/photon/farm-server/server/internal/task/infrastructure"
	"github.com/photon/farm-server/server/pkg/errcode"
)

func assertTaskErrCode(t *testing.T, err error, want errcode.Code) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error %s, got nil", want)
	}
	var e *errcode.Error
	if !errors.As(err, &e) {
		t.Fatalf("expected *errcode.Error, got %T: %v", err, err)
	}
	if e.Code != want {
		t.Errorf("expected code %s, got %s", want, e.Code)
	}
}

func TestIncrProgress_UnknownKey_Skipped(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// 未知 task_key 静默跳过，不执行任何 SQL。
	svc := taskinfra.NewMySQLTaskService(db)
	if err := svc.IncrProgress(context.Background(), 42, "unknown_task", 1); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestIncrProgress_KnownKey_ExecSQL(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectExec(`INSERT INTO player_tasks`).
		WillReturnResult(sqlmock.NewResult(1, 1))

	svc := taskinfra.NewMySQLTaskService(db)
	if err := svc.IncrProgress(context.Background(), 42, "plant_10", 1); err != nil {
		t.Fatalf("IncrProgress: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestClaimReward_AlreadyClaimed(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectBegin()
	// UPDATE 影响 0 行（状态不是 COMPLETED）。
	mock.ExpectExec(`UPDATE player_tasks`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	// SELECT 检查当前状态：REWARD_CLAIMED。
	mock.ExpectQuery(`SELECT status FROM player_tasks`).
		WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("REWARD_CLAIMED"))
	mock.ExpectRollback()

	svc := taskinfra.NewMySQLTaskService(db)
	_, err = svc.ClaimReward(context.Background(), 42, "harvest_5")
	assertTaskErrCode(t, err, errcode.TaskAlreadyClaimed)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestClaimReward_NotCompleted(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectExec(`UPDATE player_tasks`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	// 状态是 ACTIVE（未完成）。
	mock.ExpectQuery(`SELECT status FROM player_tasks`).
		WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("ACTIVE"))
	mock.ExpectRollback()

	svc := taskinfra.NewMySQLTaskService(db)
	_, err = svc.ClaimReward(context.Background(), 42, "harvest_5")
	assertTaskErrCode(t, err, errcode.TaskNotCompleted)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestClaimReward_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectBegin()
	// UPDATE 影响 1 行（COMPLETED → REWARD_CLAIMED）。
	mock.ExpectExec(`UPDATE player_tasks`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	// UPDATE wallets（coin_reward=80）。
	mock.ExpectExec(`UPDATE wallets`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	svc := taskinfra.NewMySQLTaskService(db)
	coinReward, err := svc.ClaimReward(context.Background(), 42, "harvest_5")
	if err != nil {
		t.Fatalf("ClaimReward: %v", err)
	}
	if coinReward != 80 {
		t.Fatalf("expected coinReward=80, got %d", coinReward)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestClaimReward_UnknownKey(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	svc := taskinfra.NewMySQLTaskService(db)
	_, err = svc.ClaimReward(context.Background(), 42, "unknown_task")
	assertTaskErrCode(t, err, errcode.CommonInvalidArgument)
}
