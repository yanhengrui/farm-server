// Package infrastructure 提供 pet 领域的 MySQL 实现。
// P0：小鸡宠物，价格硬编码 200 金币，购买后每 30s 自动收割一块成熟地块。
package infrastructure

import (
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"
	"math/big"
	"time"

	_ "github.com/go-sql-driver/mysql"

	petdomain "github.com/photon/farm-server/server/internal/pet/domain"
	"github.com/photon/farm-server/server/pkg/errcode"
	"github.com/photon/farm-server/server/pkg/mysqlretry"
)

// MySQLPetService 提供宠物购买功能。
type MySQLPetService struct {
	db *sql.DB
}

// NewMySQLPetService 构造 MySQLPetService。
func NewMySQLPetService(db *sql.DB) *MySQLPetService {
	return &MySQLPetService{db: db}
}

// BuyPet 购买宠物：扣金币 + 写 player_pets + 写 next_pet_action_at（单事务）。
// 已有宠物时返回 PetAlreadyEquipped 错误。
func (s *MySQLPetService) BuyPet(ctx context.Context, userID int64) error {
	return mysqlretry.Do(ctx, mysqlretry.IsTransient, func() error {
		return s.buyPetOnce(ctx, userID)
	})
}

func (s *MySQLPetService) buyPetOnce(ctx context.Context, userID int64) error {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return fmt.Errorf("begin_tx buy_pet: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	now := time.Now().UTC()

	// Global order for operations touching all three records:
	// farm -> wallet -> pet. Lock the farm first and retain the unique pet key
	// as the final concurrency guard.
	var farmExists int
	if err := tx.QueryRowContext(ctx,
		`SELECT 1 FROM farm_snapshots WHERE farm_id = ? FOR UPDATE`,
		uint64(userID),
	).Scan(&farmExists); err != nil {
		if err == sql.ErrNoRows {
			return errcode.New(errcode.FarmNotFound, "farm not found")
		}
		return fmt.Errorf("lock_farm_for_pet_purchase: %w", err)
	}

	// 1. 检查是否已有宠物（自然唯一键保护）。
	var dummy int
	err = tx.QueryRowContext(ctx,
		`SELECT 1 FROM player_pets WHERE user_id = ? LIMIT 1`,
		uint64(userID),
	).Scan(&dummy)
	if err == nil {
		return errcode.New(errcode.PetAlreadyEquipped, "already have a pet")
	}
	if err != sql.ErrNoRows {
		return fmt.Errorf("check_pet: %w", err)
	}

	// 2. 扣金币（加行锁）。
	var balance int64
	if err := tx.QueryRowContext(ctx,
		`SELECT coin_balance FROM wallets WHERE user_id = ? FOR UPDATE`,
		uint64(userID),
	).Scan(&balance); err != nil {
		return fmt.Errorf("load_wallet: %w", err)
	}
	if balance < petdomain.PetPrice {
		return errcode.New(errcode.EconomyInsufficient,
			fmt.Sprintf("金币不足：需要 %d，剩余 %d", petdomain.PetPrice, balance))
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE wallets SET coin_balance = coin_balance - ?, updated_at = ? WHERE user_id = ?`,
		petdomain.PetPrice, now, uint64(userID),
	); err != nil {
		return fmt.Errorf("deduct_wallet: %w", err)
	}

	// 3. 写 player_pets。
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO player_pets (user_id, pet_type, status, purchased_at) VALUES (?, ?, 'ACTIVE', ?)`,
		uint64(userID), string(petdomain.PetTypeChicken), now,
	); err != nil {
		return fmt.Errorf("insert_pet: %w", err)
	}

	// 4. 首次启用分散 0~3 秒，避免同一批购买的宠物同时到期。
	nextActionAt, err := nextPetActionAt(now)
	if err != nil {
		return fmt.Errorf("random_pet_schedule: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE farm_snapshots SET next_pet_action_at = ?, pet_scan_lease_owner = NULL, pet_scan_lease_until = NULL WHERE farm_id = ?`,
		nextActionAt, uint64(userID),
	); err != nil {
		return fmt.Errorf("set_next_pet_action: %w", err)
	}

	return tx.Commit()
}

// HasPet 检查用户是否已有宠物。
func (s *MySQLPetService) HasPet(ctx context.Context, userID int64) (bool, error) {
	status, err := s.GetStatus(ctx, userID)
	return status.HasPet, err
}

// GetStatus returns ownership and the persisted auto-harvest switch.
func (s *MySQLPetService) GetStatus(ctx context.Context, userID int64) (petdomain.PlayerStatus, error) {
	var enabled bool
	err := s.db.QueryRowContext(ctx,
		`SELECT auto_harvest_enabled FROM player_pets WHERE user_id = ? AND status = 'ACTIVE' LIMIT 1`,
		uint64(userID),
	).Scan(&enabled)
	if err == sql.ErrNoRows {
		return petdomain.PlayerStatus{}, nil
	}
	if err != nil {
		return petdomain.PlayerStatus{}, fmt.Errorf("get_pet_status: %w", err)
	}
	return petdomain.PlayerStatus{HasPet: true, AutoHarvestEnabled: enabled}, nil
}

// SetAutoHarvest atomically updates the switch and its farm schedule.
func (s *MySQLPetService) SetAutoHarvest(ctx context.Context, userID int64, enabled bool) error {
	return mysqlretry.Do(ctx, mysqlretry.IsTransient, func() error {
		return s.setAutoHarvestOnce(ctx, userID, enabled)
	})
}

func (s *MySQLPetService) setAutoHarvestOnce(ctx context.Context, userID int64, enabled bool) error {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return fmt.Errorf("begin_tx set_auto_harvest: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	// Global order for operations touching both records: farm -> pet.
	var farmExists int
	if err := tx.QueryRowContext(ctx,
		`SELECT 1 FROM farm_snapshots WHERE farm_id = ? FOR UPDATE`,
		uint64(userID),
	).Scan(&farmExists); err != nil {
		if err == sql.ErrNoRows {
			return errcode.New(errcode.FarmNotFound, "farm not found")
		}
		return fmt.Errorf("lock_farm_for_pet_switch: %w", err)
	}

	var current bool
	var status string
	if err := tx.QueryRowContext(ctx,
		`SELECT status, auto_harvest_enabled FROM player_pets WHERE user_id = ? FOR UPDATE`,
		uint64(userID),
	).Scan(&status, &current); err != nil {
		if err == sql.ErrNoRows {
			return errcode.New(errcode.PetNotOwned, "pet not owned")
		}
		return fmt.Errorf("lock_pet_auto_harvest: %w", err)
	}
	if status != string(petdomain.PetStatusActive) {
		return errcode.New(errcode.PetNotOwned, "active pet not owned")
	}
	if current == enabled && enabled {
		return tx.Commit()
	}
	now := time.Now().UTC()
	if _, err := tx.ExecContext(ctx,
		`UPDATE player_pets SET auto_harvest_enabled = ? WHERE user_id = ?`,
		enabled, uint64(userID),
	); err != nil {
		return fmt.Errorf("update_pet_auto_harvest: %w", err)
	}
	var next any
	if enabled {
		next, err = nextPetActionAt(now)
		if err != nil {
			return fmt.Errorf("random_pet_schedule: %w", err)
		}
	}
	result, err := tx.ExecContext(ctx,
		`UPDATE farm_snapshots SET next_pet_action_at = ?, pet_scan_lease_owner = NULL, pet_scan_lease_until = NULL WHERE farm_id = ?`,
		next, uint64(userID),
	)
	if err != nil {
		return fmt.Errorf("update_pet_schedule: %w", err)
	}
	if affected, affectedErr := result.RowsAffected(); affectedErr == nil && affected == 0 {
		return errcode.New(errcode.FarmNotFound, "farm not found")
	}
	return tx.Commit()
}

// nextPetActionAt is deliberately called only when the feature is enabled or
// bought. Subsequent 30-second cycles retain this phase offset, rather than
// repeatedly converging farms that were enabled in the same second.
func nextPetActionAt(now time.Time) (time.Time, error) {
	jitter, err := rand.Int(rand.Reader, big.NewInt(4))
	if err != nil {
		return time.Time{}, err
	}
	return now.Add(petdomain.PetAutoHarvestInterval + time.Duration(jitter.Int64())*time.Second), nil
}
