// Package infrastructure provides authoritative economy reads from MySQL.
package infrastructure

import (
	"context"
	"database/sql"
	"fmt"

	economydomain "github.com/photon/farm-server/server/internal/economy/domain"
	"github.com/photon/farm-server/server/pkg/errcode"
)

type MySQLAssetService struct{ db *sql.DB }

func NewMySQLAssetService(db *sql.DB) *MySQLAssetService { return &MySQLAssetService{db: db} }

func (s *MySQLAssetService) GetPlayerAssets(ctx context.Context, userID int64) (economydomain.Assets, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return economydomain.Assets{}, fmt.Errorf("begin_assets_read user_id=%d: %w", userID, err)
	}
	defer tx.Rollback() //nolint:errcheck

	var out economydomain.Assets
	if err := tx.QueryRowContext(ctx, `SELECT coin_balance FROM wallets WHERE user_id = ?`, uint64(userID)).Scan(&out.CoinBalance); err != nil {
		if err == sql.ErrNoRows {
			return economydomain.Assets{}, errcode.New(errcode.Internal, "player assets not initialized")
		}
		return economydomain.Assets{}, fmt.Errorf("load_asset_wallet user_id=%d: %w", userID, err)
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT item_type, item_id, quantity
		FROM inventory_items
		WHERE user_id = ? AND quantity > 0 AND deleted_at IS NULL
		ORDER BY item_type, item_id`, uint64(userID))
	if err != nil {
		return economydomain.Assets{}, fmt.Errorf("load_asset_inventory user_id=%d: %w", userID, err)
	}
	defer rows.Close()
	out.Inventory = make([]economydomain.InventoryItem, 0)
	for rows.Next() {
		var item economydomain.InventoryItem
		if err := rows.Scan(&item.ItemType, &item.ItemID, &item.Quantity); err != nil {
			return economydomain.Assets{}, fmt.Errorf("scan_asset_inventory user_id=%d: %w", userID, err)
		}
		out.Inventory = append(out.Inventory, item)
	}
	if err := rows.Err(); err != nil {
		return economydomain.Assets{}, fmt.Errorf("iterate_asset_inventory user_id=%d: %w", userID, err)
	}
	if err := tx.Commit(); err != nil {
		return economydomain.Assets{}, fmt.Errorf("commit_assets_read user_id=%d: %w", userID, err)
	}
	return out, nil
}

var _ economydomain.AssetService = (*MySQLAssetService)(nil)
