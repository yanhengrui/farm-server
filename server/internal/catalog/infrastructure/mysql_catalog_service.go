// Package infrastructure provides the MySQL catalog read service.
package infrastructure

import (
	"context"
	"database/sql"
	"fmt"

	catalogdomain "github.com/photon/farm-server/server/internal/catalog/domain"
)

// MySQLCatalogService reads catalog_unlocks without mutating projection state.
type MySQLCatalogService struct{ db *sql.DB }

func NewMySQLCatalogService(db *sql.DB) *MySQLCatalogService {
	return &MySQLCatalogService{db: db}
}

func (s *MySQLCatalogService) ListCatalogUnlocks(ctx context.Context, userID int64) ([]catalogdomain.Unlock, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT catalog_key, unlocked_at
		FROM catalog_unlocks
		WHERE user_id = ?
		ORDER BY catalog_key`, uint64(userID))
	if err != nil {
		return nil, fmt.Errorf("list_catalog_unlocks user_id=%d: %w", userID, err)
	}
	defer rows.Close()

	items := make([]catalogdomain.Unlock, 0)
	for rows.Next() {
		var item catalogdomain.Unlock
		if err := rows.Scan(&item.CatalogKey, &item.UnlockedAt); err != nil {
			return nil, fmt.Errorf("scan_catalog_unlock user_id=%d: %w", userID, err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate_catalog_unlocks user_id=%d: %w", userID, err)
	}
	return items, nil
}

var _ catalogdomain.Service = (*MySQLCatalogService)(nil)
