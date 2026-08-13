// Package domain defines the catalog read model.
package domain

import (
	"context"
	"time"
)

// Unlock is one opaque catalog entry unlocked by a player.
type Unlock struct {
	CatalogKey string
	UnlockedAt time.Time
}

// Service reads a player's catalog unlocks.
type Service interface {
	ListCatalogUnlocks(ctx context.Context, userID int64) ([]Unlock, error)
}
