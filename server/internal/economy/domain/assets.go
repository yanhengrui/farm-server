// Package domain defines the private player economy read model.
package domain

import "context"

type InventoryItem struct {
	ItemType string
	ItemID   int64
	Quantity int64
}

type Assets struct {
	CoinBalance int64
	Inventory   []InventoryItem
}

type AssetService interface {
	GetPlayerAssets(ctx context.Context, userID int64) (Assets, error)
}
