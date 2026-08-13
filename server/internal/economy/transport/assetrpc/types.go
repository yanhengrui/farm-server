// Package assetrpc provides the private player-assets internal transport.
package assetrpc

type InventoryItemDTO struct {
	ItemType string `json:"item_type"`
	ItemID   int64  `json:"item_id,string"`
	Quantity int64  `json:"quantity"`
}

type AssetsDTO struct {
	CoinBalance int64              `json:"coin_balance"`
	Inventory   []InventoryItemDTO `json:"inventory"`
}

type GetReq struct {
	UserID int64 `json:"user_id"`
}

type GetResp struct {
	Assets *AssetsDTO `json:"assets,omitempty"`
	Err    *RPCErr    `json:"error,omitempty"`
}

type RPCErr struct {
	Code         string `json:"code"`
	Message      string `json:"message"`
	Reason       string `json:"reason,omitempty"`
	RetryAfterMs int64  `json:"retry_after_ms,omitempty"`
}
