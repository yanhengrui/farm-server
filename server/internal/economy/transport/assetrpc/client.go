package assetrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	rpcv1 "github.com/photon/farm-server/server/gen/farm/rpc/v1"
	"github.com/photon/farm-server/server/pkg/errcode"
	"github.com/photon/farm-server/server/pkg/rpcgrpc"
)

type Client struct {
	baseURL string
	http    *http.Client
	grpc    rpcv1.AssetServiceClient
	mode    rpcgrpc.Mode
}

func NewClient(baseURL string) *Client {
	return &Client{baseURL: baseURL, http: &http.Client{Timeout: 10 * time.Second}}
}

func (c *Client) WithGRPC(client rpcv1.AssetServiceClient, mode rpcgrpc.Mode) *Client {
	c.grpc, c.mode = client, mode
	return c
}

func (c *Client) GetPlayerAssets(ctx context.Context, userID int64) (*AssetsDTO, error) {
	if c.grpc != nil && c.mode != rpcgrpc.ModeHTTP {
		out, err := c.grpc.GetPlayerAssets(ctx, &rpcv1.GetPlayerAssetsRequest{UserId: userID})
		if err == nil {
			assets := &AssetsDTO{CoinBalance: out.CoinBalance, Inventory: make([]InventoryItemDTO, 0, len(out.Inventory))}
			for _, item := range out.Inventory {
				assets.Inventory = append(assets.Inventory, InventoryItemDTO{ItemType: item.ItemType, ItemID: item.ItemId, Quantity: item.Quantity})
			}
			return assets, nil
		}
		if c.mode != rpcgrpc.ModeGRPCFallback || !rpcgrpc.CanFallback(err) {
			return nil, rpcgrpc.FromError(err)
		}
	}
	body, _ := json.Marshal(GetReq{UserID: userID})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/rpc/player/assets", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http_do get_player_assets: %w", err)
	}
	defer resp.Body.Close()
	var out GetResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode_get_player_assets: %w", err)
	}
	if out.Err != nil {
		return nil, errcode.FromRemoteReason(out.Err.Code, out.Err.Message, out.Err.Reason, out.Err.RetryAfterMs)
	}
	if out.Assets == nil {
		return nil, errcode.New(errcode.Internal, "gamesvr: empty player assets")
	}
	if out.Assets.Inventory == nil {
		out.Assets.Inventory = []InventoryItemDTO{}
	}
	return out.Assets, nil
}
