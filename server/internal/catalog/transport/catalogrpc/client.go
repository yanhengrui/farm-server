package catalogrpc

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
	grpc    rpcv1.CatalogServiceClient
	mode    rpcgrpc.Mode
}

func NewClient(baseURL string) *Client {
	return &Client{baseURL: baseURL, http: &http.Client{Timeout: 10 * time.Second}}
}

func (c *Client) WithGRPC(client rpcv1.CatalogServiceClient, mode rpcgrpc.Mode) *Client {
	c.grpc, c.mode = client, mode
	return c
}

func (c *Client) ListCatalogUnlocks(ctx context.Context, userID int64) ([]UnlockDTO, error) {
	if c.grpc != nil && c.mode != rpcgrpc.ModeHTTP {
		out, err := c.grpc.ListCatalogUnlocks(ctx, &rpcv1.ListCatalogUnlocksRequest{UserId: userID})
		if err == nil {
			items := make([]UnlockDTO, 0, len(out.Unlocks))
			for _, item := range out.Unlocks {
				var unlockedAt time.Time
				if item.UnlockedAt != nil && item.UnlockedAt.IsValid() {
					unlockedAt = item.UnlockedAt.AsTime()
				}
				items = append(items, UnlockDTO{CatalogKey: item.CatalogKey, UnlockedAt: unlockedAt})
			}
			return items, nil
		}
		if c.mode != rpcgrpc.ModeGRPCFallback || !rpcgrpc.CanFallback(err) {
			return nil, rpcgrpc.FromError(err)
		}
	}
	body, _ := json.Marshal(ListReq{UserID: userID})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/rpc/catalog/list", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http_do list_catalog_unlocks: %w", err)
	}
	defer resp.Body.Close()
	var out ListResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode_list_catalog_unlocks: %w", err)
	}
	if out.Err != nil {
		return nil, errcode.FromRemoteReason(out.Err.Code, out.Err.Message, out.Err.Reason, out.Err.RetryAfterMs)
	}
	if out.Unlocks == nil {
		out.Unlocks = []UnlockDTO{}
	}
	return out.Unlocks, nil
}
