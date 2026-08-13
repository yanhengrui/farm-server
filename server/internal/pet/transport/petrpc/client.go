// Package petrpc — Client 侧，由 gatesvr 使用。
package petrpc

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

// Client 通过 HTTP/JSON 调用 gamesvr 宠物服务。
type Client struct {
	baseURL string
	http    *http.Client
	grpc    rpcv1.PetServiceClient
	mode    rpcgrpc.Mode
}

func (c *Client) WithGRPC(client rpcv1.PetServiceClient, mode rpcgrpc.Mode) *Client {
	c.grpc, c.mode = client, mode
	return c
}

// NewClient 构造 Client。
func NewClient(baseURL string) *Client {
	return &Client{baseURL: baseURL, http: &http.Client{Timeout: 10 * time.Second}}
}

// BuyPet 调用 gamesvr 购买宠物。
func (c *Client) BuyPet(ctx context.Context, userID int64) error {
	if c.grpc != nil && c.mode != rpcgrpc.ModeHTTP {
		_, err := c.grpc.BuyPet(ctx, &rpcv1.BuyPetRequest{UserId: userID})
		if err == nil {
			return nil
		}
		if c.mode != rpcgrpc.ModeGRPCFallback || !rpcgrpc.CanFallback(err) {
			return rpcgrpc.FromError(err)
		}
	}
	body, _ := json.Marshal(BuyPetReq{UserID: userID})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/rpc/pet/buy", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("http_do buy_pet: %w", err)
	}
	defer resp.Body.Close()

	var out BuyPetResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return fmt.Errorf("decode_buy_pet_resp: %w", err)
	}
	if out.Err != nil {
		return errcode.FromRemoteReason(out.Err.Code, out.Err.Message, out.Err.Reason, out.Err.RetryAfterMs)
	}
	return nil
}

// HasPet 调用 gamesvr 查询用户是否有宠物。
func (c *Client) HasPet(ctx context.Context, userID int64) (bool, error) {
	status, err := c.GetStatus(ctx, userID)
	return status.HasPet, err
}

func (c *Client) GetStatus(ctx context.Context, userID int64) (StatusDTO, error) {
	if c.grpc != nil && c.mode != rpcgrpc.ModeHTTP {
		out, err := c.grpc.HasPet(ctx, &rpcv1.HasPetRequest{UserId: userID})
		if err == nil {
			return StatusDTO{HasPet: out.HasPet, AutoHarvestEnabled: out.AutoHarvestEnabled}, nil
		}
		if c.mode != rpcgrpc.ModeGRPCFallback || !rpcgrpc.CanFallback(err) {
			return StatusDTO{}, rpcgrpc.FromError(err)
		}
	}
	body, _ := json.Marshal(HasPetReq{UserID: userID})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/rpc/pet/has", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return StatusDTO{}, fmt.Errorf("http_do has_pet: %w", err)
	}
	defer resp.Body.Close()

	var out HasPetResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return StatusDTO{}, fmt.Errorf("decode_has_pet_resp: %w", err)
	}
	if out.Err != nil {
		return StatusDTO{}, errcode.FromRemoteReason(out.Err.Code, out.Err.Message, out.Err.Reason, out.Err.RetryAfterMs)
	}
	return StatusDTO{HasPet: out.HasPet, AutoHarvestEnabled: out.AutoHarvestEnabled}, nil
}

func (c *Client) SetAutoHarvest(ctx context.Context, userID int64, enabled bool) error {
	if c.grpc != nil && c.mode != rpcgrpc.ModeHTTP {
		_, err := c.grpc.SetAutoHarvest(ctx, &rpcv1.SetAutoHarvestRequest{UserId: userID, Enabled: enabled})
		if err == nil {
			return nil
		}
		if c.mode != rpcgrpc.ModeGRPCFallback || !rpcgrpc.CanFallback(err) {
			return rpcgrpc.FromError(err)
		}
	}
	body, _ := json.Marshal(SetAutoHarvestReq{UserID: userID, Enabled: enabled})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/rpc/pet/set-auto-harvest", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("http_do set_auto_harvest: %w", err)
	}
	defer resp.Body.Close()
	var out SetAutoHarvestResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return fmt.Errorf("decode_set_auto_harvest_resp: %w", err)
	}
	if out.Err != nil {
		return errcode.FromRemoteReason(out.Err.Code, out.Err.Message, out.Err.Reason, out.Err.RetryAfterMs)
	}
	return nil
}
