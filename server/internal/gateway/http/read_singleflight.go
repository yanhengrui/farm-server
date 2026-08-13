// Package http — 读请求 singleflight 合并装饰器。
//
// # 解决的问题
//
// 玩家断线重连、页面刷新或短窗口内多标签页同时发起 GetSnapshot / GetPlayerAssets，
// 会产生对同一 farmID / userID 的并发读请求。每次请求都穿透到 gamesvr，
// 造成不必要的 gRPC 往返和 MySQL 读压力。
//
// # 实现
//
// 使用 golang.org/x/sync/singleflight：窗口内第一个请求真正发出，后续相同 key
// 的请求挂起等待，第一个完成后所有人共享同一结果。
// 读请求不改变状态，共享结果安全；若失败，所有等待者收到同一 error（可立即重试）。
package http

import (
	"context"
	"fmt"

	"golang.org/x/sync/singleflight"

	"github.com/photon/farm-server/server/internal/economy/transport/assetrpc"
	"github.com/photon/farm-server/server/internal/farm/transport/farmrpc"
)

// SingleflightSnapshotClient 为 GetSnapshot 调用加 singleflight 合并。
type SingleflightSnapshotClient struct {
	inner FarmSnapshotClient
	group singleflight.Group
}

// NewSingleflightSnapshotClient 包装已有 client，同 farmID 并发读只发一次 RPC。
func NewSingleflightSnapshotClient(inner FarmSnapshotClient) *SingleflightSnapshotClient {
	return &SingleflightSnapshotClient{inner: inner}
}

func (s *SingleflightSnapshotClient) GetSnapshot(ctx context.Context, farmID int64) (*farmrpc.FarmSnapshotDTO, error) {
	key := fmt.Sprintf("snap:%d", farmID)
	v, err, _ := s.group.Do(key, func() (any, error) {
		// context 使用调用者的，但 singleflight 不传播取消到其他等待者。
		// 若此处 context 被取消，后续等待者会拿到 error，可自行重试。
		return s.inner.GetSnapshot(ctx, farmID)
	})
	if err != nil {
		return nil, err
	}
	return v.(*farmrpc.FarmSnapshotDTO), nil
}

// SingleflightAssetClient 为 GetPlayerAssets 调用加 singleflight 合并。
type SingleflightAssetClient struct {
	inner PlayerAssetClient
	group singleflight.Group
}

// NewSingleflightAssetClient 包装已有 client，同 userID 并发读只发一次 RPC。
func NewSingleflightAssetClient(inner PlayerAssetClient) *SingleflightAssetClient {
	return &SingleflightAssetClient{inner: inner}
}

func (s *SingleflightAssetClient) GetPlayerAssets(ctx context.Context, userID int64) (*assetrpc.AssetsDTO, error) {
	key := fmt.Sprintf("assets:%d", userID)
	v, err, _ := s.group.Do(key, func() (any, error) {
		return s.inner.GetPlayerAssets(ctx, userID)
	})
	if err != nil {
		return nil, err
	}
	return v.(*assetrpc.AssetsDTO), nil
}
