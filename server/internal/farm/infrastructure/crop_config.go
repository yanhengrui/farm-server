// Package infrastructure 提供 P0 作物配置（硬编码）。
// 路线 8 时改为从数据库读取。
package infrastructure

import (
	"fmt"
	"math"
	"time"

	"github.com/photon/farm-server/server/internal/farm/domain"
	"github.com/photon/farm-server/server/pkg/errcode"
)

// CropConfig 作物配置：购买价格、成熟时长、收获产量、出售单价。
type CropConfig struct {
	SeedPrice      int64         // 购买一颗种子消耗金币
	GrowthDuration time.Duration // 从播种到成熟的时长
	HarvestYield   int64         // 收获一次获得的作物数量
	SellPrice      int64         // 出售一个作物获得金币
	ItemID         int64         // stable inventory item ID for seed and harvested crop
	CatalogKey     string        // canonical catalog key unlocked on harvest
}

// cropConfigs 按 crop_id 索引，支持小麦、胡萝卜和番茄的数字 ID 与别名。
var cropConfigs = map[string]CropConfig{
	"1":      {SeedPrice: 10, GrowthDuration: 3 * time.Minute, HarvestYield: 5, SellPrice: 20, ItemID: 1, CatalogKey: "crop_WHEAT"},
	"WHEAT":  {SeedPrice: 10, GrowthDuration: 3 * time.Minute, HarvestYield: 5, SellPrice: 20, ItemID: 1, CatalogKey: "crop_WHEAT"},
	"2":      {SeedPrice: 20, GrowthDuration: 6 * time.Minute, HarvestYield: 6, SellPrice: 25, ItemID: 2, CatalogKey: "crop_CARROT"},
	"CARROT": {SeedPrice: 20, GrowthDuration: 6 * time.Minute, HarvestYield: 6, SellPrice: 25, ItemID: 2, CatalogKey: "crop_CARROT"},
	"3":      {SeedPrice: 30, GrowthDuration: 9 * time.Minute, HarvestYield: 8, SellPrice: 30, ItemID: 3, CatalogKey: "crop_TOMATO"},
	"TOMATO": {SeedPrice: 30, GrowthDuration: 9 * time.Minute, HarvestYield: 8, SellPrice: 30, ItemID: 3, CatalogKey: "crop_TOMATO"},
}

// defaultGrowthDuration 是未知作物的兜底成熟时长。
const defaultGrowthDuration = 10 * time.Minute

// getCropConfig 返回作物配置；crop_id 未知时返回 FarmCropNotFound 错误。
// 同时校验单价的乘法安全性：unitPrice * MaxEconomyQuantity 不得溢出 int64。
// 当配置从数据库读取后，此校验防止异常价格配置绕过上层数量上限保护。
func getCropConfig(cropID string) (CropConfig, error) {
	cfg, ok := cropConfigs[cropID]
	if !ok {
		return CropConfig{}, errcode.New(errcode.FarmCropNotFound,
			fmt.Sprintf("未知作物 crop_id=%q", cropID))
	}
	maxQty := domain.MaxEconomyQuantity
	if cfg.SeedPrice > 0 && cfg.SeedPrice > math.MaxInt64/maxQty {
		return CropConfig{}, errcode.New(errcode.Internal,
			fmt.Sprintf("作物 %q 种子价格 %d 超过安全乘法上限", cropID, cfg.SeedPrice))
	}
	if cfg.SellPrice > 0 && cfg.SellPrice > math.MaxInt64/maxQty {
		return CropConfig{}, errcode.New(errcode.Internal,
			fmt.Sprintf("作物 %q 出售价格 %d 超过安全乘法上限", cropID, cfg.SellPrice))
	}
	return cfg, nil
}

// harvestYieldForCrop returns the configured total yield. The fallback keeps
// legacy/unknown snapshots harvestable while configured crops remain exact.
func harvestYieldForCrop(cropID string) int64 {
	if cfg, err := getCropConfig(cropID); err == nil && cfg.HarvestYield > 0 {
		return cfg.HarvestYield
	}
	return 1
}

// cropIDToItemID 将字符串 crop_id 转为 inventory_items.item_id（BIGINT）。
// "WHEAT" 映射到 1；纯数字字符串按值转换；其他返回 1 作为兜底。
func cropIDToItemID(cropID string) int64 {
	if cfg, err := getCropConfig(cropID); err == nil && cfg.ItemID > 0 {
		return cfg.ItemID
	}
	return 1
}

func catalogKeyForCrop(cropID string) (string, bool) {
	cfg, err := getCropConfig(cropID)
	if err != nil || cfg.CatalogKey == "" {
		return "", false
	}
	return cfg.CatalogKey, true
}
