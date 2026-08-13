// Package domain 定义宠物系统的核心实体与常量。
package domain

import "time"

// PetType 宠物类型。
type PetType string

const (
	// PetTypeChicken 小鸡宠物。
	PetTypeChicken PetType = "CHICKEN"
)

// PetStatus 宠物状态。
type PetStatus string

const (
	PetStatusActive PetStatus = "ACTIVE"
)

const (
	// PetPrice 宠物购买价格（金币）。
	PetPrice = int64(200)
	// PetAutoHarvestInterval 宠物自动收割间隔。
	PetAutoHarvestInterval = 30 * time.Second
)

// Pet 玩家宠物实体。
type Pet struct {
	UserID             int64
	PetType            PetType
	Status             PetStatus
	AutoHarvestEnabled bool
	PurchasedAt        time.Time
}

type PlayerStatus struct {
	HasPet             bool
	AutoHarvestEnabled bool
}
