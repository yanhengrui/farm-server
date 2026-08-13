package infrastructure

import (
	"testing"

	"github.com/photon/farm-server/server/internal/farm/domain"
)

func TestRequiresCommandReceiptPolicy(t *testing.T) {
	for _, commandType := range []domain.CommandType{domain.CmdPlant, domain.CmdHarvest, domain.CmdStealCrop, domain.CmdPetAutoHarvest} {
		if !requiresCommandReceipt(commandType) {
			t.Fatalf("%s must retain a fallback receipt", commandType)
		}
	}
	for _, commandType := range []domain.CommandType{domain.CmdWater, domain.CmdHelpWater, domain.CmdPurchaseSeed, domain.CmdSellCrop} {
		if requiresCommandReceipt(commandType) {
			t.Fatalf("%s has version or natural business-key idempotency", commandType)
		}
	}
}
