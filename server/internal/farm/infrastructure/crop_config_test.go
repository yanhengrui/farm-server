package infrastructure

import "testing"

func TestConfiguredCropsHaveDistinctInventoryAndCatalogEntries(t *testing.T) {
	cases := []struct {
		cropID     string
		itemID     int64
		catalogKey string
	}{
		{cropID: "WHEAT", itemID: 1, catalogKey: "crop_WHEAT"},
		{cropID: "CARROT", itemID: 2, catalogKey: "crop_CARROT"},
		{cropID: "TOMATO", itemID: 3, catalogKey: "crop_TOMATO"},
	}

	seenItems := make(map[int64]string, len(cases))
	seenKeys := make(map[string]string, len(cases))
	for _, tc := range cases {
		cfg, err := getCropConfig(tc.cropID)
		if err != nil {
			t.Fatalf("getCropConfig(%s): %v", tc.cropID, err)
		}
		if cfg.ItemID != tc.itemID || cropIDToItemID(tc.cropID) != tc.itemID {
			t.Fatalf("%s item ID=%d, want %d", tc.cropID, cfg.ItemID, tc.itemID)
		}
		if prior, exists := seenItems[cfg.ItemID]; exists {
			t.Fatalf("%s and %s share item ID %d", prior, tc.cropID, cfg.ItemID)
		}
		seenItems[cfg.ItemID] = tc.cropID

		key, ok := catalogKeyForCrop(tc.cropID)
		if !ok || key != tc.catalogKey {
			t.Fatalf("%s catalog key=%q ok=%v, want %q", tc.cropID, key, ok, tc.catalogKey)
		}
		if prior, exists := seenKeys[key]; exists {
			t.Fatalf("%s and %s share catalog key %s", prior, tc.cropID, key)
		}
		seenKeys[key] = tc.cropID
	}
}

func TestCropAliasesUseCanonicalInventoryAndCatalogEntries(t *testing.T) {
	for _, tc := range []struct{ alias, canonical string }{{"1", "WHEAT"}, {"2", "CARROT"}, {"3", "TOMATO"}} {
		if cropIDToItemID(tc.alias) != cropIDToItemID(tc.canonical) {
			t.Fatalf("alias %s does not use %s inventory item", tc.alias, tc.canonical)
		}
		aliasKey, _ := catalogKeyForCrop(tc.alias)
		canonicalKey, _ := catalogKeyForCrop(tc.canonical)
		if aliasKey != canonicalKey {
			t.Fatalf("alias %s catalog key=%s, canonical=%s", tc.alias, aliasKey, canonicalKey)
		}
	}
}
