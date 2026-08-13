package farmrpc

import (
	"net/http"
	"net/http/httptest"
	"testing"

	rpcv1 "github.com/photon/farm-server/server/gen/farm/rpc/v1"
)

func TestGetSnapshotLegacyHTTPDisplayNameFallback(t *testing.T) {
	for _, response := range []string{
		`{"snapshot":{"farm_id":"123","owner_user_id":"123","version":"1","plots":[]}}`,
		`{"snapshot":{"farm_id":"123","owner_user_id":"123","owner_display_name":"   ","version":"1","plots":[]}}`,
	} {
		t.Run(response, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(response))
			}))
			defer server.Close()

			snapshot, err := NewClient(server.URL).GetSnapshot(t.Context(), 123)
			if err != nil {
				t.Fatalf("GetSnapshot: %v", err)
			}
			if snapshot.OwnerDisplayName != "123" {
				t.Fatalf("owner_display_name=%q", snapshot.OwnerDisplayName)
			}
		})
	}
}

func TestSnapshotDTOFromLegacyGRPCDisplayNameFallback(t *testing.T) {
	for _, displayName := range []string{"", " \t "} {
		snapshot := snapshotDTOFromProto(&rpcv1.FarmSnapshotView{FarmId: 123, OwnerUserId: 123, OwnerDisplayName: displayName, Version: 1})
		if snapshot.OwnerDisplayName != "123" {
			t.Fatalf("input=%q owner_display_name=%q", displayName, snapshot.OwnerDisplayName)
		}
	}
}
