package rpccontract

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	rpcv1 "github.com/photon/farm-server/server/gen/farm/rpc/v1"
	accountdomain "github.com/photon/farm-server/server/internal/account/domain"
	accountinfra "github.com/photon/farm-server/server/internal/account/infrastructure"
	"github.com/photon/farm-server/server/internal/account/transport/accountrpc"
	catalogdomain "github.com/photon/farm-server/server/internal/catalog/domain"
	"github.com/photon/farm-server/server/internal/catalog/transport/catalogrpc"
	economydomain "github.com/photon/farm-server/server/internal/economy/domain"
	"github.com/photon/farm-server/server/internal/economy/transport/assetrpc"
	"github.com/photon/farm-server/server/internal/farm/application"
	"github.com/photon/farm-server/server/internal/farm/domain"
	"github.com/photon/farm-server/server/internal/farm/transport/farmrpc"
	"github.com/photon/farm-server/server/internal/farm/transport/farmsvc"
	maildomain "github.com/photon/farm-server/server/internal/mail/domain"
	"github.com/photon/farm-server/server/internal/mail/transport/mailrpc"
	petdomain "github.com/photon/farm-server/server/internal/pet/domain"
	"github.com/photon/farm-server/server/internal/pet/transport/petrpc"
	socialdomain "github.com/photon/farm-server/server/internal/social/domain"
	"github.com/photon/farm-server/server/internal/social/transport/socialrpc"
	taskdomain "github.com/photon/farm-server/server/internal/task/domain"
	"github.com/photon/farm-server/server/internal/task/transport/taskrpc"
	"github.com/photon/farm-server/server/pkg/errcode"
	"github.com/photon/farm-server/server/pkg/rpcgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/test/bufconn"
)

func TestHTTPCapacityReasonContract(t *testing.T) {
	const reason = errcode.CapacityReasonGamesvrGlobal
	wires := []any{
		accountrpc.RPCErr{Reason: reason},
		catalogrpc.RPCErr{Reason: reason},
		assetrpc.RPCErr{Reason: reason},
		farmrpc.RPCErr{Reason: reason},
		farmsvc.RPCErr{Reason: reason},
		mailrpc.RPCErr{Reason: reason},
		petrpc.RPCErr{Reason: reason},
		socialrpc.RPCErr{Reason: reason},
		taskrpc.RPCErr{Reason: reason},
	}
	for _, wire := range wires {
		raw, err := json.Marshal(wire)
		if err != nil {
			t.Fatal(err)
		}
		var decoded map[string]any
		if err := json.Unmarshal(raw, &decoded); err != nil || decoded["reason"] != reason {
			t.Fatalf("wire=%T json=%s reason=%v err=%v", wire, raw, decoded["reason"], err)
		}
	}

	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{
			"code": string(errcode.ResourceExhausted), "message": "busy", "reason": reason,
		}})
	}))
	defer httpServer.Close()

	calls := []struct {
		name string
		call func() error
	}{
		{name: "account", call: func() error {
			_, err := accountrpc.NewClient(httpServer.URL).Authenticate(t.Context(), "token")
			return err
		}},
		{name: "catalog", call: func() error {
			_, err := catalogrpc.NewClient(httpServer.URL).ListCatalogUnlocks(t.Context(), 1)
			return err
		}},
		{name: "asset", call: func() error { _, err := assetrpc.NewClient(httpServer.URL).GetPlayerAssets(t.Context(), 1); return err }},
		{name: "farm", call: func() error { _, err := farmrpc.NewClient(httpServer.URL).LoadSnapshot(t.Context(), 1); return err }},
		{name: "farm_command", call: func() error {
			_, err := farmsvc.NewClient(httpServer.URL).SubmitCommand(t.Context(), domain.Command{})
			return err
		}},
		{name: "mail", call: func() error { _, err := mailrpc.NewClient(httpServer.URL).ListMails(t.Context(), 1, 1); return err }},
		{name: "pet", call: func() error { _, err := petrpc.NewClient(httpServer.URL).GetStatus(t.Context(), 1); return err }},
		{name: "social", call: func() error { _, err := socialrpc.NewClient(httpServer.URL).ListFriends(t.Context(), 1); return err }},
		{name: "task", call: func() error { _, err := taskrpc.NewClient(httpServer.URL).ListTasks(t.Context(), 1); return err }},
	}
	for _, call := range calls {
		t.Run(call.name, func(t *testing.T) {
			err := call.call()
			var typed *errcode.Error
			if !errors.As(err, &typed) || typed.Code != errcode.ResourceExhausted || typed.Reason != reason {
				t.Fatalf("error=%v typed=%+v", err, typed)
			}
		})
	}
}

type fixture struct {
	now                    time.Time
	guestLoginDisplayNames []string
}

func must[T any](t *testing.T, value T, err error) T {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func match[T any](t *testing.T, name string, httpValue T, httpErr error, grpcValue T, grpcErr error) {
	t.Helper()
	if httpErr != nil || grpcErr != nil {
		t.Fatalf("%s errors: http=%v grpc=%v", name, httpErr, grpcErr)
	}
	if !reflect.DeepEqual(httpValue, grpcValue) {
		t.Fatalf("%s: http=%+v grpc=%+v", name, httpValue, grpcValue)
	}
}

func matchError(t *testing.T, name string, httpErr, grpcErr error) {
	t.Helper()
	if httpErr != nil || grpcErr != nil {
		t.Fatalf("%s errors: http=%v grpc=%v", name, httpErr, grpcErr)
	}
}

func (f *fixture) CommitFarmCommand(context.Context, application.CommitRequest) (application.CommitResult, error) {
	return application.CommitResult{NewVersion: 7, EventID: "event-7", CoinBalance: 123, Patch: domain.Patch{FarmID: 11, Version: 7, ActorUser: 11, Plots: []domain.Plot{{PlotID: 2, Status: domain.PlotGrowing, CropID: "WHEAT", PlantedAt: f.now, MatureAt: f.now.Add(time.Minute), RemainingYield: 4}}}}, nil
}
func (f *fixture) Submit(ctx context.Context, cmd domain.Command) (application.CommitResult, error) {
	return f.CommitFarmCommand(ctx, application.CommitRequest{Command: cmd})
}
func (f *fixture) LoadSnapshot(context.Context, int64) (domain.Snapshot, error) {
	return domain.Snapshot{FarmID: 11, OwnerID: 11, Version: 6, RouteEpoch: 3, Plots: map[int32]domain.Plot{2: {PlotID: 2, Status: domain.PlotGrowing, CropID: "WHEAT", PlantedAt: f.now, MatureAt: f.now.Add(time.Minute), RemainingYield: 4}}}, nil
}
func (f *fixture) AdvanceRouteEpoch(context.Context, int64, int64) error { return nil }
func (f *fixture) GuestLogin(_ context.Context, _ string, displayName string) (accountinfra.GuestLoginResult, error) {
	f.guestLoginDisplayNames = append(f.guestLoginDisplayNames, displayName)
	return accountinfra.GuestLoginResult{
		Account: accountdomain.Account{UserID: 11, FarmID: 11, DisplayName: displayName},
		Session: accountdomain.SessionRecord{SessionID: "session-11", AccessToken: "access-11", RefreshToken: "refresh-11"},
	}, nil
}
func (f *fixture) Register(ctx context.Context, username, password, displayName string) (accountinfra.GuestLoginResult, error) {
	return f.GuestLogin(ctx, username, displayName)
}
func (f *fixture) PasswordLogin(ctx context.Context, username, password string) (accountinfra.GuestLoginResult, error) {
	return f.GuestLogin(ctx, username, "Player")
}
func (f *fixture) Logout(context.Context, string, string) error { return nil }
func (f *fixture) RefreshSession(context.Context, string, string) (accountinfra.RefreshSessionResult, error) {
	return accountinfra.RefreshSessionResult{AccessToken: "new-token"}, nil
}
func (f *fixture) Authenticate(string) (accountinfra.AuthenticateResult, error) {
	return accountinfra.AuthenticateResult{UserID: 11, FarmID: 11}, nil
}
func (f *fixture) CreateInvite(context.Context, int64) (string, error)    { return "invite-11", nil }
func (f *fixture) AcceptInvite(context.Context, string, int64) error      { return nil }
func (f *fixture) AreFriends(context.Context, int64, int64) (bool, error) { return true, nil }
func (f *fixture) ListFriends(context.Context, int64) ([]socialdomain.FriendInfo, error) {
	return []socialdomain.FriendInfo{{UserID: 22, DisplayName: "Henry"}}, nil
}
func (f *fixture) LoadDisplayName(context.Context, int64) (string, error)          { return "小麦糖", nil }
func (f *fixture) SendMail(context.Context, maildomain.SendMailReq) (int64, error) { return 31, nil }
func (f *fixture) ListMails(context.Context, int64, int) ([]maildomain.Mail, error) {
	return []maildomain.Mail{{MailID: 31, MailType: maildomain.MailTypeSystem, Title: "hello", Status: maildomain.MailStatusUnread, CreatedAt: f.now, Attachments: []maildomain.Attachment{{AttachmentID: 41, ItemType: "COIN", Quantity: 5}}}}, nil
}
func (f *fixture) GetSummary(context.Context, int64) (maildomain.MailboxSummary, error) {
	return maildomain.MailboxSummary{UserID: 11, UnreadCount: 2, Version: 5}, nil
}
func (f *fixture) ClaimAttachment(context.Context, int64, int64) error    { return nil }
func (f *fixture) MarkRead(context.Context, int64, int64) error           { return nil }
func (f *fixture) IncrProgress(context.Context, int64, string, int) error { return nil }
func (f *fixture) ListTasks(context.Context, int64) ([]taskdomain.TaskProgress, error) {
	return []taskdomain.TaskProgress{{TaskKey: "plant_10", Progress: 3, Status: taskdomain.TaskStatusActive, UpdatedAt: f.now}}, nil
}
func (f *fixture) ClaimReward(context.Context, int64, string) (int64, error) { return 50, nil }
func (f *fixture) BuyPet(context.Context, int64) error                       { return nil }
func (f *fixture) HasPet(context.Context, int64) (bool, error)               { return true, nil }
func (f *fixture) GetStatus(context.Context, int64) (petdomain.PlayerStatus, error) {
	return petdomain.PlayerStatus{HasPet: true, AutoHarvestEnabled: true}, nil
}
func (f *fixture) SetAutoHarvest(context.Context, int64, bool) error { return nil }
func (f *fixture) ListCatalogUnlocks(context.Context, int64) ([]catalogdomain.Unlock, error) {
	return []catalogdomain.Unlock{{CatalogKey: "crop_WHEAT", UnlockedAt: f.now}}, nil
}
func (f *fixture) GetPlayerAssets(context.Context, int64) (economydomain.Assets, error) {
	return economydomain.Assets{CoinBalance: 123, Inventory: []economydomain.InventoryItem{{ItemType: "SEED", ItemID: 1, Quantity: 5}}}, nil
}

func TestHTTPAndGRPCContractsMatch(t *testing.T) {
	f := &fixture{now: time.Date(2026, 7, 31, 8, 0, 0, 0, time.UTC)}
	farmServer := farmrpc.NewServer(f, f, f)
	farmCommandServer := farmsvc.NewServer(f)
	accountServer := accountrpc.NewServer(f)
	socialServer := socialrpc.NewServer(f)
	mailServer := mailrpc.NewServer(f)
	taskServer := taskrpc.NewServer(f)
	petServer := petrpc.NewServer(f)
	catalogServer := catalogrpc.NewServer(f)
	assetServer := assetrpc.NewServer(f)

	mux := http.NewServeMux()
	farmServer.RegisterRoutes(mux)
	farmCommandServer.RegisterRoutes(mux)
	accountServer.RegisterRoutes(mux)
	socialServer.RegisterRoutes(mux)
	mailServer.RegisterRoutes(mux)
	taskServer.RegisterRoutes(mux)
	petServer.RegisterRoutes(mux)
	catalogServer.RegisterRoutes(mux)
	assetServer.RegisterRoutes(mux)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()

	listener := bufconn.Listen(1 << 20)
	grpcServer := grpc.NewServer(grpc.ChainUnaryInterceptor(rpcgrpc.ServerTraceInterceptor()))
	rpcgrpc.RegisterHealth(grpcServer)
	farmServer.RegisterGRPC(grpcServer)
	farmCommandServer.RegisterGRPC(grpcServer)
	accountServer.RegisterGRPC(grpcServer)
	socialServer.RegisterGRPC(grpcServer)
	mailServer.RegisterGRPC(grpcServer)
	taskServer.RegisterGRPC(grpcServer)
	petServer.RegisterGRPC(grpcServer)
	catalogServer.RegisterGRPC(grpcServer)
	assetServer.RegisterGRPC(grpcServer)
	serveErr := make(chan error, 1)
	go func() { serveErr <- grpcServer.Serve(listener) }()
	defer func() {
		grpcServer.Stop()
		if err := <-serveErr; err != nil {
			t.Errorf("serve gRPC fixture: %v", err)
		}
	}()
	conn, err := grpc.NewClient("passthrough:///bufnet", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	t.Run("health", func(t *testing.T) {
		resp, err := healthv1.NewHealthClient(conn).Check(t.Context(), &healthv1.HealthCheckRequest{})
		if err != nil || resp.Status != healthv1.HealthCheckResponse_SERVING {
			t.Fatalf("status=%v err=%v", resp.GetStatus(), err)
		}
	})

	t.Run("account", func(t *testing.T) {
		httpClient := accountrpc.NewClient(httpServer.URL)
		grpcClient := accountrpc.NewClient(httpServer.URL).WithGRPC(rpcv1.NewAccountServiceClient(conn), rpcgrpc.ModeGRPC)
		h, hErr := httpClient.GuestLogin(t.Context(), "device-1", "小麦糖")
		g, gErr := grpcClient.GuestLogin(t.Context(), "device-1", "小麦糖")
		match(t, "guest_login", h, hErr, g, gErr)
		if h.DisplayName == "" || g.DisplayName == "" {
			t.Fatalf("guest login response lost display_name: http=%+v grpc=%+v", h, g)
		}
		if !reflect.DeepEqual(f.guestLoginDisplayNames, []string{"小麦糖", "小麦糖"}) {
			t.Fatalf("guest login display_name differs across transports: %v", f.guestLoginDisplayNames)
		}
		hr, hrErr := httpClient.RefreshSession(t.Context(), "session-1", "refresh-1")
		gr, grErr := grpcClient.RefreshSession(t.Context(), "session-1", "refresh-1")
		match(t, "refresh_session", hr, hrErr, gr, grErr)
		ha, haErr := httpClient.Authenticate(t.Context(), "token")
		ga, gaErr := grpcClient.Authenticate(t.Context(), "token")
		match(t, "authenticate", ha, haErr, ga, gaErr)
	})
	t.Run("farm", func(t *testing.T) {
		httpClient := farmrpc.NewClient(httpServer.URL)
		grpcClient := farmrpc.NewClient(httpServer.URL).WithGRPC(rpcv1.NewFarmServiceClient(conn), rpcgrpc.ModeGRPC)
		cmd := application.CommitRequest{Command: domain.Command{CmdID: "c1", FarmID: 11}}
		h, hErr := httpClient.CommitFarmCommand(t.Context(), cmd)
		g, gErr := grpcClient.CommitFarmCommand(t.Context(), cmd)
		match(t, "commit", h, hErr, g, gErr)
		hs, hsErr := httpClient.LoadSnapshot(t.Context(), 11)
		gs, gsErr := grpcClient.LoadSnapshot(t.Context(), 11)
		match(t, "load_snapshot", hs, hsErr, gs, gsErr)
		hv, hvErr := httpClient.GetSnapshot(t.Context(), 11)
		gv, gvErr := grpcClient.GetSnapshot(t.Context(), 11)
		match(t, "get_snapshot", hv, hvErr, gv, gvErr)
		if hv == nil || len(hv.Plots) != 1 || hv.Plots[0].RemainingYield != 4 {
			t.Fatalf("get_snapshot missing remaining_yield: %+v", hv)
		}
		if hv.OwnerDisplayName != "小麦糖" {
			t.Fatalf("get_snapshot missing owner_display_name: %+v", hv)
		}
		matchError(t, "advance_route_epoch", httpClient.AdvanceRouteEpoch(t.Context(), 11, 4), grpcClient.AdvanceRouteEpoch(t.Context(), 11, 4))
	})
	t.Run("farm_command", func(t *testing.T) {
		cmd := domain.Command{CmdID: "c2", FarmID: 11}
		h, hErr := farmsvc.NewClient(httpServer.URL).SubmitCommand(t.Context(), cmd)
		g, gErr := farmsvc.NewClient(httpServer.URL).WithGRPC(rpcv1.NewFarmCommandServiceClient(conn), rpcgrpc.ModeGRPC).SubmitCommand(t.Context(), cmd)
		h = must(t, h, hErr)
		g = must(t, g, gErr)
		if !reflect.DeepEqual(h, g) {
			t.Fatalf("http=%+v grpc=%+v", h, g)
		}
	})
	t.Run("social", func(t *testing.T) {
		httpClient := socialrpc.NewClient(httpServer.URL)
		grpcClient := socialrpc.NewClient(httpServer.URL).WithGRPC(rpcv1.NewSocialServiceClient(conn), rpcgrpc.ModeGRPC)
		hc, hcErr := httpClient.CreateInvite(t.Context(), 11)
		gc, gcErr := grpcClient.CreateInvite(t.Context(), 11)
		match(t, "create_invite", hc, hcErr, gc, gcErr)
		matchError(t, "accept_invite", httpClient.AcceptInvite(t.Context(), "invite-11", 22), grpcClient.AcceptInvite(t.Context(), "invite-11", 22))
		ha, haErr := httpClient.AreFriends(t.Context(), 11, 22)
		ga, gaErr := grpcClient.AreFriends(t.Context(), 11, 22)
		match(t, "are_friends", ha, haErr, ga, gaErr)
		h, hErr := httpClient.ListFriends(t.Context(), 11)
		g, gErr := grpcClient.ListFriends(t.Context(), 11)
		match(t, "list_friends", h, hErr, g, gErr)
		if len(h) != 1 || h[0].DisplayName != "Henry" {
			t.Fatalf("list_friends missing display_name: %+v", h)
		}
	})
	t.Run("mail", func(t *testing.T) {
		httpClient := mailrpc.NewClient(httpServer.URL)
		grpcClient := mailrpc.NewClient(httpServer.URL).WithGRPC(rpcv1.NewMailServiceClient(conn), rpcgrpc.ModeGRPC)
		req := maildomain.SendMailReq{UserID: 11, MailType: maildomain.MailTypeSystem, Title: "hello", Content: "body", Attachments: []maildomain.Attachment{{ItemType: "COIN", Quantity: 5}}}
		hs, hsErr := httpClient.SendMail(t.Context(), req)
		gs, gsErr := grpcClient.SendMail(t.Context(), req)
		match(t, "send_mail", hs, hsErr, gs, gsErr)
		h, hErr := httpClient.ListMails(t.Context(), 11, 10)
		g, gErr := grpcClient.ListMails(t.Context(), 11, 10)
		match(t, "list_mails", h, hErr, g, gErr)
		hSummary, hSummaryErr := httpClient.GetSummary(t.Context(), 11)
		gSummary, gSummaryErr := grpcClient.GetSummary(t.Context(), 11)
		match(t, "mailbox_summary", hSummary, hSummaryErr, gSummary, gSummaryErr)
		matchError(t, "claim_attachment", httpClient.ClaimAttachment(t.Context(), 41, 11), grpcClient.ClaimAttachment(t.Context(), 41, 11))
		matchError(t, "mark_read", httpClient.MarkRead(t.Context(), 31, 11), grpcClient.MarkRead(t.Context(), 31, 11))
	})
	t.Run("task", func(t *testing.T) {
		httpClient := taskrpc.NewClient(httpServer.URL)
		grpcClient := taskrpc.NewClient(httpServer.URL).WithGRPC(rpcv1.NewTaskServiceClient(conn), rpcgrpc.ModeGRPC)
		matchError(t, "incr_progress", httpClient.IncrProgress(t.Context(), 11, "plant_10", 2), grpcClient.IncrProgress(t.Context(), 11, "plant_10", 2))
		h, hErr := httpClient.ListTasks(t.Context(), 11)
		g, gErr := grpcClient.ListTasks(t.Context(), 11)
		match(t, "list_tasks", h, hErr, g, gErr)
		hc, hcErr := httpClient.ClaimReward(t.Context(), 11, "plant_10")
		gc, gcErr := grpcClient.ClaimReward(t.Context(), 11, "plant_10")
		match(t, "claim_reward", hc, hcErr, gc, gcErr)
	})
	t.Run("pet", func(t *testing.T) {
		httpClient := petrpc.NewClient(httpServer.URL)
		grpcClient := petrpc.NewClient(httpServer.URL).WithGRPC(rpcv1.NewPetServiceClient(conn), rpcgrpc.ModeGRPC)
		matchError(t, "buy_pet", httpClient.BuyPet(t.Context(), 11), grpcClient.BuyPet(t.Context(), 11))
		h, hErr := httpClient.HasPet(t.Context(), 11)
		g, gErr := grpcClient.HasPet(t.Context(), 11)
		match(t, "has_pet", h, hErr, g, gErr)
		hs, hsErr := httpClient.GetStatus(t.Context(), 11)
		gs, gsErr := grpcClient.GetStatus(t.Context(), 11)
		match(t, "pet_status", hs, hsErr, gs, gsErr)
		matchError(t, "set_auto_harvest", httpClient.SetAutoHarvest(t.Context(), 11, false), grpcClient.SetAutoHarvest(t.Context(), 11, false))
	})
	t.Run("catalog", func(t *testing.T) {
		httpClient := catalogrpc.NewClient(httpServer.URL)
		grpcClient := catalogrpc.NewClient(httpServer.URL).WithGRPC(rpcv1.NewCatalogServiceClient(conn), rpcgrpc.ModeGRPC)
		h, hErr := httpClient.ListCatalogUnlocks(t.Context(), 11)
		g, gErr := grpcClient.ListCatalogUnlocks(t.Context(), 11)
		match(t, "list_catalog_unlocks", h, hErr, g, gErr)
	})
	t.Run("assets", func(t *testing.T) {
		httpClient := assetrpc.NewClient(httpServer.URL)
		grpcClient := assetrpc.NewClient(httpServer.URL).WithGRPC(rpcv1.NewAssetServiceClient(conn), rpcgrpc.ModeGRPC)
		h, hErr := httpClient.GetPlayerAssets(t.Context(), 11)
		g, gErr := grpcClient.GetPlayerAssets(t.Context(), 11)
		match(t, "get_player_assets", h, hErr, g, gErr)
	})
	t.Run("invalid_argument_error", func(t *testing.T) {
		hErr := petrpc.NewClient(httpServer.URL).BuyPet(t.Context(), 0)
		gErr := petrpc.NewClient(httpServer.URL).WithGRPC(rpcv1.NewPetServiceClient(conn), rpcgrpc.ModeGRPC).BuyPet(t.Context(), 0)
		var hCode, gCode *errcode.Error
		if !errors.As(hErr, &hCode) || !errors.As(gErr, &gCode) {
			t.Fatalf("expected typed errors: http=%v grpc=%v", hErr, gErr)
		}
		if hCode.Code != gCode.Code || hCode.Message != gCode.Message {
			t.Fatalf("http=%+v grpc=%+v", hCode, gCode)
		}
	})
}
