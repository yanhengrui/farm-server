package ws

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/gorilla/websocket"

	wscontract "github.com/photon/farm-server/server/contracts/ws"
	"github.com/photon/farm-server/server/internal/farm/application"
	"github.com/photon/farm-server/server/internal/farm/domain"
	"github.com/photon/farm-server/server/internal/farm/infrastructure"
	"github.com/photon/farm-server/server/internal/farm/realtime"
	"github.com/photon/farm-server/server/internal/farm/transport/farmsvc"
	"github.com/photon/farm-server/server/pkg/clock"
	"github.com/photon/farm-server/server/pkg/logging"
	"github.com/photon/farm-server/server/pkg/observability"
	"github.com/photon/farm-server/server/pkg/redisstore"
	"github.com/photon/farm-server/server/pkg/session"
	"github.com/redis/go-redis/v9"
)

const testUserID = int64(1001)

var testSecret = []byte("test-secret-for-ws-tests-32b!")

// commitSubmitter wraps MemCommitter to satisfy farmsvc.CommandSubmitter.
type commitSubmitter struct {
	inner *infrastructure.MemCommitter
}

func (s *commitSubmitter) Submit(ctx context.Context, cmd domain.Command) (application.CommitResult, error) {
	return s.inner.CommitFarmCommand(ctx, application.CommitRequest{Command: cmd})
}

// newTestStack starts a farmsvc backend + gatesvr WS handler.
func newTestStack(t *testing.T) (wsBaseURL string, makeToken func(userID int64) string) {
	t.Helper()

	sub := &commitSubmitter{inner: infrastructure.NewMemCommitter(clock.System{})}
	svcMux := http.NewServeMux()
	farmsvc.NewServer(sub).RegisterRoutes(svcMux)
	svcTS := httptest.NewServer(svcMux)
	t.Cleanup(svcTS.Close)

	farmClient := farmsvc.NewClient(svcTS.URL)
	log := logging.New("gatesvr", "test", "local", "error")
	h := NewHandler(t.Context(), testSecret, farmClient, log)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	wsBaseURL = "ws" + ts.URL[len("http"):]
	makeToken = func(userID int64) string {
		return session.Sign(userID, time.Minute, testSecret)
	}
	return wsBaseURL, makeToken
}

func dialWS(t *testing.T, wsURL string) *websocket.Conn {
	t.Helper()
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("websocket.Dial failed: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func TestUpgradeRejectsLoggedOutV2Session(t *testing.T) {
	redisServer := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	log := logging.New("gatesvr", "test", "local", "error")
	h := NewHandler(t.Context(), testSecret, nil, log).
		WithRedis(HandlerDeps{AuthSessions: redisstore.NewRefreshStore(rdb)})
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	token := session.SignSession(testUserID, "logged-out-session", time.Minute, testSecret)
	_, resp, err := websocket.DefaultDialer.Dial("ws"+ts.URL[len("http"):]+"/ws?token="+token, nil)
	if err == nil {
		t.Fatal("logged-out session upgraded websocket")
	}
	if resp == nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("upgrade response=%v err=%v", resp, err)
	}
}

func sendCommand(t *testing.T, conn *websocket.Conn, farmID int64, clientSeq int64, cmdID, method string, body any) {
	sendCommandAtVersion(t, conn, farmID, clientSeq, cmdID, method, 0, body)
}

func testUUIDv7(label string) string {
	sum := sha256.Sum256([]byte(label))
	sum[6] = (sum[6] & 0x0f) | 0x70
	sum[8] = (sum[8] & 0x3f) | 0x80
	return hex.EncodeToString(sum[:16])
}

func sendCommandAtVersion(t *testing.T, conn *websocket.Conn, farmID int64, clientSeq int64, cmdID, method string, baseVersion int64, body any) {
	t.Helper()
	raw, _ := json.Marshal(body)
	frame := wscontract.Frame{
		Meta: wscontract.Meta{
			Type:        wscontract.FrameTypeCommand,
			Method:      method,
			ClientSeq:   clientSeq,
			CmdID:       testUUIDv7(cmdID),
			FarmID:      strconv.FormatInt(farmID, 10),
			BaseVersion: strconv.FormatInt(baseVersion, 10),
		},
		Body: json.RawMessage(raw),
	}
	data, _ := json.Marshal(frame)
	if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
		t.Fatalf("send command failed: %v", err)
	}
}

func sendSubscribeFarm(t *testing.T, conn *websocket.Conn, farmID, snapshotVersion int64) {
	t.Helper()
	frame := wscontract.Frame{
		Meta: wscontract.Meta{
			Type:   wscontract.FrameTypeSubscribeFarm,
			FarmID: strconv.FormatInt(farmID, 10),
		},
		Body: wscontract.SubscribeFarmBody{SnapshotVersion: strconv.FormatInt(snapshotVersion, 10)},
	}
	data, _ := json.Marshal(frame)
	if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
		t.Fatalf("subscribe farm failed: %v", err)
	}
}

func readFrameOfType(t *testing.T, conn *websocket.Conn, want wscontract.FrameType) wscontract.Frame {
	t.Helper()
	for {
		if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
			t.Fatalf("set read deadline: %v", err)
		}
		_, data, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read message (waiting for %s): %v", want, err)
		}
		var frame wscontract.Frame
		if err := json.Unmarshal(data, &frame); err != nil {
			t.Fatalf("decode frame: %v", err)
		}
		if frame.Meta.Type == want {
			return frame
		}
		// skip frames of other types (e.g., EVENT received during ACK wait)
	}
}

func readACK(t *testing.T, conn *websocket.Conn) (wscontract.Frame, wscontract.ACKBody) {
	t.Helper()
	frame := readFrameOfType(t, conn, wscontract.FrameTypeACK)
	bodyBytes, _ := json.Marshal(frame.Body)
	var ackBody wscontract.ACKBody
	_ = json.Unmarshal(bodyBytes, &ackBody)
	return frame, ackBody
}

func readEvent(t *testing.T, conn *websocket.Conn) (wscontract.Frame, wscontract.FarmEventBody) {
	t.Helper()
	frame := readFrameOfType(t, conn, wscontract.FrameTypeEvent)
	bodyBytes, _ := json.Marshal(frame.Body)
	var evtBody wscontract.FarmEventBody
	_ = json.Unmarshal(bodyBytes, &evtBody)
	return frame, evtBody
}

func TestParseCommandRequiresUUIDv7AndBaseVersion(t *testing.T) {
	h := &Handler{}
	c := &connState{userID: 42}
	body := json.RawMessage(`{"plot_id":1}`)
	validID := testUUIDv7("valid")
	for name, meta := range map[string]wscontract.Meta{
		"missing_cmd_id":       {Method: "farm.Water", BaseVersion: "0"},
		"invalid_cmd_id":       {Method: "farm.Water", CmdID: "cmd-1", BaseVersion: "0"},
		"missing_base_version": {Method: "farm.Water", CmdID: validID},
		"invalid_base_version": {Method: "farm.Water", CmdID: validID, BaseVersion: "-1"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := h.parseCommand(c, meta, body); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
	cmd, err := h.parseCommand(c, wscontract.Meta{Method: "farm.Water", CmdID: validID, BaseVersion: "0"}, body)
	if err != nil || cmd.CmdID != validID || cmd.BaseVersion != 0 {
		t.Fatalf("cmd=%+v err=%v", cmd, err)
	}
}

func TestFirstPlotPatchIncludesRemainingYieldForACKAndEvent(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 10, 0, 0, time.UTC)
	plot := domain.Plot{
		PlotID: 2, CropID: "WHEAT", Status: domain.PlotGrowing,
		PlantedAt: now.Add(-10 * time.Minute), MatureAt: now.Add(-time.Minute), RemainingYield: 4,
	}
	patch := firstPlotPatch(domain.Patch{Plots: []domain.Plot{plot}}, now)
	if patch.State != string(domain.PlotGrowing) || patch.GrowthStage != string(domain.CropGrowthStageMature) || patch.RemainingYield != 4 {
		t.Fatalf("patch=%+v", patch)
	}
	raw, err := json.Marshal(patch)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"remaining_yield":4`) {
		t.Fatalf("json=%s", raw)
	}
}

// TestWS_Connect_ValidToken 验证有效 token 可以建立连接。
func TestWS_Connect_ValidToken(t *testing.T) {
	base, makeToken := newTestStack(t)
	_ = dialWS(t, base+"/ws?token="+makeToken(testUserID))
}

// TestWS_Connect_NoToken 验证无 token 时连接被拒绝。
func TestWS_Connect_NoToken(t *testing.T) {
	base, _ := newTestStack(t)
	_, resp, err := websocket.DefaultDialer.Dial(base+"/ws", nil)
	if err == nil {
		t.Fatal("expected rejection without token")
	}
	if resp != nil && resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", resp.StatusCode)
	}
}

// TestWS_Plant_ACK_OK 验证 PLANT 命令收到 ACK OK，版本递增。
func TestWS_Plant_ACK_OK(t *testing.T) {
	base, makeToken := newTestStack(t)
	conn := dialWS(t, base+"/ws?token="+makeToken(testUserID))

	sendCommand(t, conn, testUserID, 1, "cmd-1", "farm.Plant",
		wscontract.PlantBody{PlotID: 2, SeedItemID: "wheat"})
	_, ack := readACK(t, conn)

	if ack.Result != "OK" {
		t.Errorf("expected OK, got %q", ack.Result)
	}
	if ack.NewVersion == "" || ack.NewVersion == "0" {
		t.Errorf("expected non-zero new_version, got %q", ack.NewVersion)
	}
	if ack.Patch.PlantedAt == "" || ack.Patch.MatureAt == "" || ack.Patch.GrowthStage == "" {
		t.Errorf("ACK patch missing growth fields: %+v", ack.Patch)
	}
}

// TestWS_Plant_Idempotent 验证同一 cmd_id 重发收到 Replayed=true 的 ACK。
func TestWS_Plant_Idempotent(t *testing.T) {
	base, makeToken := newTestStack(t)
	conn := dialWS(t, base+"/ws?token="+makeToken(testUserID))

	sendCommand(t, conn, testUserID, 1, "idem-1", "farm.Plant",
		wscontract.PlantBody{PlotID: 5, SeedItemID: "corn"})
	if _, ack := readACK(t, conn); ack.Result != "OK" {
		t.Fatalf("first ACK: %q", ack.Result)
	}

	// Replay same cmd_id, skip client_seq check.
	sendCommand(t, conn, testUserID, 0, "idem-1", "farm.Plant",
		wscontract.PlantBody{PlotID: 5, SeedItemID: "corn"})
	if _, ack := readACK(t, conn); !ack.Replayed {
		t.Error("replay ACK should have replayed=true")
	}
}

// TestWS_ClientSeq_Stale 验证旧 client_seq 被拒绝。
func TestWS_ClientSeq_Stale(t *testing.T) {
	base, makeToken := newTestStack(t)
	conn := dialWS(t, base+"/ws?token="+makeToken(testUserID))

	sendCommand(t, conn, testUserID, 5, "cmd-a", "farm.Plant",
		wscontract.PlantBody{PlotID: 0, SeedItemID: "wheat"})
	if _, ack := readACK(t, conn); ack.Result != "OK" {
		t.Fatalf("first cmd should succeed")
	}

	sendCommand(t, conn, testUserID, 3, "cmd-b", "farm.Plant",
		wscontract.PlantBody{PlotID: 1, SeedItemID: "wheat"})
	if _, ack := readACK(t, conn); ack.Result == "OK" {
		t.Error("expected error ACK for stale client_seq")
	}
}

// TestWS_EventBroadcast 验证访客只需在安装 Snapshot 后显式订阅，无需先操作
// 好友农场，就能收到后续完整 EVENT。
func TestWS_EventBroadcast(t *testing.T) {
	base, makeToken := newTestStack(t)

	// 操作者 A（user=1001）连接并执行一次播种，先建立 hub 订阅。
	connA := dialWS(t, base+"/ws?token="+makeToken(1001))
	sendCommand(t, connA, 1001, 1, "setup", "farm.Plant",
		wscontract.PlantBody{PlotID: 0, SeedItemID: "wheat"})
	if _, ack := readACK(t, connA); ack.Result != "OK" {
		t.Fatalf("setup plant failed: %s", ack.Result)
	}

	// 观察者 B 安装 A 的 version=1 Snapshot 后显式切换观看订阅，不执行任何好友命令。
	connB := dialWS(t, base+"/ws?token="+makeToken(2002))
	sendSubscribeFarm(t, connB, 1001, 1)
	if _, ack := readACK(t, connB); ack.Result != "OK" || ack.NewVersion != "1" {
		t.Fatalf("subscribe ACK: result=%s version=%s", ack.Result, ack.NewVersion)
	}

	// 现在 A 执行一个新命令，B 应收到 EVENT。
	sendCommandAtVersion(t, connA, 1001, 2, "broadcast-cmd", "farm.Plant", 1,
		wscontract.PlantBody{PlotID: 3, SeedItemID: "WHEAT"})
	if _, ack := readACK(t, connA); ack.Result != "OK" {
		t.Fatalf("A plant plot 3 failed: %s", ack.Result)
	}

	// B 应收到 EVENT 帧。
	frame, evt := readEvent(t, connB)
	if frame.Meta.Type != wscontract.FrameTypeEvent {
		t.Errorf("expected EVENT frame, got %s", frame.Meta.Type)
	}
	if evt.ActorUserID != "1001" {
		t.Errorf("expected actor_user_id=1001, got %q", evt.ActorUserID)
	}
	if evt.Patch.PlantedAt == "" || evt.Patch.MatureAt == "" || evt.Patch.GrowthStage == "" {
		t.Errorf("EVENT patch missing growth fields: %+v", evt.Patch)
	}
	if evt.Patch.RemainingYield != 5 {
		t.Errorf("EVENT patch remaining_yield=%d", evt.Patch.RemainingYield)
	}
}

// TestWS_CommandDoesNotSwitchWatchedFarm 防止重新引入“成功执行命令才切订阅”的耦合。
func TestWS_CommandDoesNotSwitchWatchedFarm(t *testing.T) {
	base, makeToken := newTestStack(t)
	owner := dialWS(t, base+"/ws?token="+makeToken(1001))
	viewer := dialWS(t, base+"/ws?token="+makeToken(2002))

	sendSubscribeFarm(t, viewer, 1001, 0)
	if _, ack := readACK(t, viewer); ack.Result != "OK" {
		t.Fatalf("subscribe failed: %s", ack.Result)
	}

	// viewer 操作自己的农场，不应离开当前观看的 1001 农场。
	sendCommand(t, viewer, 2002, 1, "viewer-own-plant", "farm.Plant",
		wscontract.PlantBody{PlotID: 1, SeedItemID: "corn"})
	if _, ack := readACK(t, viewer); ack.Result != "OK" {
		t.Fatalf("viewer own command failed: %s", ack.Result)
	}

	sendCommand(t, owner, 1001, 1, "owner-plant", "farm.Plant",
		wscontract.PlantBody{PlotID: 2, SeedItemID: "wheat"})
	if _, ack := readACK(t, owner); ack.Result != "OK" {
		t.Fatalf("owner command failed: %s", ack.Result)
	}
	if _, event := readEvent(t, viewer); event.ActorUserID != "1001" {
		t.Fatalf("viewer left watched farm after unrelated command: actor=%s", event.ActorUserID)
	}
}

func TestBroadcastRemoteEventIncludesCompleteGrowthPatch(t *testing.T) {
	log := logging.New("gatesvr", "test", "local", "error")
	h := NewHandler(t.Context(), testSecret, nil, log)
	viewer := makeTestConn()
	viewer.userID = 2002
	if err := h.hub.Subscribe(1001, 4, viewer); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	plantedAt := time.Now().UTC().Add(-time.Minute)
	h.broadcastRemoteEvent(realtime.Message{
		FarmID:      1001,
		EventID:     "event-5",
		FarmVersion: 5,
		ActorUserID: 1001,
		CommandType: domain.CmdPetAutoHarvest,
		Patch: domain.Patch{Plots: []domain.Plot{{
			PlotID: 3, Status: domain.PlotGrowing, CropID: "wheat",
			PlantedAt: plantedAt, MatureAt: plantedAt.Add(10 * time.Minute), RemainingYield: 4,
		}}},
	})

	select {
	case raw := <-viewer.writeCh:
		var frame rawFrame
		if err := json.Unmarshal(raw, &frame); err != nil {
			t.Fatalf("decode frame: %v", err)
		}
		var event wscontract.FarmEventBody
		if err := json.Unmarshal(frame.Body, &event); err != nil {
			t.Fatalf("decode event: %v", err)
		}
		if event.Patch.PlantedAt == "" || event.Patch.MatureAt == "" || event.Patch.GrowthStage == "" {
			t.Fatalf("remote EVENT patch missing growth fields: %+v", event.Patch)
		}
		if event.Patch.RemainingYield != 4 {
			t.Fatalf("remote EVENT patch remaining_yield=%d", event.Patch.RemainingYield)
		}
		if event.CommandType != string(domain.CmdPetAutoHarvest) {
			t.Fatalf("remote EVENT command_type=%q", event.CommandType)
		}
	case <-time.After(time.Second):
		t.Fatal("remote EVENT not delivered")
	}
}

func TestPushMailboxChangedContainsOnlyBadgeState(t *testing.T) {
	log := logging.New("gatesvr", "test", "local", "error")
	h := NewHandler(t.Context(), testSecret, nil, log)
	connection := makeTestConn()
	connection.userID = 42
	h.conns[connection] = struct{}{}

	h.PushMailboxChanged(42, 3, 8)
	select {
	case raw := <-connection.writeCh:
		var frame struct {
			Meta wscontract.Meta               `json:"meta"`
			Body wscontract.MailboxChangedBody `json:"body"`
		}
		if err := json.Unmarshal(raw, &frame); err != nil {
			t.Fatal(err)
		}
		if frame.Meta.Service != "mail" || frame.Meta.Method != "MailboxChanged" || frame.Body.UnreadCount != 3 || frame.Body.Version != 8 {
			t.Fatalf("frame=%+v", frame)
		}
		if strings.Contains(string(raw), "title") || strings.Contains(string(raw), "content") {
			t.Fatalf("badge event leaked mail details: %s", raw)
		}
	case <-time.After(time.Second):
		t.Fatal("mailbox badge event not delivered")
	}
}

func TestPushMailboxChangedDropsOutOfOrderAndFullQueue(t *testing.T) {
	log := logging.New("gatesvr", "test", "local", "error")
	metrics := observability.New("gatesvr", log)
	h := NewHandler(t.Context(), testSecret, nil, log).WithMetrics(metrics)
	connection := makeTestConn()
	connection.userID = 42
	h.conns[connection] = struct{}{}

	h.PushMailboxChanged(42, 3, 8)
	<-connection.writeCh
	h.PushMailboxChanged(42, 9, 7)
	select {
	case raw := <-connection.writeCh:
		t.Fatalf("out-of-order mailbox event delivered: %s", raw)
	default:
	}

	for len(connection.writeCh) < cap(connection.writeCh) {
		connection.writeCh <- []byte("full")
	}
	h.PushMailboxChanged(42, 4, 9)

	rr := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rr.Body.String()
	if !strings.Contains(body, "farm_mailbox_out_of_order_dropped_total 1") {
		t.Fatalf("out-of-order metric missing: %s", body)
	}
	if !strings.Contains(body, "farm_mailbox_ws_queue_dropped_total 1") {
		t.Fatalf("queue-drop metric missing: %s", body)
	}
}

func newRoute95RealtimeGateway(t *testing.T, redisAddr string) string {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	t.Cleanup(func() { _ = rdb.Close() })
	log := logging.New("gatesvr", "test", "local", "error")
	handler := NewHandler(t.Context(), testSecret, nil, log)
	if err := handler.WithFarmEvents(realtime.NewSubscriber(rdb, 10*time.Millisecond)); err != nil {
		t.Fatalf("start farm events: %v", err)
	}
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return "ws" + server.URL[len("http"):]
}

// TestRoute95MultiGatewayGapAndSnapshotRecovery covers two independent
// gatesvr Hub/Subscriber instances. Both receive a cross-gateway patch; a
// missing versions still deliver the newest authoritative plot patch. Clients
// can update that plot immediately and refresh the rest of the snapshot in the
// background without a visible stale-state pause.
func TestRoute95MultiGatewayGapAndSnapshotRecovery(t *testing.T) {
	redisServer := miniredis.RunT(t)
	gateA := newRoute95RealtimeGateway(t, redisServer.Addr())
	gateB := newRoute95RealtimeGateway(t, redisServer.Addr())
	viewerA := dialWS(t, gateA+"/ws?token="+session.Sign(2001, time.Minute, testSecret))
	viewerB := dialWS(t, gateB+"/ws?token="+session.Sign(2002, time.Minute, testSecret))
	for _, viewer := range []*websocket.Conn{viewerA, viewerB} {
		sendSubscribeFarm(t, viewer, 1001, 0)
		if _, ack := readACK(t, viewer); ack.Result != "OK" {
			t.Fatalf("subscribe failed: %s", ack.Result)
		}
	}

	publisherClient := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	t.Cleanup(func() { _ = publisherClient.Close() })
	publisher := realtime.NewPublisher(publisherClient)
	plantedAt := time.Now().UTC()
	event := realtime.Message{
		FarmID: 1001, EventID: "route95-v1", FarmVersion: 1, ActorUserID: 1001,
		Patch: domain.Patch{Plots: []domain.Plot{{PlotID: 1, Status: domain.PlotGrowing, CropID: "wheat", PlantedAt: plantedAt, MatureAt: plantedAt.Add(time.Minute)}}},
	}
	time.Sleep(20 * time.Millisecond)
	if err := publisher.Publish(t.Context(), event); err != nil {
		t.Fatal(err)
	}
	for _, viewer := range []*websocket.Conn{viewerA, viewerB} {
		if _, got := readEvent(t, viewer); got.Version != "1" {
			t.Fatalf("event version=%s", got.Version)
		}
	}

	event.EventID, event.FarmVersion = "route95-v3", 3
	if err := publisher.Publish(t.Context(), event); err != nil {
		t.Fatal(err)
	}
	for _, viewer := range []*websocket.Conn{viewerA, viewerB} {
		if _, got := readEvent(t, viewer); got.Version != "3" {
			t.Fatalf("gap event version=%s", got.Version)
		}
	}

	event.EventID, event.FarmVersion = "route95-v4", 4
	time.Sleep(20 * time.Millisecond)
	if err := publisher.Publish(t.Context(), event); err != nil {
		t.Fatal(err)
	}
	for _, viewer := range []*websocket.Conn{viewerA, viewerB} {
		if _, got := readEvent(t, viewer); got.Version != "4" {
			t.Fatalf("event after gap version=%s", got.Version)
		}
	}
}
