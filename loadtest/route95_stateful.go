// Command route95_stateful drives sustained state-changing traffic through the
// public gatesvr HTTP/WebSocket protocol. It never calls an internal commit or
// database API. Test-only fixture creation is a separate, auditable step.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	wscontract "github.com/photon/farm-server/server/contracts/ws"
	"github.com/photon/farm-server/server/pkg/id"
)

type runConfig struct {
	URLs           []string
	Users          int
	TargetRPS      int
	MinAcceptRatio float64
	Duration       time.Duration
	Warmup         time.Duration
	SetupParallel  int
	DevicePrefix   string
	Mix            workloadMix
	HotViewers     []int
	RequestTimeout time.Duration
	ACKLossEvery   int64
	Output         string
	HTTPOnly       bool
	SetupOnly      bool
}

type workloadMix struct {
	Purchase int
	Sell     int
	Plant    int
	Water    int
	Harvest  int
}

func (m workloadMix) total() int { return m.Purchase + m.Sell + m.Plant + m.Water + m.Harvest }

func parseMix(raw string) (workloadMix, error) {
	var mix workloadMix
	for _, item := range strings.Split(raw, ",") {
		parts := strings.SplitN(strings.TrimSpace(item), "=", 2)
		if len(parts) != 2 {
			return mix, fmt.Errorf("invalid workload item %q", item)
		}
		value, err := strconv.Atoi(parts[1])
		if err != nil || value < 0 {
			return mix, fmt.Errorf("invalid workload weight %q", item)
		}
		switch strings.ToLower(parts[0]) {
		case "purchase":
			mix.Purchase = value
		case "sell":
			mix.Sell = value
		case "plant":
			mix.Plant = value
		case "water":
			mix.Water = value
		case "harvest":
			mix.Harvest = value
		default:
			return mix, fmt.Errorf("unknown workload command %q", parts[0])
		}
	}
	if mix.total() == 0 {
		return mix, errors.New("workload mix total must be positive")
	}
	return mix, nil
}

func parsePositiveList(raw string) ([]int, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	var out []int
	for _, part := range strings.Split(raw, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil || n < 1 {
			return nil, fmt.Errorf("invalid positive integer %q", part)
		}
		out = append(out, n)
	}
	return out, nil
}

func parseConfig(args []string) (runConfig, error) {
	fs := flag.NewFlagSet("route95_stateful", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	urls := fs.String("urls", "http://127.0.0.1:28080", "comma-separated gatesvr base URLs")
	users := fs.Int("users", 1000, "independent virtual users/farms")
	targetRPS := fs.Int("target-rps", 1000, "attempted state-changing commands per second")
	minAcceptRatio := fs.Float64("min-accepted-ratio", 1.0, "minimum accepted RPS as a ratio of target-rps")
	duration := fs.Duration("duration", 30*time.Minute, "measured duration")
	warmup := fs.Duration("warmup", 30*time.Second, "warm-up duration")
	setupParallel := fs.Int("setup-parallel", 64, "parallel public guest-login/snapshot setup workers")
	devicePrefix := fs.String("device-prefix", "route95-stateful", "stable device ID prefix")
	mixRaw := fs.String("mix", "purchase=49,sell=49,plant=1,water=1,harvest=0", "command weight mix")
	hotRaw := fs.String("hot-viewers", "1,2,4,8,20", "viewer counts assigned to representative hot farms")
	requestTimeout := fs.Duration("request-timeout", 5*time.Second, "HTTP and ACK timeout")
	ackLossEvery := fs.Int64("ack-loss-every", 0, "discard every Nth WS ACK and retry the same cmd_id; 0 disables")
	output := fs.String("output", "", "optional JSON output path")
	httpOnly := fs.Bool("http-only", false, "do not open WebSockets; only valid for purchase/sell workloads")
	setupOnly := fs.Bool("setup-only", false, "create/reuse users via public login and exit before opening load")
	if err := fs.Parse(args); err != nil {
		return runConfig{}, err
	}
	mix, err := parseMix(*mixRaw)
	if err != nil {
		return runConfig{}, err
	}
	if *httpOnly && (mix.Plant > 0 || mix.Water > 0 || mix.Harvest > 0) {
		return runConfig{}, errors.New("http-only requires a purchase/sell-only workload")
	}
	hot, err := parsePositiveList(*hotRaw)
	if err != nil {
		return runConfig{}, err
	}
	var parsedURLs []string
	for _, raw := range strings.Split(*urls, ",") {
		raw = strings.TrimRight(strings.TrimSpace(raw), "/")
		u, parseErr := url.Parse(raw)
		if parseErr != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return runConfig{}, fmt.Errorf("invalid gatesvr URL %q", raw)
		}
		parsedURLs = append(parsedURLs, raw)
	}
	if len(parsedURLs) == 0 || *users < 1 || *targetRPS < 1 || *minAcceptRatio <= 0 || *minAcceptRatio > 1 || *duration <= 0 || *warmup < 0 || *setupParallel < 1 || *requestTimeout <= 0 || *devicePrefix == "" || *ackLossEvery < 0 {
		return runConfig{}, errors.New("users, target-rps, duration, setup-parallel, timeout and device-prefix must be valid positive values")
	}
	return runConfig{URLs: parsedURLs, Users: *users, TargetRPS: *targetRPS, MinAcceptRatio: *minAcceptRatio, Duration: *duration, Warmup: *warmup, SetupParallel: *setupParallel, DevicePrefix: *devicePrefix, Mix: mix, HotViewers: hot, RequestTimeout: *requestTimeout, ACKLossEvery: *ackLossEvery, Output: *output, HTTPOnly: *httpOnly, SetupOnly: *setupOnly}, nil
}

type loginResponse struct {
	AccessToken string `json:"access_token"`
	UserID      string `json:"user_id"`
	FarmID      string `json:"farm_id"`
}

type snapshotPlot struct {
	PlotID         int32  `json:"plot_id"`
	Status         string `json:"status"`
	CropID         string `json:"crop_id"`
	MatureAt       string `json:"mature_at"`
	RemainingYield int64  `json:"remaining_yield"`
}

type snapshotResponse struct {
	FarmID  string         `json:"farm_id"`
	Version string         `json:"version"`
	Plots   []snapshotPlot `json:"plots"`
}

type apiError struct {
	Code   string `json:"code"`
	Reason string `json:"reason"`
}

type farmPlot struct {
	status         string
	cropID         string
	matureAt       time.Time
	remainingYield int64
	watered        bool
}

type farmState struct {
	mu      sync.Mutex
	farmID  string
	version int64
	plots   map[int32]farmPlot
}

func newFarmState(snapshot snapshotResponse) (*farmState, error) {
	version, err := strconv.ParseInt(snapshot.Version, 10, 64)
	if err != nil {
		return nil, err
	}
	state := &farmState{farmID: snapshot.FarmID, version: version, plots: make(map[int32]farmPlot, len(snapshot.Plots))}
	for _, plot := range snapshot.Plots {
		var matureAt time.Time
		if plot.MatureAt != "" {
			matureAt, _ = time.Parse(time.RFC3339Nano, plot.MatureAt)
		}
		state.plots[plot.PlotID] = farmPlot{status: plot.Status, cropID: plot.CropID, matureAt: matureAt, remainingYield: plot.RemainingYield}
	}
	return state, nil
}

func (f *farmState) snapshotVersion() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.version
}

func (f *farmState) replace(snapshot snapshotResponse) error {
	next, err := newFarmState(snapshot)
	if err != nil {
		return err
	}
	f.mu.Lock()
	f.version, f.plots = next.version, next.plots
	f.mu.Unlock()
	return nil
}

func (f *farmState) advance(version int64, patch wscontract.PlotPatch) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if version < f.version {
		return fmt.Errorf("farm_version_regression current=%d received=%d", f.version, version)
	}
	if version == f.version {
		return nil
	}
	if version != f.version+1 {
		return fmt.Errorf("farm_version_gap current=%d received=%d", f.version, version)
	}
	plot := f.plots[patch.PlotID]
	if patch.State != "" {
		if patch.State == "PLANTED" {
			plot.status = "GROWING"
		} else {
			plot.status = patch.State
		}
	}
	if patch.CropID != "" {
		plot.cropID = patch.CropID
	}
	if patch.MatureAt != "" {
		plot.matureAt, _ = time.Parse(time.RFC3339Nano, patch.MatureAt)
	}
	plot.remainingYield = patch.RemainingYield
	if plot.status == "EMPTY" {
		plot.watered = false
		plot.cropID = ""
		plot.matureAt = time.Time{}
	}
	f.plots[patch.PlotID] = plot
	f.version = version
	return nil
}

func (f *farmState) choosePlant() (int32, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for id, plot := range f.plots {
		if plot.status == "" || plot.status == "EMPTY" {
			return id, true
		}
	}
	return 0, false
}

func (f *farmState) chooseWater() (int32, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for id, plot := range f.plots {
		if plot.status == "GROWING" && !plot.watered {
			plot.watered = true
			f.plots[id] = plot
			return id, true
		}
	}
	return 0, false
}

func (f *farmState) undoWater(plotID int32) {
	f.mu.Lock()
	plot := f.plots[plotID]
	plot.watered = false
	f.plots[plotID] = plot
	f.mu.Unlock()
}

func (f *farmState) chooseHarvest(now time.Time) (int32, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for id, plot := range f.plots {
		if plot.status == "GROWING" && !plot.matureAt.IsZero() && !now.Before(plot.matureAt) && plot.remainingYield > 0 {
			return id, true
		}
	}
	return 0, false
}

type virtualUser struct {
	index           int
	baseURL         string
	token           string
	userID          string
	farm            *farmState
	client          *http.Client
	ws              *websocket.Conn
	clientSeq       int64
	commandSequence uint64
	jobs            chan struct{}
	viewFarm        string
	viewVer         int64
	mu              sync.Mutex
}

type latencyHistogram struct {
	mu      sync.Mutex
	bounds  []int64
	counts  []uint64
	total   uint64
	maximum int64
}

func newLatencyHistogram() *latencyHistogram {
	bounds := make([]int64, 0, 400)
	for value := 10.0; value < 60_000_000; value *= 1.05 {
		bounds = append(bounds, int64(value))
	}
	return &latencyHistogram{bounds: bounds, counts: make([]uint64, len(bounds)+1)}
}

func (h *latencyHistogram) observe(d time.Duration) {
	us := d.Microseconds()
	h.mu.Lock()
	idx := sort.Search(len(h.bounds), func(i int) bool { return h.bounds[i] >= us })
	h.counts[idx]++
	h.total++
	if us > h.maximum {
		h.maximum = us
	}
	h.mu.Unlock()
}

func (h *latencyHistogram) percentile(q float64) float64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.total == 0 {
		return 0
	}
	want := uint64(math.Ceil(float64(h.total) * q))
	var seen uint64
	for i, count := range h.counts {
		seen += count
		if seen >= want {
			if i >= len(h.bounds) {
				return float64(h.maximum) / 1000
			}
			return float64(h.bounds[i]) / 1000
		}
	}
	return float64(h.maximum) / 1000
}

type counters struct {
	attempted atomic.Uint64
	accepted  atomic.Uint64
	ackOK     atomic.Uint64
	rejected  atomic.Uint64
	failed    atomic.Uint64
	retried   atomic.Uint64
	queueFull atomic.Uint64
	skipped   atomic.Uint64
	versions  atomic.Uint64
	inflight  atomic.Int64
	mu        sync.Mutex
	errors    map[string]uint64
	commands  map[string]uint64
	skips     map[string]uint64
	latency   *latencyHistogram
}

func newCounters() *counters {
	return &counters{errors: make(map[string]uint64), commands: make(map[string]uint64), skips: make(map[string]uint64), latency: newLatencyHistogram()}
}

func (c *counters) error(code string, rejected bool) {
	if code == "" {
		code = "UNKNOWN"
	}
	if rejected {
		c.rejected.Add(1)
	} else {
		c.failed.Add(1)
	}
	c.mu.Lock()
	c.errors[code]++
	c.mu.Unlock()
}

func (c *counters) command(name string) {
	c.mu.Lock()
	c.commands[name]++
	c.mu.Unlock()
}

func (c *counters) skip(name, reason string) {
	c.skipped.Add(1)
	c.mu.Lock()
	c.skips[name+":"+reason]++
	c.mu.Unlock()
}

func (c *counters) success(ack bool, latency time.Duration) {
	// A virtual user executes one pending command synchronously. Transport
	// retries stay inside runHTTP/runWS and reach this method only once, after
	// the command's retry loop has completed. Keeping every successful cmd_id
	// globally would therefore add no correctness and would grow for the full
	// duration of a capacity run.
	c.accepted.Add(1)
	if ack {
		c.ackOK.Add(1)
	}
	c.latency.observe(latency)
}

type rawFrame struct {
	Meta wscontract.Meta `json:"meta"`
	Body json.RawMessage `json:"body"`
}

func requestJSON(ctx context.Context, client *http.Client, method, endpoint, token, idem string, body any, out any) (int, string, error) {
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return 0, "", err
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, reader)
	if err != nil {
		return 0, "", err
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if idem != "" {
		req.Header.Set("Idempotency-Key", idem)
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return resp.StatusCode, "", err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var apiErr apiError
		_ = json.Unmarshal(raw, &apiErr)
		if apiErr.Reason == "" {
			apiErr.Reason = resp.Header.Get("X-Capacity-Reason")
		}
		if apiErr.Reason != "" {
			apiErr.Code += "@" + apiErr.Reason
		}
		return resp.StatusCode, apiErr.Code, fmt.Errorf("HTTP_%d", resp.StatusCode)
	}
	if out != nil {
		return resp.StatusCode, "", json.Unmarshal(raw, out)
	}
	return resp.StatusCode, "", nil
}

func setupUsers(ctx context.Context, cfg runConfig) ([]*virtualUser, error) {
	users := make([]*virtualUser, cfg.Users)
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		MaxIdleConns:          cfg.Users + cfg.SetupParallel,
		MaxIdleConnsPerHost:   cfg.Users + cfg.SetupParallel,
		IdleConnTimeout:       90 * time.Second,
		ResponseHeaderTimeout: cfg.RequestTimeout,
	}
	client := &http.Client{Transport: transport, Timeout: cfg.RequestTimeout}
	indices := make(chan int)
	errs := make(chan error, cfg.SetupParallel)
	var wg sync.WaitGroup
	for workerIndex := 0; workerIndex < cfg.SetupParallel; workerIndex++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range indices {
				baseURL := cfg.URLs[index%len(cfg.URLs)]
				var login loginResponse
				status, code, err := requestJSON(ctx, client, http.MethodPost, baseURL+"/api/v1/auth/guest-login", "", "", map[string]string{"device_id": fmt.Sprintf("%s-%08d", cfg.DevicePrefix, index)}, &login)
				if err != nil {
					errs <- fmt.Errorf("login user %d: status=%d code=%s: %w", index, status, code, err)
					return
				}
				var snapshot snapshotResponse
				_, _, err = requestJSON(ctx, client, http.MethodGet, baseURL+"/api/v1/farm/snapshot", login.AccessToken, "", nil, &snapshot)
				if err != nil {
					errs <- fmt.Errorf("snapshot user %d: %w", index, err)
					return
				}
				farm, err := newFarmState(snapshot)
				if err != nil {
					errs <- fmt.Errorf("snapshot state user %d: %w", index, err)
					return
				}
				users[index] = &virtualUser{index: index, baseURL: baseURL, token: login.AccessToken, userID: login.UserID, farm: farm, client: client, jobs: make(chan struct{}, 1), viewFarm: farm.farmID, viewVer: farm.version}
			}
		}()
	}
	go func() {
		defer close(indices)
		for i := 0; i < cfg.Users; i++ {
			select {
			case indices <- i:
			case <-ctx.Done():
				return
			}
		}
	}()
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			return nil, err
		}
	}
	return users, nil
}

func assignHotViewers(users []*virtualUser, counts []int) {
	next := 0
	for _, count := range counts {
		if next >= len(users) {
			return
		}
		owner := users[next]
		for i := 0; i < count && next+i < len(users); i++ {
			users[next+i].viewFarm = owner.farm.farmID
			users[next+i].viewVer = owner.farm.snapshotVersion()
		}
		next += count
	}
}

func websocketEndpoint(baseURL, token string) string {
	u, _ := url.Parse(baseURL)
	if u.Scheme == "https" {
		u.Scheme = "wss"
	} else {
		u.Scheme = "ws"
	}
	u.Path = "/ws"
	q := u.Query()
	q.Set("token", token)
	u.RawQuery = q.Encode()
	return u.String()
}

func (v *virtualUser) connect(ctx context.Context, timeout time.Duration) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.ws != nil {
		_ = v.ws.Close()
	}
	dialer := *websocket.DefaultDialer
	dialer.HandshakeTimeout = timeout
	conn, _, err := dialer.DialContext(ctx, websocketEndpoint(v.baseURL, v.token), nil)
	if err != nil {
		return err
	}
	v.ws = conn
	frame := wscontract.Frame{Meta: wscontract.Meta{Type: wscontract.FrameTypeSubscribeFarm, FarmID: v.viewFarm}, Body: wscontract.SubscribeFarmBody{SnapshotVersion: strconv.FormatInt(v.viewVer, 10)}}
	if err := conn.WriteJSON(frame); err != nil {
		_ = conn.Close()
		v.ws = nil
		return err
	}
	_, _, err = v.readACKLocked(timeout, "")
	return err
}

func (v *virtualUser) refreshOwnSnapshot(ctx context.Context, timeout time.Duration) error {
	requestCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var snapshot snapshotResponse
	if _, _, err := requestJSON(requestCtx, v.client, http.MethodGet, v.baseURL+"/api/v1/farm/snapshot", v.token, "", nil, &snapshot); err != nil {
		return err
	}
	if err := v.farm.replace(snapshot); err != nil {
		return err
	}
	if v.viewFarm == v.farm.farmID {
		v.viewVer = v.farm.snapshotVersion()
	}
	return nil
}

func (v *virtualUser) readACKLocked(timeout time.Duration, cmdID string) (wscontract.ACKBody, string, error) {
	if err := v.ws.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return wscontract.ACKBody{}, "", err
	}
	for {
		_, raw, err := v.ws.ReadMessage()
		if err != nil {
			return wscontract.ACKBody{}, "", err
		}
		var frame rawFrame
		if err := json.Unmarshal(raw, &frame); err != nil {
			return wscontract.ACKBody{}, "", err
		}
		if frame.Meta.Type != wscontract.FrameTypeACK || (cmdID != "" && frame.Meta.CmdID != cmdID) {
			continue
		}
		var ack wscontract.ACKBody
		if err := json.Unmarshal(frame.Body, &ack); err != nil {
			return wscontract.ACKBody{}, "", err
		}
		return ack, frame.Meta.CmdID, nil
	}
}

type pendingCommand struct {
	name        string
	skipReason  string
	cmdID       string
	baseVersion int64
	plotID      int32
	body        any
}

func chooseCommand(mix workloadMix, sequence uint64, farm *farmState) pendingCommand {
	point := int(sequence % uint64(mix.total()))
	if point < mix.Purchase {
		return pendingCommand{name: "Purchase", cmdID: id.NewV7(), body: map[string]any{"crop_id": "WHEAT", "quantity": 1}}
	}
	point -= mix.Purchase
	if point < mix.Sell {
		return pendingCommand{name: "Sell", cmdID: id.NewV7(), body: map[string]any{"crop_id": "WHEAT", "quantity": 1}}
	}
	point -= mix.Sell
	if point < mix.Plant {
		if plotID, ok := farm.choosePlant(); ok {
			return pendingCommand{name: "Plant", cmdID: id.NewV7(), baseVersion: farm.snapshotVersion(), plotID: plotID, body: wscontract.PlantBody{PlotID: plotID, SeedItemID: "WHEAT"}}
		}
		return pendingCommand{name: "Plant", skipReason: "no_plantable_plot"}
	}
	point -= mix.Plant
	if point < mix.Water {
		if plotID, ok := farm.chooseWater(); ok {
			return pendingCommand{name: "Water", cmdID: id.NewV7(), baseVersion: farm.snapshotVersion(), plotID: plotID, body: wscontract.WaterBody{PlotID: plotID}}
		}
		return pendingCommand{name: "Water", skipReason: "no_waterable_plot"}
	}
	if mix.Harvest > 0 {
		if plotID, ok := farm.chooseHarvest(time.Now()); ok {
			return pendingCommand{name: "Harvest", cmdID: id.NewV7(), baseVersion: farm.snapshotVersion(), plotID: plotID, body: wscontract.HarvestBody{PlotID: plotID}}
		}
		return pendingCommand{name: "Harvest", skipReason: "no_harvestable_plot"}
	}
	return pendingCommand{}
}

func isRejected(status int, code string) bool {
	return status == http.StatusTooManyRequests || status == http.StatusConflict || strings.Contains(code, "RESOURCE_EXHAUSTED") || strings.Contains(code, "VERSION_CONFLICT")
}

func transportErrorReason(err error) string {
	if errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "Client.Timeout") {
		return "downstream_timeout"
	}
	return err.Error()
}

func (v *virtualUser) runHTTP(ctx context.Context, cmd pendingCommand, stats *counters, timeout time.Duration) {
	endpoint := "/api/v1/shop/purchase"
	if cmd.name == "Sell" {
		endpoint = "/api/v1/farm/sell"
	}
	started := time.Now()
	for attempt := 0; attempt < 2; attempt++ {
		requestCtx, cancel := context.WithTimeout(ctx, timeout)
		status, code, err := requestJSON(requestCtx, v.client, http.MethodPost, v.baseURL+endpoint, v.token, cmd.cmdID, cmd.body, nil)
		cancel()
		if err == nil {
			stats.success(false, time.Since(started))
			return
		}
		if attempt == 0 && status == 0 && ctx.Err() == nil {
			stats.retried.Add(1)
			continue
		}
		if code == "" {
			code = transportErrorReason(err)
		}
		stats.error(code, isRejected(status, code))
		return
	}
}

func methodFor(name string) string { return "farm." + name }

func supportsDurableReplay(name string) bool {
	// These commands write cmd_receipts in the authoritative transaction.
	// Water is version-fenced but intentionally has no durable receipt.
	return name == "Plant" || name == "Harvest"
}

func (v *virtualUser) runWS(ctx context.Context, cmd pendingCommand, stats *counters, cfg runConfig, sequence uint64) {
	started := time.Now()
	for attempt := 0; attempt < 2; attempt++ {
		v.mu.Lock()
		if v.ws == nil {
			v.mu.Unlock()
			if err := v.refreshOwnSnapshot(ctx, cfg.RequestTimeout); err != nil {
				stats.error("SNAPSHOT_REFRESH", false)
				return
			}
			if err := v.connect(ctx, cfg.RequestTimeout); err != nil {
				stats.error("WS_CONNECT", false)
				return
			}
			v.mu.Lock()
		}
		v.clientSeq++
		clientSeq := v.clientSeq
		if attempt > 0 {
			// A retry may be sent on a newly established WebSocket after the
			// original ACK was lost. Zero asks the server to use cmd_id
			// idempotency without treating the retry as a continuation of the
			// previous connection's client sequence.
			clientSeq = 0
		}
		frame := wscontract.Frame{Meta: wscontract.Meta{Type: wscontract.FrameTypeCommand, Method: methodFor(cmd.name), ClientSeq: clientSeq, CmdID: cmd.cmdID, FarmID: v.farm.farmID, BaseVersion: strconv.FormatInt(cmd.baseVersion, 10)}, Body: cmd.body}
		err := v.ws.WriteJSON(frame)
		discard := cfg.ACKLossEvery > 0 && int64(sequence)%cfg.ACKLossEvery == 0 && attempt == 0 && supportsDurableReplay(cmd.name)
		if err == nil && discard {
			// Model an ACK lost after the command reached the server. Closing the
			// socket immediately can instead cancel delivery and turns this into
			// a transport-loss test with an indeterminate first attempt.
			timer := time.NewTimer(200 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				v.mu.Unlock()
				return
			case <-timer.C:
			}
			_ = v.ws.Close()
			v.ws = nil
			v.mu.Unlock()
			stats.retried.Add(1)
			continue
		}
		var ack wscontract.ACKBody
		if err == nil {
			ack, _, err = v.readACKLocked(cfg.RequestTimeout, cmd.cmdID)
		}
		if err != nil {
			_ = v.ws.Close()
			v.ws = nil
			v.mu.Unlock()
			if attempt == 0 && ctx.Err() == nil {
				stats.retried.Add(1)
				continue
			}
			stats.error("WS_ACK", false)
			if cmd.name == "Water" {
				v.farm.undoWater(cmd.plotID)
			}
			return
		}
		v.mu.Unlock()
		if ack.Result != "OK" {
			stats.error(ack.Result, isRejected(0, ack.Result))
			if cmd.name == "Water" {
				v.farm.undoWater(cmd.plotID)
			}
			return
		}
		version, parseErr := strconv.ParseInt(ack.NewVersion, 10, 64)
		if parseErr != nil {
			stats.error("INVALID_ACK_VERSION", false)
			return
		}
		if err := v.farm.advance(version, ack.Patch); err != nil {
			stats.versions.Add(1)
			stats.error("FARM_VERSION_INVARIANT", false)
			return
		}
		// Reconcile from the public snapshot after every farm mutation. ACK
		// patches intentionally carry only changed fields, while command
		// selection needs the complete plot state (including fields cleared by
		// Harvest and state observed after a replay).
		if err := v.refreshOwnSnapshot(ctx, cfg.RequestTimeout); err != nil {
			stats.error("SNAPSHOT_RECONCILE", false)
			return
		}
		stats.success(true, time.Since(started))
		return
	}
}

func (v *virtualUser) run(ctx context.Context, cfg runConfig, stats *counters, sequence *atomic.Uint64, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case <-v.jobs:
			stats.inflight.Add(1)
			seq := sequence.Add(1)
			// Route the configured mix per virtual user instead of only globally.
			// At a target RPS below the user count, global sequencing otherwise
			// assigns a user the same command forever (for example Sell before it
			// has ever purchased inventory), which invalidates the workload.
			v.commandSequence++
			cmd := chooseCommand(cfg.Mix, v.commandSequence, v.farm)
			if cmd.skipReason != "" {
				stats.skip(cmd.name, cmd.skipReason)
				stats.inflight.Add(-1)
				continue
			}
			if cmd.name == "" {
				stats.skip("unknown", "empty_command")
				stats.inflight.Add(-1)
				continue
			}
			stats.command(cmd.name)
			if cmd.name == "Purchase" || cmd.name == "Sell" {
				v.runHTTP(ctx, cmd, stats, cfg.RequestTimeout)
			} else {
				v.runWS(ctx, cmd, stats, cfg, seq)
			}
			stats.inflight.Add(-1)
		}
	}
}

type minuteSample struct {
	ElapsedSeconds int64   `json:"elapsed_seconds"`
	Attempted      uint64  `json:"attempted"`
	Accepted       uint64  `json:"accepted"`
	Rejected       uint64  `json:"rejected"`
	Failed         uint64  `json:"failed"`
	Skipped        uint64  `json:"skipped_commands"`
	AcceptedRPS    float64 `json:"accepted_rps"`
}

func dispatch(ctx context.Context, users []*virtualUser, targetRPS int, stats *counters, samples *[]minuteSample, sampleMu *sync.Mutex) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	started := time.Now()
	lastSample := started
	lastAccepted := uint64(0)
	cursor := 0
	credit := 0.0
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			credit += float64(targetRPS) / 100
			jobs := int(credit)
			credit -= float64(jobs)
			for i := 0; i < jobs; i++ {
				stats.attempted.Add(1)
				user := users[cursor%len(users)]
				cursor++
				select {
				case user.jobs <- struct{}{}:
				default:
					stats.queueFull.Add(1)
					stats.error("loadgen_queue", false)
				}
			}
			if now.Sub(lastSample) >= time.Minute {
				accepted := stats.accepted.Load()
				seconds := now.Sub(lastSample).Seconds()
				sample := minuteSample{ElapsedSeconds: int64(now.Sub(started).Seconds()), Attempted: stats.attempted.Load(), Accepted: accepted, Rejected: stats.rejected.Load(), Failed: stats.failed.Load(), Skipped: stats.skipped.Load(), AcceptedRPS: float64(accepted-lastAccepted) / seconds}
				sampleMu.Lock()
				*samples = append(*samples, sample)
				sampleMu.Unlock()
				lastAccepted, lastSample = accepted, now
			}
		}
	}
}

type result struct {
	Status              string            `json:"status"`
	FailureReasons      []string          `json:"failure_reasons,omitempty"`
	StartedAt           string            `json:"started_at"`
	DurationSeconds     float64           `json:"duration_seconds"`
	TargetRPS           int               `json:"target_rps"`
	MinAcceptedRatio    float64           `json:"min_accepted_ratio"`
	RequiredAcceptedRPS float64           `json:"required_accepted_rps"`
	Users               int               `json:"users"`
	Gateways            int               `json:"gateways"`
	MixConfigured       workloadMix       `json:"mix_configured"`
	CommandsActual      map[string]uint64 `json:"commands_actual"`
	Attempted           uint64            `json:"attempted"`
	Accepted            uint64            `json:"accepted"`
	ACKOK               uint64            `json:"ack_ok"`
	Rejected            uint64            `json:"rejected"`
	Failed              uint64            `json:"failed"`
	Retried             uint64            `json:"retried"`
	QueueFull           uint64            `json:"loadgen_queue_full"`
	SkippedCommands     uint64            `json:"skipped_commands"`
	Skips               map[string]uint64 `json:"skip_reasons"`
	AcceptedRPS         float64           `json:"accepted_rps"`
	P50MS               float64           `json:"p50_ms"`
	P95MS               float64           `json:"p95_ms"`
	P99MS               float64           `json:"p99_ms"`
	Errors              map[string]uint64 `json:"errors"`
	VersionErrors       uint64            `json:"farm_version_errors"`
	MinuteSamples       []minuteSample    `json:"minute_samples"`
	HotViewers          []int             `json:"hot_viewers"`
}

const (
	statusComplete    = "COMPLETE"
	statusInterrupted = "INTERRUPTED"
	statusFailed      = "FAILED"
)

var (
	errRunInterrupted = errors.New("measurement interrupted")
	errRunFailed      = errors.New("measurement failed acceptance criteria")
)

func resultStatus(cfg runConfig, stats *counters, elapsed time.Duration, interrupted bool) (string, []string) {
	if interrupted {
		return statusInterrupted, []string{"measurement_interrupted"}
	}
	var reasons []string
	if stats.failed.Load() > 0 {
		reasons = append(reasons, "failed_commands")
	}
	if stats.rejected.Load() > 0 {
		reasons = append(reasons, "rejected_commands")
	}
	if stats.queueFull.Load() > 0 {
		reasons = append(reasons, "loadgen_queue_full")
	}
	if stats.skipped.Load() > 0 {
		reasons = append(reasons, "skipped_commands")
	}
	if stats.versions.Load() > 0 {
		reasons = append(reasons, "farm_version_errors")
	}
	acceptedRPS := 0.0
	if elapsed > 0 {
		acceptedRPS = float64(stats.accepted.Load()) / elapsed.Seconds()
	}
	if acceptedRPS < float64(cfg.TargetRPS)*cfg.MinAcceptRatio {
		reasons = append(reasons, "accepted_rps_below_required")
	}
	if len(reasons) > 0 {
		return statusFailed, reasons
	}
	return statusComplete, nil
}

func snapshotResult(cfg runConfig, stats *counters, started time.Time, elapsed time.Duration, samples []minuteSample, interrupted bool) result {
	stats.mu.Lock()
	errorsCopy := make(map[string]uint64, len(stats.errors))
	commandsCopy := make(map[string]uint64, len(stats.commands))
	skipsCopy := make(map[string]uint64, len(stats.skips))
	for key, value := range stats.errors {
		errorsCopy[key] = value
	}
	for key, value := range stats.commands {
		commandsCopy[key] = value
	}
	for key, value := range stats.skips {
		skipsCopy[key] = value
	}
	stats.mu.Unlock()
	accepted := stats.accepted.Load()
	status, failureReasons := resultStatus(cfg, stats, elapsed, interrupted)
	acceptedRPS := 0.0
	if elapsed > 0 {
		acceptedRPS = float64(accepted) / elapsed.Seconds()
	}
	return result{Status: status, FailureReasons: failureReasons, StartedAt: started.UTC().Format(time.RFC3339), DurationSeconds: elapsed.Seconds(), TargetRPS: cfg.TargetRPS, MinAcceptedRatio: cfg.MinAcceptRatio, RequiredAcceptedRPS: float64(cfg.TargetRPS) * cfg.MinAcceptRatio, Users: cfg.Users, Gateways: len(cfg.URLs), MixConfigured: cfg.Mix, CommandsActual: commandsCopy, Attempted: stats.attempted.Load(), Accepted: accepted, ACKOK: stats.ackOK.Load(), Rejected: stats.rejected.Load(), Failed: stats.failed.Load(), Retried: stats.retried.Load(), QueueFull: stats.queueFull.Load(), SkippedCommands: stats.skipped.Load(), Skips: skipsCopy, AcceptedRPS: acceptedRPS, P50MS: stats.latency.percentile(.50), P95MS: stats.latency.percentile(.95), P99MS: stats.latency.percentile(.99), Errors: errorsCopy, VersionErrors: stats.versions.Load(), MinuteSamples: append([]minuteSample(nil), samples...), HotViewers: append([]int(nil), cfg.HotViewers...)}
}

func writeResult(path string, value any) error {
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	if path == "" {
		_, err = os.Stdout.Write(raw)
		return err
	}
	return os.WriteFile(path, raw, 0o644)
}

func runContext(parent context.Context, args []string) error {
	cfg, err := parseConfig(args)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	users, err := setupUsers(ctx, cfg)
	if err != nil {
		return err
	}
	if cfg.SetupOnly {
		return writeResult(cfg.Output, map[string]any{"status": "SETUP_COMPLETE", "users": len(users), "device_prefix": cfg.DevicePrefix})
	}
	if !cfg.HTTPOnly {
		assignHotViewers(users, cfg.HotViewers)
		for _, user := range users {
			if err := user.connect(ctx, cfg.RequestTimeout); err != nil {
				return fmt.Errorf("connect user %d: %w", user.index, err)
			}
		}
	}
	defer func() {
		for _, user := range users {
			if user.ws != nil {
				_ = user.ws.Close()
			}
		}
	}()

	var sequence atomic.Uint64
	if cfg.Warmup > 0 {
		warmStats := newCounters()
		warmWorkerCtx, stopWarmWorkers := context.WithCancel(ctx)
		var warmWorkers sync.WaitGroup
		for _, user := range users {
			warmWorkers.Add(1)
			go user.run(warmWorkerCtx, cfg, warmStats, &sequence, &warmWorkers)
		}
		warmCtx, stopWarmDispatch := context.WithTimeout(ctx, cfg.Warmup)
		var ignored []minuteSample
		var ignoredMu sync.Mutex
		dispatch(warmCtx, users, cfg.TargetRPS, warmStats, &ignored, &ignoredMu)
		stopWarmDispatch()
		stopWarmWorkers()
		warmWorkers.Wait()
		for _, user := range users {
			select {
			case <-user.jobs:
			default:
			}
		}
	}
	stats := newCounters()
	workerCtx, stopWorkers := context.WithCancel(ctx)
	var workers sync.WaitGroup
	for _, user := range users {
		workers.Add(1)
		go user.run(workerCtx, cfg, stats, &sequence, &workers)
	}
	started := time.Now()
	measuredCtx, stopMeasured := context.WithTimeout(ctx, cfg.Duration)
	var samples []minuteSample
	var sampleMu sync.Mutex
	dispatch(measuredCtx, users, cfg.TargetRPS, stats, &samples, &sampleMu)
	measurementElapsed := time.Since(started)
	interrupted := parent.Err() != nil
	stopMeasured()
	drainDeadline := time.Now().Add(2 * cfg.RequestTimeout)
	for time.Now().Before(drainDeadline) {
		queued := 0
		for _, user := range users {
			queued += len(user.jobs)
		}
		if queued == 0 && stats.inflight.Load() == 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	stopWorkers()
	workers.Wait()
	sampleMu.Lock()
	final := snapshotResult(cfg, stats, started, measurementElapsed, samples, interrupted)
	sampleMu.Unlock()
	if err := writeResult(cfg.Output, final); err != nil {
		return err
	}
	switch final.Status {
	case statusInterrupted:
		return fmt.Errorf("%w: %s", errRunInterrupted, strings.Join(final.FailureReasons, ","))
	case statusFailed:
		return fmt.Errorf("%w: %s", errRunFailed, strings.Join(final.FailureReasons, ","))
	default:
		return nil
	}
}

func run(args []string) error { return runContext(context.Background(), args) }

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := runContext(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "route95_stateful:", err)
		os.Exit(1)
	}
}
