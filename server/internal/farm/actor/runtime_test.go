package actor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/photon/farm-server/server/internal/farm/application"
	"github.com/photon/farm-server/server/internal/farm/domain"
	"github.com/photon/farm-server/server/internal/farm/infrastructure"
	"github.com/photon/farm-server/server/pkg/clock"
	"github.com/photon/farm-server/server/pkg/errcode"
)

// TestSubmit_PlantThenIdempotentReplay 验证：命令经 Actor 串行提交后版本递增；
// 同一 cmd_id 重试命中幂等回执，不重复结算（ADR-018）。
func TestSubmit_PlantThenIdempotentReplay(t *testing.T) {
	committer := infrastructure.NewMemCommitter(clock.System{})
	rt := NewRuntime(4, 16, committer)
	ctx := t.Context()
	rt.Start(ctx)

	cmd := domain.Command{
		CmdID:       "cmd-1",
		FarmID:      1001,
		ActorUser:   1001,
		Type:        domain.CmdPlant,
		PlotID:      3,
		CropID:      "wheat",
		BaseVersion: 0,
	}
	res1, err := rt.Submit(ctx, cmd)
	if err != nil {
		t.Fatalf("first submit failed: %v", err)
	}
	if res1.NewVersion != 1 {
		t.Fatalf("expected version 1, got %d", res1.NewVersion)
	}
	if res1.Replayed {
		t.Fatalf("first submit should not be replayed")
	}

	// 同 cmd_id 重试：应命中幂等回执，版本不再递增。
	res2, err := rt.Submit(ctx, cmd)
	if err != nil {
		t.Fatalf("replay submit failed: %v", err)
	}
	if !res2.Replayed {
		t.Fatalf("second submit should be replayed")
	}
	if res2.NewVersion != res1.NewVersion {
		t.Fatalf("replay must not advance version: %d vs %d", res2.NewVersion, res1.NewVersion)
	}
}

// TestSubmit_IdempotentReplayWithStaleNonZeroBase verifies that a hot Actor
// does not reject an exact ACK-loss retry before gamesvr can consult its
// durable receipt. BaseVersion=0 on a new farm would hide this ordering bug.
func TestSubmit_IdempotentReplayWithStaleNonZeroBase(t *testing.T) {
	committer := infrastructure.NewMemCommitter(clock.System{})
	rt := NewRuntime(4, 16, committer)
	ctx := t.Context()
	rt.Start(ctx)
	farmID := int64(1002)
	if _, err := rt.Submit(ctx, domain.Command{
		CmdID: "route95-prior", FarmID: farmID, ActorUser: farmID,
		Type: domain.CmdPlant, PlotID: 0, CropID: "WHEAT",
	}); err != nil {
		t.Fatalf("prior command: %v", err)
	}
	command := domain.Command{
		CmdID: "route95-ack-loss-nonzero", FarmID: farmID, ActorUser: farmID,
		Type: domain.CmdPlant, PlotID: 1, CropID: "WHEAT", BaseVersion: 1,
	}
	first, err := rt.Submit(ctx, command)
	if err != nil || first.NewVersion != 2 || first.Replayed {
		t.Fatalf("first command: result=%+v err=%v", first, err)
	}
	replayed, err := rt.Submit(ctx, command)
	if err != nil || !replayed.Replayed || replayed.NewVersion != first.NewVersion {
		t.Fatalf("stale-base replay: first=%+v replay=%+v err=%v", first, replayed, err)
	}
}

// TestSubmit_VersionConflict 验证 base_version 过旧在预校验阶段被拒绝。
func TestSubmit_VersionConflict(t *testing.T) {
	committer := infrastructure.NewMemCommitter(clock.System{})
	rt := NewRuntime(4, 16, committer)
	ctx := t.Context()
	rt.Start(ctx)

	base := domain.Command{CmdID: "c1", FarmID: 2001, ActorUser: 2001, Type: domain.CmdPlant, PlotID: 1, CropID: "wheat", BaseVersion: 0}
	if _, err := rt.Submit(ctx, base); err != nil {
		t.Fatalf("plant failed: %v", err)
	}
	// version=99 远大于实际版本 1；预校验应拒绝。
	stale := domain.Command{CmdID: "c2", FarmID: 2001, ActorUser: 2001, Type: domain.CmdPlant, PlotID: 2, CropID: "wheat", BaseVersion: 99}
	if _, err := rt.Submit(ctx, stale); err == nil {
		t.Fatalf("expected version conflict, got nil")
	}
}

// TestSubmit_PreValidate_StaleVersionAfterCommit 验证：第一个命令提交后，
// 携带旧 base_version 的新命令仍由 gamesvr 权威版本校验拒绝。
func TestSubmit_PreValidate_StaleVersionAfterCommit(t *testing.T) {
	committer := infrastructure.NewMemCommitter(clock.System{})
	rt := NewRuntime(4, 16, committer)
	ctx := t.Context()
	rt.Start(ctx)

	farmID := int64(3001)

	// 第一个命令成功，快照版本变为 1。
	cmd1 := domain.Command{CmdID: "c1", FarmID: farmID, ActorUser: 1, Type: domain.CmdPlant, PlotID: 0, CropID: "wheat", BaseVersion: 0}
	res1, err := rt.Submit(ctx, cmd1)
	if err != nil {
		t.Fatalf("first cmd failed: %v", err)
	}
	if res1.NewVersion != 1 {
		t.Fatalf("expected version 1 after first commit, got %d", res1.NewVersion)
	}

	// 第二个命令携带显式旧版本（99），应被权威提交层拒绝。
	cmd2 := domain.Command{CmdID: "c2", FarmID: farmID, ActorUser: 1, Type: domain.CmdPlant, PlotID: 1, CropID: "corn", BaseVersion: 99}
	_, err = rt.Submit(ctx, cmd2)
	if err == nil {
		t.Fatal("expected version conflict for stale base_version, got nil")
	}
	var ec *errcode.Error
	if !errors.As(err, &ec) {
		t.Fatalf("expected errcode.Error, got %T: %v", err, err)
	}
	if ec.Code != errcode.FarmVersionConflict {
		t.Errorf("expected FarmVersionConflict, got %s", ec.Code)
	}
}

// TestSubmit_HotSnapshot_NotUpdatedOnGameSvrFailure 验证：gamesvr 失败后，
// farmsvr 不更新热快照（下次命令仍可用正确版本预校验）。
func TestSubmit_HotSnapshot_NotUpdatedOnGameSvrFailure(t *testing.T) {
	// failOnSecond：前 N 次成功，第 N+1 次返回错误。
	real := infrastructure.NewMemCommitter(clock.System{})
	fail := &failAfterCommitter{inner: real, failAfter: 1}
	rt := NewRuntime(4, 16, fail)
	ctx := t.Context()
	rt.Start(ctx)

	farmID := int64(4001)

	// 第一次成功：版本应变为 1，热快照更新。
	cmd1 := domain.Command{CmdID: "c1", FarmID: farmID, ActorUser: 1, Type: domain.CmdPlant, PlotID: 0, CropID: "wheat"}
	res1, err := rt.Submit(ctx, cmd1)
	if err != nil {
		t.Fatalf("first cmd should succeed: %v", err)
	}
	if res1.NewVersion != 1 {
		t.Fatalf("expected version 1, got %d", res1.NewVersion)
	}

	// 第二次失败：gamesvr 返回错误，热快照不应被更新（仍为版本 1）。
	cmd2 := domain.Command{CmdID: "c2", FarmID: farmID, ActorUser: 1, Type: domain.CmdPlant, PlotID: 1, CropID: "corn"}
	if _, err := rt.Submit(ctx, cmd2); err == nil {
		t.Fatal("second cmd should fail (gamesvr error)")
	}

	// 第三次用 base_version=1（当前正确版本），应通过预校验。
	// 注意：failAfter 此时已经超过，不再 fail。
	fail.setFailAfter(999) // 之后不再失败
	cmd3 := domain.Command{CmdID: "c3", FarmID: farmID, ActorUser: 1, Type: domain.CmdPlant, PlotID: 1, CropID: "corn", BaseVersion: 1}
	if _, err := rt.Submit(ctx, cmd3); err != nil {
		t.Fatalf("third cmd with correct base_version should succeed: %v", err)
	}
}

// TestSubmit_ConcurrentCommandsSamePlot 验证：两个并发命令争抢同一地块，
// Actor 保证串行，只有一个成功；另一个因地块已被占用而失败。
func TestSubmit_ConcurrentCommandsSamePlot(t *testing.T) {
	committer := infrastructure.NewMemCommitter(clock.System{})
	rt := NewRuntime(4, 16, committer)
	ctx := t.Context()
	rt.Start(ctx)

	farmID := int64(5001)
	var wg sync.WaitGroup
	errs := make([]error, 2)

	for i := range 2 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, errs[idx] = rt.Submit(ctx, domain.Command{
				CmdID:  fmt.Sprintf("concurrent-%d", idx),
				FarmID: farmID,
				Type:   domain.CmdPlant,
				PlotID: 7,
				CropID: "wheat",
			})
		}(i)
	}
	wg.Wait()

	success := 0
	for _, err := range errs {
		if err == nil {
			success++
		}
	}
	if success != 1 {
		t.Errorf("expected exactly 1 success for concurrent same-plot plant, got %d (errs: %v)", success, errs)
	}
}

// TestSubmit_ReadyQueueFull 验证 worker 和全局 ready queue 都饱和后快速背压。
func TestSubmit_ReadyQueueFull(t *testing.T) {
	started := make(chan struct{})
	block := make(chan struct{})
	slow := &signalSlowCommitter{started: started, block: block}

	cfg := DefaultConfig()
	cfg.SchedulerShards, cfg.IngressCap, cfg.Workers, cfg.ReadyCap = 4, 128, 1, 1
	cfg.ExecutionTimeout = 30 * time.Second
	rt := NewRuntimeWithConfig(cfg, slow, nil, clock.System{})
	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		close(block)
		cancel()
	}()
	rt.Start(ctx)

	submit := func(farmID int64, id string) error {
		_, err := rt.Submit(ctx, domain.Command{CmdID: id, FarmID: farmID, Type: domain.CmdPlant, PlotID: 0, CropID: "wheat"})
		return err
	}

	// 1. farm 6001 提交 slow-1，等待其占满信号量。
	slowDone := make(chan error, 1)
	go func() { slowDone <- submit(6001, "slow-1") }()
	<-started

	// 2. farm 6002 占满 ready queue。
	queued := make(chan error, 1)
	go func() { queued <- submit(6002, "queued") }()
	deadline := time.Now().Add(time.Second)
	for rt.Stats().Ready != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if rt.Stats().Ready != 1 {
		t.Fatal("ready queue did not fill")
	}

	// 3. farm 6003 无槽位，应在有界等待后返回背压。
	err := submit(6003, "overflow")
	if err == nil {
		t.Fatal("expected RESOURCE_EXHAUSTED for semaphore overflow, got nil")
	}
	var ec *errcode.Error
	if !errors.As(err, &ec) {
		t.Fatalf("expected *errcode.Error, got %T: %v", err, err)
	}
	if ec.Code != errcode.ResourceExhausted {
		t.Errorf("expected ResourceExhausted, got %s", ec.Code)
	}
}

// TestSubmit_CtxCancelDrainsPendingTasks 验证 runtime 取消时，已在 per-farm FIFO
// 排队的命令能立即拿到 ctx.Err()，而不是阻塞到命令超时。
func TestSubmit_CtxCancelDrainsPendingTasks(t *testing.T) {
	started := make(chan struct{})
	block := make(chan struct{})
	slow := &signalSlowCommitter{started: started, block: block}

	rt := newRuntimeWithSemCap(4, 128, slow, 1)
	ctx, cancel := context.WithCancel(context.Background())
	rt.Start(ctx)

	submit := func(c context.Context, farmID int64, id string) error {
		_, err := rt.Submit(c, domain.Command{CmdID: id, FarmID: farmID, Type: domain.CmdPlant, PlotID: 0, CropID: "wheat"})
		return err
	}

	// slow-1 占住信号量并阻塞。
	slowDone := make(chan error, 1)
	go func() { slowDone <- submit(ctx, 7001, "slow-1") }()
	<-started

	// cmd-2 进入同 farm worker 队列，等待信号量释放。
	done2 := make(chan error, 1)
	go func() { done2 <- submit(ctx, 7001, "cmd-2") }()

	// 取消 ctx：slow-1 因 ctx.Done 退出，worker 退出前 drain 队列，cmd-2 应立即返回。
	cancel()
	close(block) // 解除 slow committer 阻塞

	select {
	case err := <-done2:
		if err == nil {
			t.Fatal("expected error after ctx cancel, got nil")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cmd-2 did not return after ctx cancel; worker may not be draining queue")
	}
}

// TestSubmit_CompletedActorRemainsReachable 验证命令完成后轻量 actorState 可继续接收命令。
func TestSubmit_CompletedActorRemainsReachable(t *testing.T) {
	committer := infrastructure.NewMemCommitter(clock.System{})
	rt := NewRuntime(4, 16, committer)
	ctx := t.Context()
	rt.Start(ctx)

	farmID := int64(8001)

	// 第一次提交：创建 actorState。
	cmd1 := domain.Command{CmdID: "r1", FarmID: farmID, ActorUser: farmID, Type: domain.CmdPlant, PlotID: 0, CropID: "wheat"}
	if _, err := rt.Submit(ctx, cmd1); err != nil {
		t.Fatalf("first submit: %v", err)
	}

	// 第二次提交：复用 actorState，再由任一空闲固定 worker 执行。
	cmd2 := domain.Command{CmdID: "r2", FarmID: farmID, ActorUser: farmID, Type: domain.CmdHarvest, PlotID: 0}
	if _, err := rt.Submit(ctx, cmd2); err != nil {
		// 允许业务错误（如地块未成熟），但不允许容量错误。
		var ec *errcode.Error
		if errors.As(err, &ec) && ec.Code == errcode.ResourceExhausted {
			t.Fatalf("second submit returned ResourceExhausted: %v", err)
		}
	}
}

func TestSlowFarmDoesNotBlockSameSchedulerShard(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	committer := &selectiveSlowCommitter{slowFarm: 9001, started: started, release: release}
	cfg := DefaultConfig()
	cfg.SchedulerShards = 1
	cfg.Workers = 2
	cfg.ReadyCap = 2
	cfg.ExecutionTimeout = time.Second
	rt := NewRuntimeWithConfig(cfg, committer, nil, clock.System{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt.Start(ctx)
	slowDone := make(chan error, 1)
	go func() {
		_, err := rt.Submit(ctx, domain.Command{CmdID: "slow", FarmID: 9001, Type: domain.CmdPlant, PlotID: 1, CropID: "wheat"})
		slowDone <- err
	}()
	<-started
	done := make(chan error, 1)
	go func() {
		_, err := rt.Submit(ctx, domain.Command{CmdID: "fast", FarmID: 9002, Type: domain.CmdPlant, PlotID: 1, CropID: "wheat"})
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("unrelated farm blocked behind slow farm")
	}
	close(release)
	if err := <-slowDone; err != nil {
		t.Fatalf("slow farm submit: %v", err)
	}
}

func TestCallerCancelDoesNotCancelAcceptedCommand(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	committed := make(chan struct{})
	c := &acceptedCommitter{started: started, release: release, committed: committed}
	cfg := DefaultConfig()
	cfg.Workers = 1
	cfg.ExecutionTimeout = time.Second
	rt := NewRuntimeWithConfig(cfg, c, nil, clock.System{})
	runtimeCtx, cancelRuntime := context.WithCancel(context.Background())
	defer cancelRuntime()
	rt.Start(runtimeCtx)
	requestCtx, cancelRequest := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := rt.Submit(requestCtx, domain.Command{CmdID: "accepted", FarmID: 9101, Type: domain.CmdPlant, PlotID: 1, CropID: "wheat"})
		done <- err
	}()
	<-started
	cancelRequest()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("caller result=%v", err)
	}
	close(release)
	select {
	case <-committed:
	case <-time.After(time.Second):
		t.Fatal("accepted command did not finish")
	}
}

func TestPerFarmFIFOIsBounded(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	c := &signalSlowCommitter{started: started, block: release}
	cfg := DefaultConfig()
	cfg.SchedulerShards = 1
	cfg.Workers = 1
	cfg.ReadyCap = 1
	cfg.FarmQueueCap = 8
	cfg.IngressCap = 64
	cfg.ExecutionTimeout = 30 * time.Second
	rt := NewRuntimeWithConfig(cfg, c, nil, clock.System{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt.Start(ctx)

	const farmID = 9201
	results := make(chan error, cfg.FarmQueueCap+1)
	go func() {
		_, err := rt.Submit(ctx, domain.Command{CmdID: "running", FarmID: farmID})
		results <- err
	}()
	<-started
	for i := 0; i < cfg.FarmQueueCap; i++ {
		go func(i int) {
			_, err := rt.Submit(ctx, domain.Command{CmdID: fmt.Sprintf("queued-%d", i), FarmID: farmID})
			results <- err
		}(i)
	}
	waitForStats(t, rt, func(s Stats) bool { return s.Queued == int64(cfg.FarmQueueCap) })

	_, err := rt.Submit(ctx, domain.Command{CmdID: "overflow", FarmID: farmID})
	var ec *errcode.Error
	if !errors.As(err, &ec) || ec.Code != errcode.ResourceExhausted {
		t.Fatalf("overflow error=%v, want ResourceExhausted", err)
	}
	close(release)
	for i := 0; i < cfg.FarmQueueCap+1; i++ {
		<-results
	}
}

func TestFixedWorkersBoundConcurrentExecution(t *testing.T) {
	const workers = 3
	c := newConcurrencyCommitter(workers)
	cfg := DefaultConfig()
	cfg.SchedulerShards = 4
	cfg.Workers = workers
	cfg.ReadyCap = 16
	cfg.ExecutionTimeout = 30 * time.Second
	rt := NewRuntimeWithConfig(cfg, c, nil, clock.System{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt.Start(ctx)

	results := make(chan error, 12)
	for i := 0; i < 12; i++ {
		go func(i int) {
			_, err := rt.Submit(ctx, domain.Command{CmdID: fmt.Sprintf("farm-%d", i), FarmID: int64(9300 + i)})
			results <- err
		}(i)
	}
	select {
	case <-c.full:
	case <-time.After(time.Second):
		t.Fatal("workers did not reach configured concurrency")
	}
	if got := c.max.Load(); got != workers {
		t.Fatalf("max concurrent executions=%d, want %d", got, workers)
	}
	if got := rt.Stats().WorkerBusy; got != workers {
		t.Fatalf("busy workers=%d, want %d", got, workers)
	}
	close(c.release)
	for range 12 {
		if err := <-results; err != nil {
			t.Fatalf("submit failed: %v", err)
		}
	}
}

func TestActiveActorStatesAreBounded(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxActiveActors = 1
	rt := NewRuntimeWithConfig(cfg, infrastructure.NewMemCommitter(clock.System{}), nil, clock.System{})
	ctx := t.Context()
	rt.Start(ctx)
	if _, err := rt.Submit(ctx, domain.Command{CmdID: "first", FarmID: 9401, Type: domain.CmdPlant, PlotID: 1, CropID: "wheat"}); err != nil {
		t.Fatalf("first submit: %v", err)
	}
	_, err := rt.Submit(ctx, domain.Command{CmdID: "second", FarmID: 9402, Type: domain.CmdPlant, PlotID: 1, CropID: "wheat"})
	var ec *errcode.Error
	if !errors.As(err, &ec) || ec.Code != errcode.ResourceExhausted {
		t.Fatalf("second error=%v, want ResourceExhausted", err)
	}
	if got := errcode.RetryAfter(err); got != cfg.RetryAfter {
		t.Fatalf("retry_after=%s, want %s", got, cfg.RetryAfter)
	}
	if got := rt.Stats().ActiveActors; got != 1 {
		t.Fatalf("active actors=%d, want 1", got)
	}
}

func waitForStats(t *testing.T, rt *Runtime, ready func(Stats) bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if ready(rt.Stats()) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("runtime stats did not reach expected state: %+v", rt.Stats())
}

// ── test helpers ─────────────────────────────────────────────────────────────

// failAfterCommitter 前 failAfter 次代理 inner，之后返回固定错误。
type failAfterCommitter struct {
	inner     application.Committer
	mu        sync.Mutex
	count     int
	failAfter int
}

func (f *failAfterCommitter) setFailAfter(n int) {
	f.mu.Lock()
	f.failAfter = n
	f.mu.Unlock()
}

func (f *failAfterCommitter) CommitFarmCommand(ctx context.Context, req application.CommitRequest) (application.CommitResult, error) {
	f.mu.Lock()
	f.count++
	should := f.count > f.failAfter
	f.mu.Unlock()
	if should {
		return application.CommitResult{}, errcode.New(errcode.Internal, "injected gamesvr failure")
	}
	return f.inner.CommitFarmCommand(ctx, req)
}

// signalSlowCommitter 进入 CommitFarmCommand 时关闭 started，然后阻塞直到 block 关闭。
type signalSlowCommitter struct {
	started chan struct{}
	block   chan struct{}
	once    sync.Once
}

type selectiveSlowCommitter struct {
	slowFarm         int64
	started, release chan struct{}
	once             sync.Once
}

func (s *selectiveSlowCommitter) CommitFarmCommand(ctx context.Context, req application.CommitRequest) (application.CommitResult, error) {
	if req.Command.FarmID == s.slowFarm {
		s.once.Do(func() { close(s.started) })
		select {
		case <-s.release:
		case <-ctx.Done():
			return application.CommitResult{}, ctx.Err()
		}
	}
	return application.CommitResult{NewVersion: 1}, nil
}

type acceptedCommitter struct {
	started, release, committed chan struct{}
	once                        sync.Once
}

type concurrencyCommitter struct {
	current atomic.Int64
	max     atomic.Int64
	full    chan struct{}
	release chan struct{}
	once    sync.Once
	target  int64
}

func newConcurrencyCommitter(target int64) *concurrencyCommitter {
	return &concurrencyCommitter{full: make(chan struct{}), release: make(chan struct{}), target: target}
}

func (c *concurrencyCommitter) CommitFarmCommand(ctx context.Context, _ application.CommitRequest) (application.CommitResult, error) {
	n := c.current.Add(1)
	defer c.current.Add(-1)
	for {
		old := c.max.Load()
		if n <= old || c.max.CompareAndSwap(old, n) {
			break
		}
	}
	if n == c.target {
		c.once.Do(func() { close(c.full) })
	}
	select {
	case <-c.release:
		return application.CommitResult{NewVersion: 1}, nil
	case <-ctx.Done():
		return application.CommitResult{}, ctx.Err()
	}
}

func (a *acceptedCommitter) CommitFarmCommand(ctx context.Context, _ application.CommitRequest) (application.CommitResult, error) {
	a.once.Do(func() { close(a.started) })
	select {
	case <-a.release:
		close(a.committed)
		return application.CommitResult{NewVersion: 1}, nil
	case <-ctx.Done():
		return application.CommitResult{}, ctx.Err()
	}
}

func (s *signalSlowCommitter) CommitFarmCommand(ctx context.Context, _ application.CommitRequest) (application.CommitResult, error) {
	s.once.Do(func() { close(s.started) })
	select {
	case <-s.block:
	case <-ctx.Done():
		return application.CommitResult{}, ctx.Err()
	}
	return application.CommitResult{}, errors.New("slow committer unblocked")
}

func TestRuntime_RouteEpochPromotionAndStaleOwnerRejection(t *testing.T) {
	mem := infrastructure.NewMemCommitter(clock.System{})
	r := NewRuntimeFull(1, 8, mem, mem, clock.System{}).WithRouteFencer(mem)
	r.Start(t.Context())
	t.Cleanup(func() { r.Stop(); r.Wait() })

	_, err := r.Submit(t.Context(), domain.Command{CmdID: "epoch-4", FarmID: 44, ActorUser: 44, Type: domain.CmdPlant, PlotID: 1, CropID: "WHEAT", RouteEpoch: 4})
	if err != nil {
		t.Fatalf("new owner submit: %v", err)
	}
	snapshot, err := mem.LoadSnapshot(t.Context(), 44)
	if err != nil || snapshot.RouteEpoch != 4 {
		t.Fatalf("snapshot=%+v err=%v", snapshot, err)
	}

	_, err = r.Submit(t.Context(), domain.Command{CmdID: "epoch-3", FarmID: 44, ActorUser: 44, Type: domain.CmdPlant, PlotID: 2, CropID: "WHEAT", RouteEpoch: 3})
	var coded *errcode.Error
	if !errors.As(err, &coded) || coded.Code != errcode.RoutingFenced {
		t.Fatalf("stale owner err=%v", err)
	}
}
