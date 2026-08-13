// Package actor implements logical per-farm serialization on a bounded shared runtime.
package actor

import (
	"context"
	"errors"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/photon/farm-server/server/internal/farm/application"
	"github.com/photon/farm-server/server/internal/farm/domain"
	"github.com/photon/farm-server/server/pkg/clock"
	"github.com/photon/farm-server/server/pkg/errcode"
)

type Config struct {
	SchedulerShards  int
	IngressCap       int
	FarmQueueCap     int
	ReadyCap         int
	Workers          int
	MaxActiveActors  int
	EnqueueWait      time.Duration
	RetryAfter       time.Duration
	ExecutionTimeout time.Duration
	IdleTTL          time.Duration
}

func DefaultConfig() Config {
	return Config{SchedulerShards: 64, IngressCap: 256, FarmQueueCap: 8, ReadyCap: 128, Workers: 64, MaxActiveActors: 100000, EnqueueWait: 5 * time.Millisecond, RetryAfter: 50 * time.Millisecond, ExecutionTimeout: 3 * time.Second, IdleTTL: 5 * time.Minute}
}

type BackpressureError struct{ RetryAfter time.Duration }

func (e *BackpressureError) Error() string { return "farm actor capacity exhausted" }
func (e *BackpressureError) Unwrap() error {
	return errcode.New(errcode.ResourceExhausted, "farm actor capacity exhausted")
}
func (e *BackpressureError) RetryAfterDuration() time.Duration { return e.RetryAfter }

var ErrMailboxFull error = &BackpressureError{RetryAfter: 50 * time.Millisecond}

type task struct {
	cmd        domain.Command
	result     chan commitOutcome
	enqueuedAt time.Time
}

type commitOutcome struct {
	res application.CommitResult
	err error
}

type actorState struct {
	farmID   int64
	snapshot *domain.Snapshot
	running  bool
	inflight *task
	queue    []task
	lastUsed time.Time
}

type completion struct {
	state    *actorState
	task     task
	snapshot *domain.Snapshot
	out      commitOutcome
}

type workItem struct {
	shard    *shardState
	state    *actorState
	task     task
	snapshot *domain.Snapshot
}

type shardState struct {
	inbox     chan task
	completed chan completion
	actors    map[int64]*actorState
}

type Stats struct {
	ActiveActors int64
	Queued       int64
	Ready        int
	WorkerBusy   int64
	Workers      int
}

type Observer interface {
	ActorStateDelta(int)
	ActorQueueDelta(int)
	ActorWorkerDelta(int)
	ActorReadyDelta(int)
	ActorQueueWait(time.Duration)
	ActorRejected(string)
}

type Runtime struct {
	cfg          Config
	shards       []*shardState
	ready        chan workItem
	committer    application.Committer
	loader       application.SnapshotLoader
	fencer       application.RouteFencer
	clk          clock.Clock
	observer     Observer
	ctx          context.Context
	cancel       context.CancelFunc
	startOnce    sync.Once
	wg           sync.WaitGroup
	activeActors atomic.Int64
	queued       atomic.Int64
	workerBusy   atomic.Int64
}

func NewRuntime(shardCount, mailboxCap int, committer application.Committer) *Runtime {
	cfg := DefaultConfig()
	cfg.SchedulerShards = shardCount
	cfg.IngressCap = mailboxCap
	return NewRuntimeWithConfig(cfg, committer, nil, clock.System{})
}

func NewRuntimeFull(shardCount, mailboxCap int, committer application.Committer, loader application.SnapshotLoader, clk clock.Clock) *Runtime {
	cfg := DefaultConfig()
	cfg.SchedulerShards = shardCount
	cfg.IngressCap = mailboxCap
	return NewRuntimeWithConfig(cfg, committer, loader, clk)
}

// newRuntimeWithSemCap is retained for existing tests; semCap now means fixed workers.
func newRuntimeWithSemCap(shardCount, mailboxCap int, committer application.Committer, semCap int) *Runtime {
	cfg := DefaultConfig()
	cfg.SchedulerShards = shardCount
	cfg.IngressCap = mailboxCap
	cfg.Workers = semCap
	cfg.ReadyCap = semCap
	return NewRuntimeWithConfig(cfg, committer, nil, clock.System{})
}

func NewRuntimeWithConfig(cfg Config, committer application.Committer, loader application.SnapshotLoader, clk clock.Clock) *Runtime {
	def := DefaultConfig()
	if cfg.SchedulerShards <= 0 {
		cfg.SchedulerShards = def.SchedulerShards
	}
	if cfg.IngressCap <= 0 {
		cfg.IngressCap = def.IngressCap
	}
	if cfg.FarmQueueCap <= 0 {
		cfg.FarmQueueCap = def.FarmQueueCap
	}
	if cfg.ReadyCap <= 0 {
		cfg.ReadyCap = def.ReadyCap
	}
	if cfg.Workers <= 0 {
		cfg.Workers = def.Workers
	}
	if cfg.MaxActiveActors <= 0 {
		cfg.MaxActiveActors = def.MaxActiveActors
	}
	if cfg.EnqueueWait <= 0 {
		cfg.EnqueueWait = def.EnqueueWait
	}
	if cfg.RetryAfter <= 0 {
		cfg.RetryAfter = def.RetryAfter
	}
	if cfg.ExecutionTimeout <= 0 {
		cfg.ExecutionTimeout = def.ExecutionTimeout
	}
	if cfg.IdleTTL <= 0 {
		cfg.IdleTTL = def.IdleTTL
	}
	if clk == nil {
		clk = clock.System{}
	}
	r := &Runtime{cfg: cfg, ready: make(chan workItem, cfg.ReadyCap), committer: committer, loader: loader, clk: clk}
	r.shards = make([]*shardState, cfg.SchedulerShards)
	for i := range r.shards {
		r.shards[i] = &shardState{inbox: make(chan task, cfg.IngressCap), completed: make(chan completion, cfg.Workers), actors: make(map[int64]*actorState)}
	}
	return r
}

func (r *Runtime) WithObserver(o Observer) *Runtime                   { r.observer = o; return r }
func (r *Runtime) WithRouteFencer(f application.RouteFencer) *Runtime { r.fencer = f; return r }

func (r *Runtime) Start(parent context.Context) {
	r.startOnce.Do(func() {
		r.ctx, r.cancel = context.WithCancel(parent)
		for _, s := range r.shards {
			r.wg.Add(1)
			go r.schedulerLoop(s)
		}
		for range r.cfg.Workers {
			r.wg.Add(1)
			go r.workerLoop()
		}
	})
}

func (r *Runtime) Stop() {
	if r.cancel != nil {
		r.cancel()
	}
}
func (r *Runtime) Wait() { r.wg.Wait() }

func (r *Runtime) Stats() Stats {
	return Stats{ActiveActors: r.activeActors.Load(), Queued: r.queued.Load(), Ready: len(r.ready), WorkerBusy: r.workerBusy.Load(), Workers: r.cfg.Workers}
}

func (r *Runtime) shardFor(farmID int64) *shardState {
	h := fnv.New32a()
	var b [8]byte
	for i := range 8 {
		b[i] = byte(farmID >> (8 * i))
	}
	_, _ = h.Write(b[:])
	return r.shards[int(h.Sum32())%len(r.shards)]
}

func (r *Runtime) Submit(ctx context.Context, cmd domain.Command) (application.CommitResult, error) {
	if r.ctx == nil {
		return application.CommitResult{}, errors.New("actor runtime not started")
	}
	t := task{cmd: cmd, result: make(chan commitOutcome, 1), enqueuedAt: time.Now()}
	timer := time.NewTimer(r.cfg.EnqueueWait)
	defer timer.Stop()
	select {
	case r.shardFor(cmd.FarmID).inbox <- t:
	case <-timer.C:
		r.reject("ingress_full")
		return application.CommitResult{}, r.backpressure()
	case <-r.ctx.Done():
		return application.CommitResult{}, r.ctx.Err()
	case <-ctx.Done():
		return application.CommitResult{}, ctx.Err()
	}
	select {
	case out := <-t.result:
		return out.res, out.err
	case <-ctx.Done():
		return application.CommitResult{}, ctx.Err()
	case <-r.ctx.Done():
		return application.CommitResult{}, r.ctx.Err()
	}
}

func (r *Runtime) schedulerLoop(s *shardState) {
	defer r.wg.Done()
	tickEvery := r.cfg.IdleTTL / 2
	if tickEvery > time.Minute {
		tickEvery = time.Minute
	}
	if tickEvery < time.Second {
		tickEvery = time.Second
	}
	ticker := time.NewTicker(tickEvery)
	defer ticker.Stop()
	for {
		select {
		case <-r.ctx.Done():
			r.failAll(s, r.ctx.Err())
			return
		case t := <-s.inbox:
			r.accept(s, t)
		case c := <-s.completed:
			r.complete(s, c)
		case now := <-ticker.C:
			r.evictIdle(s, now)
		}
	}
}

func (r *Runtime) accept(s *shardState, t task) {
	st := s.actors[t.cmd.FarmID]
	created := false
	if st == nil {
		if !r.tryActorAdd() {
			r.reject("actor_limit")
			deliver(t, commitOutcome{err: r.backpressure()})
			return
		}
		st = &actorState{farmID: t.cmd.FarmID, lastUsed: time.Now()}
		s.actors[t.cmd.FarmID] = st
		created = true
	}
	if st.running {
		if len(st.queue) >= r.cfg.FarmQueueCap {
			r.reject("farm_fifo_full")
			deliver(t, commitOutcome{err: r.backpressure()})
			return
		}
		st.queue = append(st.queue, t)
		r.queueDelta(1)
		return
	}
	if !r.tryDispatch(s, st, t) {
		r.reject("ready_full")
		deliver(t, commitOutcome{err: r.backpressure()})
		if created {
			delete(s.actors, st.farmID)
			r.actorDelta(-1)
		}
	}
}

func (r *Runtime) tryDispatch(s *shardState, st *actorState, t task) bool {
	w := workItem{shard: s, state: st, task: t, snapshot: st.snapshot}
	select {
	case r.ready <- w:
		r.readyDelta(1)
		st.running = true
		st.inflight = &t
		st.lastUsed = time.Now()
		return true
	default:
		return false
	}
}

func (r *Runtime) complete(s *shardState, c completion) {
	st := s.actors[c.state.farmID]
	if st != c.state {
		deliver(c.task, c.out)
		return
	}
	st.running = false
	st.inflight = nil
	st.snapshot = c.snapshot
	st.lastUsed = time.Now()
	deliver(c.task, c.out)
	if len(st.queue) == 0 {
		return
	}
	next := st.queue[0]
	st.queue = st.queue[1:]
	r.queueDelta(-1)
	// A completing worker immediately returns to ready, so this bounded handoff cannot wait on downstream I/O.
	select {
	case r.ready <- workItem{shard: s, state: st, task: next, snapshot: st.snapshot}:
		r.readyDelta(1)
		st.running = true
		st.inflight = &next
	case <-r.ctx.Done():
		deliver(next, commitOutcome{err: r.ctx.Err()})
	}
}

func (r *Runtime) workerLoop() {
	defer r.wg.Done()
	for {
		select {
		case <-r.ctx.Done():
			return
		case w := <-r.ready:
			r.readyDelta(-1)
			r.observeWait(time.Since(w.task.enqueuedAt))
			r.workerDelta(1)
			c := r.execute(w)
			r.workerDelta(-1)
			select {
			case w.shard.completed <- c:
			case <-r.ctx.Done():
				deliver(w.task, commitOutcome{err: r.ctx.Err()})
			}
		}
	}
}

func (r *Runtime) execute(w workItem) completion {
	ctx, cancel := context.WithTimeout(r.ctx, r.cfg.ExecutionTimeout)
	defer cancel()
	snap := w.snapshot
	if snap == nil || w.task.cmd.RouteEpoch > snap.RouteEpoch {
		loaded, err := r.coldLoad(ctx, w.task.cmd.FarmID, w.task.cmd.RouteEpoch)
		if err != nil {
			return completion{state: w.state, task: w.task, snapshot: nil, out: commitOutcome{err: err}}
		}
		snap = &loaded
	}
	if r.committer == nil {
		return completion{state: w.state, task: w.task, snapshot: snap, out: commitOutcome{err: errors.New("committer not wired")}}
	}
	// The hot snapshot can be newer than the BaseVersion carried by an exact
	// retry whose ACK was lost. Durable receipts live in gamesvr/MySQL, so a
	// command with a stable CmdID must reach the authoritative committer even
	// when the local pre-check is stale. A new stale command is still rejected
	// by the same version/epoch checks inside the gamesvr transaction.
	if err := preValidate(snap, w.task.cmd); err != nil && w.task.cmd.CmdID == "" {
		return completion{state: w.state, task: w.task, snapshot: snap, out: commitOutcome{err: err}}
	}
	res, err := r.committer.CommitFarmCommand(ctx, application.CommitRequest{Command: w.task.cmd})
	if err == nil && !res.Replayed {
		applyPatchToSnapshot(snap, res.Patch, res.NewVersion)
	}
	return completion{state: w.state, task: w.task, snapshot: snap, out: commitOutcome{res: res, err: err}}
}

func (r *Runtime) failAll(s *shardState, err error) {
	for _, st := range s.actors {
		if st.inflight != nil {
			deliver(*st.inflight, commitOutcome{err: err})
		}
		for _, t := range st.queue {
			deliver(t, commitOutcome{err: err})
			r.queueDelta(-1)
		}
		r.actorDelta(-1)
	}
	for {
		select {
		case t := <-s.inbox:
			deliver(t, commitOutcome{err: err})
		default:
			return
		}
	}
}

func (r *Runtime) evictIdle(s *shardState, now time.Time) {
	for id, st := range s.actors {
		if !st.running && len(st.queue) == 0 && now.Sub(st.lastUsed) >= r.cfg.IdleTTL {
			delete(s.actors, id)
			r.actorDelta(-1)
		}
	}
}
func deliver(t task, out commitOutcome) {
	select {
	case t.result <- out:
	default:
	}
}
func (r *Runtime) actorDelta(n int) {
	r.activeActors.Add(int64(n))
	if r.observer != nil {
		r.observer.ActorStateDelta(n)
	}
}
func (r *Runtime) tryActorAdd() bool {
	for {
		current := r.activeActors.Load()
		if current >= int64(r.cfg.MaxActiveActors) {
			return false
		}
		if r.activeActors.CompareAndSwap(current, current+1) {
			if r.observer != nil {
				r.observer.ActorStateDelta(1)
			}
			return true
		}
	}
}
func (r *Runtime) queueDelta(n int) {
	r.queued.Add(int64(n))
	if r.observer != nil {
		r.observer.ActorQueueDelta(n)
	}
}
func (r *Runtime) workerDelta(n int) {
	r.workerBusy.Add(int64(n))
	if r.observer != nil {
		r.observer.ActorWorkerDelta(n)
	}
}
func (r *Runtime) readyDelta(n int) {
	if r.observer != nil {
		r.observer.ActorReadyDelta(n)
	}
}
func (r *Runtime) reject(reason string) {
	if r.observer != nil {
		r.observer.ActorRejected(reason)
	}
}
func (r *Runtime) backpressure() error { return &BackpressureError{RetryAfter: r.cfg.RetryAfter} }
func (r *Runtime) observeWait(d time.Duration) {
	if r.observer != nil {
		r.observer.ActorQueueWait(d)
	}
}

func (r *Runtime) coldLoad(ctx context.Context, farmID, routeEpoch int64) (domain.Snapshot, error) {
	if routeEpoch > 0 && r.fencer != nil {
		if err := r.fencer.AdvanceRouteEpoch(ctx, farmID, routeEpoch); err != nil {
			return domain.Snapshot{}, err
		}
	}
	if r.loader == nil {
		return domain.Snapshot{FarmID: farmID, OwnerID: farmID, RouteEpoch: routeEpoch, Plots: make(map[int32]domain.Plot)}, nil
	}
	return r.loader.LoadSnapshot(ctx, farmID)
}
func preValidate(s *domain.Snapshot, cmd domain.Command) error {
	if cmd.RouteEpoch != s.RouteEpoch {
		return errcode.New(errcode.RoutingFenced, "pre-validate: route epoch does not own farm")
	}
	if cmd.Type != domain.CmdPetAutoHarvest && cmd.BaseVersion != s.Version {
		return errcode.New(errcode.FarmVersionConflict, "pre-validate: base_version stale")
	}
	return nil
}
func applyPatchToSnapshot(s *domain.Snapshot, patch domain.Patch, version int64) {
	s.Version = version
	for _, p := range patch.Plots {
		s.Plots[p.PlotID] = p
	}
}
