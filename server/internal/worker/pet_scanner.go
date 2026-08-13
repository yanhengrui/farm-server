// Package worker provides workersvr-specific background tasks.
package worker

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/photon/farm-server/server/internal/farm/application"
	"github.com/photon/farm-server/server/internal/farm/domain"
)

const (
	defaultPetScanWorkers = 4
	defaultPetScanRate    = 50
	defaultPetLeaseTTL    = 15 * time.Second
)

// FarmCommandClient submits an authoritative farm command.
type FarmCommandClient interface {
	SubmitCommand(context.Context, domain.Command) (application.CommitResult, error)
}

// PetScanMetrics keeps worker independent from the Prometheus implementation.
// The shard label is stable and never contains a farm or user identifier.
type PetScanMetrics interface {
	PetScanDuration(shard string, d time.Duration)
	PetSubmission(shard, result string)
}

// PetScannerOptions configures the bounded work performed by one physical
// shard scanner. Rate is farms per second, shared by its fixed worker pool.
type PetScannerOptions struct {
	Shard      string
	LeaseTTL   time.Duration
	Workers    int
	RatePerSec int
	Metrics    PetScanMetrics
}

// PetScanResult is used by the scheduler to decide whether it should drain
// immediately or back off. Claimed includes farms without a mature plot.
type PetScanResult struct {
	Claimed      int
	Harvested    int
	Rescheduled  int
	SubmitFailed int
}

// PetScanner scans due pet schedules and submits CmdPetAutoHarvest commands.
// A database lease makes the scanner safe when several workersvr processes run.
type PetScanner struct {
	db         *sql.DB
	farmClient FarmCommandClient
	log        *slog.Logger
	opts       PetScannerOptions
}

// NewPetScanner retains the small default used by unit tests and local runs.
func NewPetScanner(db *sql.DB, farmClient FarmCommandClient, log *slog.Logger) *PetScanner {
	return NewPetScannerWithOptions(db, farmClient, log, PetScannerOptions{})
}

func NewPetScannerWithOptions(db *sql.DB, farmClient FarmCommandClient, log *slog.Logger, opts PetScannerOptions) *PetScanner {
	if opts.Shard == "" {
		opts.Shard = "primary"
	}
	if opts.LeaseTTL <= 0 {
		opts.LeaseTTL = defaultPetLeaseTTL
	}
	if opts.Workers <= 0 {
		opts.Workers = defaultPetScanWorkers
	}
	if opts.RatePerSec <= 0 {
		opts.RatePerSec = defaultPetScanRate
	}
	return &PetScanner{db: db, farmClient: farmClient, log: log, opts: opts}
}

type farmToScan struct {
	farmID       int64
	ownerID      int64
	snapJSON     []byte
	nextActionAt time.Time
	leaseToken   string
}

// Scan claims the oldest due schedules in a short transaction, then processes
// them outside that transaction. FOR UPDATE SKIP LOCKED prevents another
// workersvr from selecting the same farm; the persisted lease covers process
// death after claim and before the authoritative command is submitted.
func (s *PetScanner) Scan(ctx context.Context, batchSize int) (result PetScanResult, err error) {
	started := time.Now()
	defer func() {
		if s.opts.Metrics != nil {
			s.opts.Metrics.PetScanDuration(s.opts.Shard, time.Since(started))
		}
	}()
	if batchSize <= 0 {
		return result, nil
	}
	farms, err := s.claimDue(ctx, batchSize)
	if err != nil {
		return result, err
	}
	result.Claimed = len(farms)
	if len(farms) == 0 {
		return result, nil
	}

	jobs := make(chan farmToScan)
	results := make(chan PetScanResult, len(farms))
	workers := s.opts.Workers
	if workers > len(farms) {
		workers = len(farms)
	}
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for farm := range jobs {
				results <- s.processClaimedFarm(ctx, farm)
			}
		}()
	}
	go func() {
		defer close(jobs)
		interval := time.Second / time.Duration(s.opts.RatePerSec)
		if interval <= 0 {
			interval = time.Nanosecond
		}
		limiter := time.NewTicker(interval)
		defer limiter.Stop()
		for _, farm := range farms {
			select {
			case <-ctx.Done():
				return
			case <-limiter.C:
			}
			select {
			case <-ctx.Done():
				return
			case jobs <- farm:
			}
		}
	}()
	wg.Wait()
	close(results)
	for r := range results {
		result.Harvested += r.Harvested
		result.Rescheduled += r.Rescheduled
		result.SubmitFailed += r.SubmitFailed
	}
	if ctx.Err() != nil {
		return result, ctx.Err()
	}
	return result, nil
}

// ScanAndHarvest is retained for callers that only need the completed harvest
// count. New scheduling code should use Scan so it can drain claimed work.
func (s *PetScanner) ScanAndHarvest(ctx context.Context, batchSize int) (int, error) {
	result, err := s.Scan(ctx, batchSize)
	return result.Harvested, err
}

func (s *PetScanner) claimDue(ctx context.Context, batchSize int) ([]farmToScan, error) {
	now := time.Now().UTC()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("pet_scan begin claim: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck
	const q = `
		SELECT fs.farm_id, fs.owner_user_id, fs.snapshot, fs.next_pet_action_at
		FROM farm_snapshots AS fs
		JOIN player_pets AS pp ON pp.user_id = fs.owner_user_id
			AND pp.status = 'ACTIVE' AND pp.auto_harvest_enabled = 1
		WHERE fs.next_pet_action_at IS NOT NULL AND fs.next_pet_action_at <= ?
			AND (fs.pet_scan_lease_until IS NULL OR fs.pet_scan_lease_until <= ?)
		ORDER BY fs.next_pet_action_at, fs.farm_id
		LIMIT ? FOR UPDATE SKIP LOCKED
	`
	rows, err := tx.QueryContext(ctx, q, now, now, batchSize)
	if err != nil {
		return nil, fmt.Errorf("pet_scan query: %w", err)
	}
	var farms []farmToScan
	for rows.Next() {
		var f farmToScan
		if err := rows.Scan(&f.farmID, &f.ownerID, &f.snapJSON, &f.nextActionAt); err != nil {
			rows.Close()
			return nil, fmt.Errorf("pet_scan scan: %w", err)
		}
		farms = append(farms, f)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	leaseUntil := now.Add(s.opts.LeaseTTL)
	for i := range farms {
		token, err := newPetLeaseToken()
		if err != nil {
			return nil, fmt.Errorf("pet_scan lease token: %w", err)
		}
		farms[i].leaseToken = token
		if _, err := tx.ExecContext(ctx,
			`UPDATE farm_snapshots SET pet_scan_lease_owner = ?, pet_scan_lease_until = ? WHERE farm_id = ?`,
			farms[i].leaseToken, leaseUntil, uint64(farms[i].farmID),
		); err != nil {
			return nil, fmt.Errorf("pet_scan claim farm_id=%d: %w", farms[i].farmID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("pet_scan commit claim: %w", err)
	}
	return farms, nil
}

func (s *PetScanner) processClaimedFarm(ctx context.Context, f farmToScan) PetScanResult {
	now := time.Now().UTC()
	renewed, err := s.renewLease(ctx, f.farmID, f.leaseToken, now)
	if err != nil {
		s.log.Warn("pet scan renew lease failed", slog.Int64("farm_id", f.farmID), slog.String("error", err.Error()))
		return PetScanResult{SubmitFailed: 1}
	}
	if !renewed {
		// A delayed batch must never submit or reschedule after another worker
		// has reclaimed this farm's expired lease.
		return PetScanResult{}
	}
	plotID, ok, err := findSmallestMaturePlotChecked(f.snapJSON, now)
	if err != nil {
		s.log.Warn("pet scan invalid snapshot", slog.Int64("farm_id", f.farmID), slog.String("error", err.Error()))
		return PetScanResult{SubmitFailed: 1}
	}
	if !ok {
		moved, err := s.pushNextPetAction(ctx, f.farmID, f.nextActionAt, f.leaseToken, now)
		if err != nil {
			s.log.Warn("pet scan reschedule failed", slog.Int64("farm_id", f.farmID), slog.String("error", err.Error()))
			return PetScanResult{SubmitFailed: 1}
		}
		if moved {
			return PetScanResult{Rescheduled: 1}
		}
		return PetScanResult{}
	}
	cmd := domain.Command{
		CmdID:          petAutoHarvestKey(f.farmID, f.nextActionAt, plotID),
		FarmID:         f.farmID,
		ActorUser:      f.ownerID,
		Type:           domain.CmdPetAutoHarvest,
		PlotID:         plotID,
		PetScheduledAt: f.nextActionAt,
	}
	if _, err := s.farmClient.SubmitCommand(ctx, cmd); err != nil {
		s.log.Warn("pet auto harvest failed", slog.Int64("farm_id", f.farmID), slog.Int64("plot_id", int64(plotID)), slog.String("error", err.Error()))
		if s.opts.Metrics != nil {
			s.opts.Metrics.PetSubmission(s.opts.Shard, "failed")
		}
		return PetScanResult{SubmitFailed: 1}
	}
	if s.opts.Metrics != nil {
		s.opts.Metrics.PetSubmission(s.opts.Shard, "succeeded")
	}
	return PetScanResult{Harvested: 1}
}

func petAutoHarvestKey(farmID int64, scheduledAt time.Time, plotID int32) string {
	return fmt.Sprintf("pet-auto:%d:%d:%d", farmID, scheduledAt.UTC().UnixMilli(), plotID)
}

func (s *PetScanner) renewLease(ctx context.Context, farmID int64, token string, now time.Time) (bool, error) {
	result, err := s.db.ExecContext(ctx,
		`UPDATE farm_snapshots SET pet_scan_lease_until = ? WHERE farm_id = ? AND pet_scan_lease_owner = ? AND pet_scan_lease_until > ?`,
		now.Add(s.opts.LeaseTTL), uint64(farmID), token, now,
	)
	if err != nil {
		return false, fmt.Errorf("pet_scan renew farm_id=%d: %w", farmID, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("pet_scan renew rows farm_id=%d: %w", farmID, err)
	}
	return affected == 1, nil
}

// pushNextPetAction only advances a still-valid lease matched by its unique
// claim token. A delayed worker cannot clear a later claimant's lease.
func (s *PetScanner) pushNextPetAction(ctx context.Context, farmID int64, expected time.Time, token string, now time.Time) (bool, error) {
	result, err := s.db.ExecContext(ctx,
		`UPDATE farm_snapshots
		 SET next_pet_action_at = ?, pet_scan_lease_owner = NULL, pet_scan_lease_until = NULL
		 WHERE farm_id = ? AND next_pet_action_at = ? AND pet_scan_lease_owner = ? AND pet_scan_lease_until > ?`,
		now.Add(30*time.Second), uint64(farmID), expected, token, now,
	)
	if err != nil {
		return false, fmt.Errorf("pet_scan reschedule farm_id=%d: %w", farmID, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("pet_scan reschedule rows farm_id=%d: %w", farmID, err)
	}
	return affected == 1, nil
}

// newPetLeaseToken is per claim, rather than per process. This prevents a
// restarted or delayed worker from acting on a lease another worker reclaimed.
func newPetLeaseToken() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

type petPlotRaw struct {
	Status   string    `json:"status"`
	MatureAt time.Time `json:"mature_at"`
}

type petSnapRaw struct {
	Plots map[string]petPlotRaw `json:"plots"`
}

func findSmallestMaturePlot(snapJSON []byte, now time.Time) (int32, bool) {
	plotID, ok, _ := findSmallestMaturePlotChecked(snapJSON, now)
	return plotID, ok
}

func findSmallestMaturePlotChecked(snapJSON []byte, now time.Time) (int32, bool, error) {
	var snap petSnapRaw
	if err := json.Unmarshal(snapJSON, &snap); err != nil {
		return 0, false, err
	}
	if len(snap.Plots) == 0 {
		return 0, false, nil
	}
	var maturePlots []int32
	for key, p := range snap.Plots {
		if p.Status == "GROWING" && !p.MatureAt.IsZero() && !now.Before(p.MatureAt) {
			plotID, err := strconv.ParseInt(key, 10, 32)
			if err == nil {
				maturePlots = append(maturePlots, int32(plotID))
			}
		}
	}
	if len(maturePlots) == 0 {
		return 0, false, nil
	}
	sort.Slice(maturePlots, func(i, j int) bool { return maturePlots[i] < maturePlots[j] })
	return maturePlots[0], true, nil
}
