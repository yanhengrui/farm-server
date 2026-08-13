package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/photon/farm-server/server/internal/economy/transport/assetrpc"
	"github.com/photon/farm-server/server/internal/farm/transport/farmrpc"
	"github.com/photon/farm-server/server/pkg/errcode"
	"github.com/redis/go-redis/v9"
)

const readCacheVersion = "v1"

var (
	cacheFillScript = redis.NewScript(`
local current = redis.call('GET', KEYS[2])
if not current then current = '0' end
if current ~= ARGV[1] then return 0 end
redis.call('PSETEX', KEYS[1], ARGV[2], ARGV[3])
return 1`)
	cacheInvalidateScript = redis.NewScript(`
redis.call('INCR', KEYS[2])
return redis.call('DEL', KEYS[1])`)
)

// ReadCacheObserver deliberately exposes only bounded resource/result labels.
type ReadCacheObserver interface {
	ReadCache(resource, result string, duration time.Duration)
}

// ReadCache is a shared, fail-open Redis cache for read-heavy public views.
// MySQL remains authoritative. A generation key prevents a read that started
// before a successful write from repopulating stale data after invalidation.
type ReadCache struct {
	rdb      redis.Cmdable
	prefix   string
	ttl      time.Duration
	staleTTL time.Duration
	timeout  time.Duration
	observer ReadCacheObserver
}

func NewReadCache(rdb redis.Cmdable, prefix string, ttl, staleTTL, timeout time.Duration, observer ReadCacheObserver) *ReadCache {
	if prefix == "" {
		prefix = "farm:read"
	}
	if timeout <= 0 {
		timeout = 25 * time.Millisecond
	}
	if staleTTL < ttl {
		staleTTL = ttl
	}
	return &ReadCache{rdb: rdb, prefix: prefix, ttl: ttl, staleTTL: staleTTL, timeout: timeout, observer: observer}
}

type cacheEnvelope struct {
	FreshUntil int64           `json:"fresh_until_unix_ms"`
	Payload    json.RawMessage `json:"payload"`
}

func (c *ReadCache) snapshotKey(farmID int64) string {
	return fmt.Sprintf("%s:%s:snapshot:%d", c.prefix, readCacheVersion, farmID)
}

func (c *ReadCache) assetsKey(userID int64) string {
	return fmt.Sprintf("%s:%s:assets:%d", c.prefix, readCacheVersion, userID)
}

func (c *ReadCache) observe(resource, result string, started time.Time) {
	if c.observer != nil {
		c.observer.ReadCache(resource, result, time.Since(started))
	}
}

func (c *ReadCache) commandContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(parent), c.timeout)
}

// load returns a generation token only when Redis was healthy. Callers skip
// cache fill when it is empty so Redis incidents cannot slow or fail DB reads.
func (c *ReadCache) load(ctx context.Context, resource, key string, dst any) (state, generation string) {
	started := time.Now()
	cacheCtx, cancel := c.commandContext(ctx)
	defer cancel()
	raw, err := c.rdb.Get(cacheCtx, key).Bytes()
	if err == nil {
		var envelope cacheEnvelope
		if json.Unmarshal(raw, &envelope) == nil && json.Unmarshal(envelope.Payload, dst) == nil {
			if time.Now().UnixMilli() <= envelope.FreshUntil {
				c.observe(resource, "hit", started)
				return "hit", ""
			}
			generation, err = c.rdb.Get(cacheCtx, key+":gen").Result()
			if err == redis.Nil {
				generation = "0"
			} else if err != nil {
				c.observe(resource, "error", started)
				return "miss", ""
			}
			c.observe(resource, "stale", started)
			return "stale", generation
		}
		c.observe(resource, "corrupt", started)
		_ = c.invalidateKey(ctx, resource, key)
		return "miss", ""
	}
	if err != redis.Nil {
		c.observe(resource, "error", started)
		return "miss", ""
	}
	generation, err = c.rdb.Get(cacheCtx, key+":gen").Result()
	if err == redis.Nil {
		generation = "0"
	} else if err != nil {
		c.observe(resource, "error", started)
		return "miss", ""
	}
	c.observe(resource, "miss", started)
	return "miss", generation
}

func (c *ReadCache) fill(ctx context.Context, resource, key, generation string, value any, identity int64) {
	if generation == "" || c.ttl <= 0 {
		return
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return
	}
	raw, err := json.Marshal(cacheEnvelope{FreshUntil: time.Now().Add(c.ttl).UnixMilli(), Payload: payload})
	if err != nil {
		return
	}
	// Stable per-entity jitter spreads expirations without a shared RNG lock.
	ttl := c.staleTTL + time.Duration(identity%1000)*c.staleTTL/5000
	started := time.Now()
	cacheCtx, cancel := c.commandContext(ctx)
	defer cancel()
	result, err := cacheFillScript.Run(cacheCtx, c.rdb, []string{key, key + ":gen"}, generation, strconv.FormatInt(ttl.Milliseconds(), 10), raw).Int()
	if err != nil {
		c.observe(resource, "fill_error", started)
		return
	}
	if result == 0 {
		c.observe(resource, "stale_fill_dropped", started)
		return
	}
	c.observe(resource, "filled", started)
}

func canServeStale(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return false
	}
	var coded *errcode.Error
	if !errors.As(err, &coded) {
		return true
	}
	switch coded.Code {
	case errcode.Internal, errcode.ResourceExhausted, errcode.RoutingOwnerNotFound,
		errcode.RoutingEpochStale, errcode.FarmActorMigrating:
		return true
	default:
		return false
	}
}

func (c *ReadCache) invalidateKey(ctx context.Context, resource, key string) error {
	started := time.Now()
	cacheCtx, cancel := c.commandContext(ctx)
	defer cancel()
	err := cacheInvalidateScript.Run(cacheCtx, c.rdb, []string{key, key + ":gen"}).Err()
	if err != nil {
		c.observe(resource, "invalidate_error", started)
		return err
	}
	c.observe(resource, "invalidated", started)
	return nil
}

// Invalidate removes public farm state and private player assets after an
// authoritative command commits. Failures are observable and bounded by TTL.
func (c *ReadCache) Invalidate(ctx context.Context, farmID, userID int64) {
	if c == nil {
		return
	}
	if farmID > 0 {
		_ = c.invalidateKey(ctx, "snapshot", c.snapshotKey(farmID))
	}
	if userID > 0 {
		_ = c.invalidateKey(ctx, "assets", c.assetsKey(userID))
	}
}

type CachedSnapshotClient struct {
	inner FarmSnapshotClient
	cache *ReadCache
}

func NewCachedSnapshotClient(inner FarmSnapshotClient, cache *ReadCache) *CachedSnapshotClient {
	return &CachedSnapshotClient{inner: inner, cache: cache}
}

func (c *CachedSnapshotClient) GetSnapshot(ctx context.Context, farmID int64) (*farmrpc.FarmSnapshotDTO, error) {
	var cached farmrpc.FarmSnapshotDTO
	key := c.cache.snapshotKey(farmID)
	state, generation := c.cache.load(ctx, "snapshot", key, &cached)
	if state == "hit" {
		return &cached, nil
	}
	value, err := c.inner.GetSnapshot(ctx, farmID)
	if err != nil && state == "stale" && canServeStale(err) {
		c.cache.observe("snapshot", "stale_served", time.Now())
		return &cached, nil
	}
	if err == nil && value != nil {
		c.cache.fill(ctx, "snapshot", key, generation, value, farmID)
	}
	return value, err
}

type CachedAssetClient struct {
	inner PlayerAssetClient
	cache *ReadCache
}

func NewCachedAssetClient(inner PlayerAssetClient, cache *ReadCache) *CachedAssetClient {
	return &CachedAssetClient{inner: inner, cache: cache}
}

func (c *CachedAssetClient) GetPlayerAssets(ctx context.Context, userID int64) (*assetrpc.AssetsDTO, error) {
	var cached assetrpc.AssetsDTO
	key := c.cache.assetsKey(userID)
	state, generation := c.cache.load(ctx, "assets", key, &cached)
	if state == "hit" {
		if cached.Inventory == nil {
			cached.Inventory = []assetrpc.InventoryItemDTO{}
		}
		return &cached, nil
	}
	value, err := c.inner.GetPlayerAssets(ctx, userID)
	if err != nil && state == "stale" && canServeStale(err) {
		if cached.Inventory == nil {
			cached.Inventory = []assetrpc.InventoryItemDTO{}
		}
		c.cache.observe("assets", "stale_served", time.Now())
		return &cached, nil
	}
	if err == nil && value != nil {
		c.cache.fill(ctx, "assets", key, generation, value, userID)
	}
	return value, err
}
