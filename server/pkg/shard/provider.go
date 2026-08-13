package shard

import (
	"database/sql"
	"fmt"
)

// DBProvider owns independent pools for every logical shard.  It does not open
// DSNs itself: service composition remains responsible for driver setup, pool
// limits, readiness probes, metrics and Close ordering.
type DBProvider struct {
	router *Router
	pools  map[string]*sql.DB
}

func NewDBProvider(router *Router, pools map[string]*sql.DB) (*DBProvider, error) {
	if router == nil {
		return nil, fmt.Errorf("router is required")
	}
	copyPools := make(map[string]*sql.DB, len(pools))
	for _, shardName := range router.Shards() {
		db := pools[shardName]
		if db == nil {
			return nil, fmt.Errorf("missing database pool for shard %q", shardName)
		}
		copyPools[shardName] = db
	}
	if len(copyPools) != len(pools) {
		return nil, fmt.Errorf("database pools contain an unknown shard")
	}
	return &DBProvider{router: router, pools: copyPools}, nil
}

func (p *DBProvider) DBForUserID(userID int64) (shardName string, db *sql.DB, err error) {
	if p == nil {
		return "", nil, fmt.Errorf("database provider is not configured")
	}
	shardName, err = p.router.ShardForUserID(userID)
	if err != nil {
		return "", nil, err
	}
	return shardName, p.pools[shardName], nil
}

func (p *DBProvider) DBForFarmID(farmID int64) (shardName string, db *sql.DB, err error) {
	return p.DBForUserID(farmID)
}

// Shards returns the configured names only; it never exposes DSNs.
func (p *DBProvider) Shards() []string {
	if p == nil {
		return nil
	}
	return p.router.Shards()
}
