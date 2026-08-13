// Command loadfixture prepares only route11-prefixed load-test accounts.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

const applyConfirmation = "APPLY_ROUTE11_FIXTURE"

type shardResult struct {
	Shard          string `json:"shard"`
	Users          int64  `json:"users"`
	MinCoinBalance int64  `json:"min_coin_balance"`
	WalletRows     int64  `json:"wallet_rows_updated,omitempty"`
}

type output struct {
	Mode     string        `json:"mode"`
	Prefix   string        `json:"device_prefix"`
	Shards   []shardResult `json:"shards"`
	Total    int64         `json:"total_users"`
	Coin     int64         `json:"coin_balance,omitempty"`
	Seed     int64         `json:"seed_quantity,omitempty"`
	Crop     int64         `json:"crop_quantity,omitempty"`
	Recorded string        `json:"recorded_at"`
}

func processEnv(pid int) (string, error) {
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/environ", pid))
	if err != nil {
		return "", err
	}
	for _, item := range strings.Split(string(raw), "\x00") {
		if value, ok := strings.CutPrefix(item, "MYSQL_SHARD_DSNS="); ok {
			return value, nil
		}
	}
	return "", errors.New("MYSQL_SHARD_DSNS not found in process environment")
}

func parseShards(raw string) (map[string]string, error) {
	result := make(map[string]string)
	for _, item := range strings.Split(raw, ",") {
		name, dsn, ok := strings.Cut(strings.TrimSpace(item), "=")
		if !ok || name == "" || dsn == "" {
			return nil, fmt.Errorf("invalid shard entry")
		}
		if _, exists := result[name]; exists {
			return nil, fmt.Errorf("duplicate shard %q", name)
		}
		result[name] = dsn
	}
	if len(result) < 2 {
		return nil, errors.New("at least two shards are required")
	}
	return result, nil
}

func inspect(ctx context.Context, db *sql.DB, prefix string) (int64, int64, error) {
	var users, minBalance int64
	err := db.QueryRowContext(ctx, `
		SELECT COUNT(*), COALESCE(MIN(w.coin_balance), 0)
		FROM auth_identities ai
		JOIN wallets w ON w.user_id = ai.user_id
		WHERE ai.provider = 'guest' AND ai.provider_subject LIKE CONCAT(?, '%')`, prefix).Scan(&users, &minBalance)
	return users, minBalance, err
}

func apply(ctx context.Context, db *sql.DB, prefix string, coin, seed, crop int64) (int64, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	updated, err := tx.ExecContext(ctx, `
		UPDATE wallets w
		JOIN auth_identities ai ON ai.user_id = w.user_id
		SET w.coin_balance = ?, w.row_version = w.row_version + 1, w.updated_at = UTC_TIMESTAMP(3)
		WHERE ai.provider = 'guest' AND ai.provider_subject LIKE CONCAT(?, '%')`, coin, prefix)
	if err != nil {
		return 0, err
	}
	for _, item := range []struct {
		typeName string
		quantity int64
	}{{"SEED", seed}, {"CROP", crop}} {
		_, err = tx.ExecContext(ctx, `
			INSERT INTO inventory_items (user_id, item_type, item_id, quantity, row_version, created_at, updated_at)
			SELECT ai.user_id, ?, 1, ?, 1, UTC_TIMESTAMP(3), UTC_TIMESTAMP(3)
			FROM auth_identities ai
			WHERE ai.provider = 'guest' AND ai.provider_subject LIKE CONCAT(?, '%')
			ON DUPLICATE KEY UPDATE quantity = VALUES(quantity), row_version = row_version + 1, updated_at = UTC_TIMESTAMP(3)`, item.typeName, item.quantity, prefix)
		if err != nil {
			return 0, err
		}
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return updated.RowsAffected()
}

func main() {
	envPID := flag.Int("env-pid", 0, "PID of gamesvr process holding MYSQL_SHARD_DSNS")
	prefix := flag.String("device-prefix", "", "required route11 test device prefix")
	confirm := flag.String("confirm", "", "set APPLY_ROUTE11_FIXTURE to apply changes")
	coin := flag.Int64("coin-balance", 1_000_000_000, "test wallet balance")
	seed := flag.Int64("seed-quantity", 1_000_000_000, "WHEAT seed quantity")
	crop := flag.Int64("crop-quantity", 1_000_000_000, "WHEAT crop quantity")
	flag.Parse()
	if *envPID <= 0 || !strings.HasPrefix(*prefix, "route11-") || *coin < 0 || *seed < 0 || *crop < 0 {
		fmt.Fprintln(os.Stderr, "env-pid, a route11- device prefix and non-negative quantities are required")
		os.Exit(2)
	}
	rawDSNs, err := processEnv(*envPID)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	shards, err := parseShards(rawDSNs)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	names := make([]string, 0, len(shards))
	for name := range shards {
		names = append(names, name)
	}
	sort.Strings(names)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	result := output{Prefix: *prefix, Coin: *coin, Seed: *seed, Crop: *crop, Recorded: time.Now().UTC().Format(time.RFC3339)}
	if *confirm == applyConfirmation {
		result.Mode = "APPLIED"
	} else {
		result.Mode = "DRY_RUN"
	}
	for _, name := range names {
		db, openErr := sql.Open("mysql", shards[name])
		if openErr != nil {
			fmt.Fprintln(os.Stderr, openErr)
			os.Exit(1)
		}
		db.SetMaxOpenConns(2)
		users, minBalance, inspectErr := inspect(ctx, db, *prefix)
		if inspectErr != nil {
			_ = db.Close()
			fmt.Fprintln(os.Stderr, inspectErr)
			os.Exit(1)
		}
		entry := shardResult{Shard: name, Users: users, MinCoinBalance: minBalance}
		if result.Mode == "APPLIED" {
			rows, applyErr := apply(ctx, db, *prefix, *coin, *seed, *crop)
			if applyErr != nil {
				_ = db.Close()
				fmt.Fprintln(os.Stderr, applyErr)
				os.Exit(1)
			}
			entry.WalletRows = rows
		}
		_ = db.Close()
		result.Shards = append(result.Shards, entry)
		result.Total += users
	}
	if result.Total == 0 {
		fmt.Fprintln(os.Stderr, "no matching test accounts")
		os.Exit(1)
	}
	raw, _ := json.MarshalIndent(result, "", "  ")
	fmt.Printf("%s\n", raw)
}
