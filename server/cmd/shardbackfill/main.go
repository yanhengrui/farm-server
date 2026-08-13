// shardbackfill copies a legacy single-MySQL dataset into two empty physical
// shards without modifying the source. It is intentionally an operator tool,
// not an application startup path: it refuses non-empty targets and produces
// a machine-readable audit so a cutover cannot silently route old users to an
// empty shard.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type tableSpec struct {
	name         string
	filterColumn string
	query        func(parity int) string
}

type auditRow struct {
	Shard    string `json:"shard"`
	Table    string `json:"table"`
	Expected int64  `json:"expected_rows"`
	Actual   int64  `json:"actual_rows"`
}

type report struct {
	StartedAt  time.Time  `json:"started_at"`
	FinishedAt time.Time  `json:"finished_at"`
	DryRun     bool       `json:"dry_run"`
	Rows       []auditRow `json:"rows"`
	Result     string     `json:"result"`
}

var allTargetTables = []string{
	"accounts", "auth_identities", "catalog_unlocks", "cmd_receipts",
	"consumed_events", "consumer_failed_events", "economy_transactions",
	"farm_snapshots", "friendship_edges", "friendships", "inventory_items",
	"mail_attachments", "mails", "outbox_events", "player_pets",
	"player_tasks", "sessions", "wallets",
}

var userTables = []tableSpec{
	{name: "accounts", filterColumn: "user_id"},
	{name: "auth_identities", filterColumn: "user_id"},
	{name: "wallets", filterColumn: "user_id"},
	{name: "farm_snapshots", filterColumn: "owner_user_id"},
	{name: "catalog_unlocks", filterColumn: "user_id"},
	{name: "inventory_items", filterColumn: "user_id"},
	{name: "player_pets", filterColumn: "user_id"},
	{name: "player_tasks", filterColumn: "user_id"},
	{name: "mails", filterColumn: "user_id"},
	{name: "sessions", filterColumn: "user_id"},
	{name: "cmd_receipts", filterColumn: "user_id"},
	{name: "economy_transactions", filterColumn: "user_id"},
	{
		name: "mail_attachments",
		query: func(parity int) string {
			return fmt.Sprintf("SELECT ma.* FROM `mail_attachments` ma JOIN `mails` m ON m.mail_id=ma.mail_id WHERE MOD(m.user_id,2)=%d", parity)
		},
	},
}

func main() {
	var sourceDSN, shard0DSN, shard1DSN, reportPath string
	var batchSize int
	var dryRun bool
	flag.StringVar(&sourceDSN, "source-dsn", "", "legacy single-MySQL DSN (required)")
	flag.StringVar(&shard0DSN, "shard-0-dsn", "", "empty shard-0 MySQL DSN (required)")
	flag.StringVar(&shard1DSN, "shard-1-dsn", "", "empty shard-1 MySQL DSN (required)")
	flag.StringVar(&reportPath, "report", "", "JSON audit output path (required)")
	flag.IntVar(&batchSize, "batch-size", 1000, "rows per target transaction")
	flag.BoolVar(&dryRun, "dry-run", false, "validate schemas and calculate source distribution without writing")
	flag.Parse()
	// A systemd runner supplies the DSNs through a root-only EnvironmentFile.
	// Do not require secrets on argv: they would otherwise be visible through
	// process listings for the duration of a large backfill.
	if sourceDSN == "" {
		sourceDSN = os.Getenv("SHARDBACKFILL_SOURCE_DSN")
	}
	if shard0DSN == "" {
		shard0DSN = os.Getenv("SHARDBACKFILL_SHARD0_DSN")
	}
	if shard1DSN == "" {
		shard1DSN = os.Getenv("SHARDBACKFILL_SHARD1_DSN")
	}
	if sourceDSN == "" || shard0DSN == "" || shard1DSN == "" || reportPath == "" || batchSize < 1 {
		log.Fatal("source-dsn, shard-0-dsn, shard-1-dsn, report and a positive batch-size are required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Hour)
	defer cancel()
	r := report{StartedAt: time.Now().UTC(), DryRun: dryRun}
	if err := run(ctx, sourceDSN, shard0DSN, shard1DSN, batchSize, dryRun, &r); err != nil {
		r.FinishedAt, r.Result = time.Now().UTC(), "FAIL"
		writeReport(reportPath, r)
		log.Fatal(err)
	}
	r.FinishedAt, r.Result = time.Now().UTC(), "PASS"
	writeReport(reportPath, r)
	log.Printf("PASS: audit=%s", reportPath)
}

func run(ctx context.Context, sourceDSN, shard0DSN, shard1DSN string, batchSize int, dryRun bool, r *report) error {
	source, err := open(ctx, sourceDSN)
	if err != nil {
		return fmt.Errorf("open source: %w", err)
	}
	defer source.Close()
	shards := []struct {
		name   string
		parity int
		dsn    string
		db     *sql.DB
	}{
		{name: "shard-0", parity: 0, dsn: shard0DSN},
		{name: "shard-1", parity: 1, dsn: shard1DSN},
	}

	for i := range shards {
		shards[i].db, err = open(ctx, shards[i].dsn)
		if err != nil {
			return fmt.Errorf("open %s: %w", shards[i].name, err)
		}
		defer shards[i].db.Close()
		if err := assertTargetSchema(ctx, source, shards[i].db); err != nil {
			return fmt.Errorf("%s schema: %w", shards[i].name, err)
		}
		if err := assertTargetEmpty(ctx, shards[i].db); err != nil {
			return fmt.Errorf("%s: %w", shards[i].name, err)
		}
	}
	if err := assertUnsupportedSourceTablesEmpty(ctx, source); err != nil {
		return err
	}

	for _, spec := range userTables {
		for i := range shards {
			query := selectQuery(spec, shards[i].parity)
			expected, err := countQuery(ctx, source, "SELECT COUNT(*) FROM ("+query+") AS source_rows")
			if err != nil {
				return fmt.Errorf("count source %s/%s: %w", shards[i].name, spec.name, err)
			}
			if !dryRun && expected > 0 {
				copied, err := copyRows(ctx, source, shards[i].db, spec.name, query, batchSize)
				if err != nil {
					return fmt.Errorf("copy %s/%s: %w", shards[i].name, spec.name, err)
				}
				if copied != expected {
					return fmt.Errorf("copy %s/%s row count %d, expected %d", shards[i].name, spec.name, copied, expected)
				}
			}
			actual := int64(0)
			if !dryRun {
				actual, err = countQuery(ctx, shards[i].db, "SELECT COUNT(*) FROM "+ident(spec.name))
				if err != nil {
					return fmt.Errorf("count target %s/%s: %w", shards[i].name, spec.name, err)
				}
				if actual != expected {
					return fmt.Errorf("audit %s/%s got %d rows, expected %d", shards[i].name, spec.name, actual, expected)
				}
			}
			if dryRun {
				r.Rows = append(r.Rows, auditRow{Shard: shards[i].name, Table: spec.name, Expected: expected, Actual: actual})
			}
			log.Printf("%s %s expected=%d actual=%d", shards[i].name, spec.name, expected, actual)
		}
	}
	if err := copyFriendships(ctx, source, shards[0].db, shards[1].db, dryRun, r); err != nil {
		return err
	}
	if !dryRun {
		// Count again after every table has been copied. This does not pretend to
		// solve a live-write cutover, but it makes any row-count drift during the
		// snapshot visible and prevents a seemingly successful stale seed.
		for _, spec := range userTables {
			for i := range shards {
				query := selectQuery(spec, shards[i].parity)
				expected, err := countQuery(ctx, source, "SELECT COUNT(*) FROM ("+query+") AS source_rows")
				if err != nil {
					return fmt.Errorf("final source count %s/%s: %w", shards[i].name, spec.name, err)
				}
				actual, err := countQuery(ctx, shards[i].db, "SELECT COUNT(*) FROM "+ident(spec.name))
				if err != nil {
					return fmt.Errorf("final target count %s/%s: %w", shards[i].name, spec.name, err)
				}
				if actual != expected {
					return fmt.Errorf("final audit %s/%s got %d rows, expected %d", shards[i].name, spec.name, actual, expected)
				}
				r.Rows = append(r.Rows, auditRow{Shard: shards[i].name, Table: spec.name, Expected: expected, Actual: actual})
			}
		}
		for i := range shards {
			if err := assertNoUnexpectedRows(ctx, shards[i].db); err != nil {
				return fmt.Errorf("%s final empty-table audit: %w", shards[i].name, err)
			}
		}
	}
	return nil
}

func open(ctx context.Context, dsn string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(2)
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func assertTargetSchema(ctx context.Context, source, target *sql.DB) error {
	for _, spec := range userTables {
		sourceCols, err := tableColumns(ctx, source, spec.name)
		if err != nil {
			return err
		}
		targetCols, err := tableColumns(ctx, target, spec.name)
		if err != nil {
			return err
		}
		if !sameStrings(sourceCols, targetCols) {
			return fmt.Errorf("columns differ for %s", spec.name)
		}
	}
	return nil
}

func assertTargetEmpty(ctx context.Context, db *sql.DB) error {
	for _, table := range allTargetTables {
		n, err := countQuery(ctx, db, "SELECT COUNT(*) FROM "+ident(table))
		if err != nil {
			return err
		}
		if n != 0 {
			return fmt.Errorf("target table %s is not empty (%d rows); refusing to overwrite it", table, n)
		}
	}
	return nil
}

func assertUnsupportedSourceTablesEmpty(ctx context.Context, source *sql.DB) error {
	for _, table := range []string{"outbox_events", "consumed_events", "consumer_failed_events"} {
		n, err := countQuery(ctx, source, "SELECT COUNT(*) FROM "+ident(table))
		if err != nil {
			return err
		}
		if n != 0 {
			return fmt.Errorf("legacy %s contains %d rows; route and replay semantics must be migrated explicitly", table, n)
		}
	}
	return nil
}

func assertNoUnexpectedRows(ctx context.Context, target *sql.DB) error {
	for _, table := range []string{"outbox_events", "consumed_events", "consumer_failed_events"} {
		n, err := countQuery(ctx, target, "SELECT COUNT(*) FROM "+ident(table))
		if err != nil {
			return err
		}
		if n != 0 {
			return fmt.Errorf("target %s unexpectedly contains %d rows", table, n)
		}
	}
	return nil
}

func selectQuery(spec tableSpec, parity int) string {
	if spec.query != nil {
		return spec.query(parity)
	}
	return fmt.Sprintf("SELECT * FROM %s WHERE MOD(%s,2)=%d", ident(spec.name), ident(spec.filterColumn), parity)
}

func copyRows(ctx context.Context, source, target *sql.DB, table, query string, batchSize int) (int64, error) {
	rows, err := source.QueryContext(ctx, query)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return 0, err
	}
	var copied int64
	batch := make([]any, 0, batchSize*len(columns))
	batchRows := 0
	flush := func() error {
		if batchRows == 0 {
			return nil
		}
		// One multi-value statement per bounded batch removes millions of
		// client/server round trips while retaining a small, committed recovery
		// boundary. Batch size is operator controlled for max_packet safety.
		rowPlaceholders := "(" + placeholders(len(columns)) + ")"
		insert := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", ident(table), identList(columns), strings.TrimRight(strings.Repeat(rowPlaceholders+",", batchRows), ","))
		tx, err := target.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, insert, batch...); err != nil {
			_ = tx.Rollback()
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		batch = batch[:0]
		batchRows = 0
		return nil
	}
	for rows.Next() {
		values := make([]any, len(columns))
		dest := make([]any, len(columns))
		for i := range values {
			dest[i] = &values[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return copied, err
		}
		batch = append(batch, values...)
		batchRows++
		copied++
		if batchRows == batchSize {
			if err := flush(); err != nil {
				return copied, err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return copied, err
	}
	return copied, flush()
}

func copyFriendships(ctx context.Context, source, shard0, shard1 *sql.DB, dryRun bool, r *report) error {
	rows, err := source.QueryContext(ctx, "SELECT friendship_id, user_id_a, user_id_b, created_at FROM friendships ORDER BY friendship_id")
	if err != nil {
		return fmt.Errorf("read friendships: %w", err)
	}
	defer rows.Close()
	counts := map[string]int64{"shard-0/friendships": 0, "shard-1/friendships": 0, "shard-0/friendship_edges": 0, "shard-1/friendship_edges": 0}
	for rows.Next() {
		var id, a, b uint64
		var created time.Time
		if err := rows.Scan(&id, &a, &b, &created); err != nil {
			return err
		}
		pa, pb := int(a%2), int(b%2)
		if pa == pb {
			name, target := "shard-0", shard0
			if pa == 1 {
				name, target = "shard-1", shard1
			}
			counts[name+"/friendships"]++
			if !dryRun {
				if _, err := target.ExecContext(ctx, "INSERT INTO friendships (friendship_id,user_id_a,user_id_b,created_at) VALUES (?,?,?,?)", id, a, b, created); err != nil {
					return fmt.Errorf("copy same-shard friendship %d: %w", id, err)
				}
			}
			continue
		}
		for _, edge := range []struct {
			user, friend uint64
			target       *sql.DB
			name         string
		}{
			{user: a, friend: b, target: map[int]*sql.DB{0: shard0, 1: shard1}[pa], name: map[int]string{0: "shard-0", 1: "shard-1"}[pa]},
			{user: b, friend: a, target: map[int]*sql.DB{0: shard0, 1: shard1}[pb], name: map[int]string{0: "shard-0", 1: "shard-1"}[pb]},
		} {
			counts[edge.name+"/friendship_edges"]++
			if !dryRun {
				eventID := fmt.Sprintf("legacy-friendship-%d-%d", id, edge.user)
				if _, err := edge.target.ExecContext(ctx, "INSERT INTO friendship_edges (user_id,friend_user_id,state,source_event_id,created_at,updated_at) VALUES (?,?,'ACTIVE',?,?,?)", edge.user, edge.friend, eventID, created, created); err != nil {
					return fmt.Errorf("copy cross-shard friendship edge %d: %w", id, err)
				}
			}
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, name := range []string{"shard-0/friendships", "shard-1/friendships", "shard-0/friendship_edges", "shard-1/friendship_edges"} {
		parts := strings.Split(name, "/")
		actual := int64(0)
		if !dryRun {
			target := shard0
			if parts[0] == "shard-1" {
				target = shard1
			}
			actual, err = countQuery(ctx, target, "SELECT COUNT(*) FROM "+ident(parts[1]))
			if err != nil {
				return err
			}
			if actual != counts[name] {
				return fmt.Errorf("audit %s got %d rows, expected %d", name, actual, counts[name])
			}
		}
		r.Rows = append(r.Rows, auditRow{Shard: parts[0], Table: parts[1], Expected: counts[name], Actual: actual})
	}
	return nil
}

func tableColumns(ctx context.Context, db *sql.DB, table string) ([]string, error) {
	rows, err := db.QueryContext(ctx, "SELECT column_name FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name=? ORDER BY ordinal_position", table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var columns []string
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, err
		}
		columns = append(columns, column)
	}
	return columns, rows.Err()
}

func countQuery(ctx context.Context, db *sql.DB, query string) (int64, error) {
	var n int64
	err := db.QueryRowContext(ctx, query).Scan(&n)
	return n, err
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func ident(value string) string { return "`" + strings.ReplaceAll(value, "`", "``") + "`" }

func identList(values []string) string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		result = append(result, ident(value))
	}
	return strings.Join(result, ",")
}

func placeholders(n int) string { return strings.TrimRight(strings.Repeat("?,", n), ",") }

func writeReport(path string, value report) {
	value.Rows = append([]auditRow(nil), value.Rows...)
	sort.Slice(value.Rows, func(i, j int) bool {
		if value.Rows[i].Shard == value.Rows[j].Shard {
			return value.Rows[i].Table < value.Rows[j].Table
		}
		return value.Rows[i].Shard < value.Rows[j].Shard
	})
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		log.Printf("marshal report: %v", err)
		return
	}
	if err := os.WriteFile(path, append(raw, '\n'), 0o600); err != nil {
		log.Printf("write report %s: %v", path, err)
	}
}
