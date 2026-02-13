package engine_test

import (
	"os"
	"strings"
	"testing"

	"github.com/harshithgowdakt/granuledb/internal/engine"
	"github.com/harshithgowdakt/granuledb/internal/parser"
	"github.com/harshithgowdakt/granuledb/internal/storage"
)

func setupTestDB(t *testing.T) *storage.Database {
	t.Helper()
	dir, err := os.MkdirTemp("", "goosedb-test-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	db, err := storage.NewDatabase(dir)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func execSQL(t *testing.T, db *storage.Database, sql string) *engine.ExecuteResult {
	t.Helper()
	stmt, err := parser.ParseSQL(sql)
	if err != nil {
		t.Fatalf("parse error for %q: %v", sql, err)
	}
	result, err := engine.Execute(stmt, db)
	if err != nil {
		t.Fatalf("execute error for %q: %v", sql, err)
	}
	return result
}

func TestCreateAndInsert(t *testing.T) {
	db := setupTestDB(t)

	// Create table
	result := execSQL(t, db, `CREATE TABLE test (id UInt64, name String) ENGINE = MergeTree() ORDER BY (id)`)
	if result.Message != "OK" {
		t.Fatalf("expected OK, got %s", result.Message)
	}

	// Insert data
	result = execSQL(t, db, `INSERT INTO test VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')`)
	if !strings.Contains(result.Message, "3 rows") {
		t.Fatalf("expected 3 rows inserted, got %s", result.Message)
	}

	// Verify with SELECT *
	result = execSQL(t, db, `SELECT * FROM test`)
	totalRows := 0
	for _, block := range result.Blocks {
		totalRows += block.NumRows()
	}
	if totalRows != 3 {
		t.Fatalf("expected 3 rows, got %d", totalRows)
	}
}

func TestSelectWithWhere(t *testing.T) {
	db := setupTestDB(t)

	execSQL(t, db, `CREATE TABLE test (id UInt64, value Int64) ENGINE = MergeTree() ORDER BY (id)`)
	execSQL(t, db, `INSERT INTO test VALUES (1, 100), (2, 200), (3, 300), (4, 400), (5, 500)`)

	result := execSQL(t, db, `SELECT id, value FROM test WHERE id > 2`)
	totalRows := 0
	for _, block := range result.Blocks {
		totalRows += block.NumRows()
	}
	if totalRows != 3 {
		t.Fatalf("expected 3 rows where id > 2, got %d", totalRows)
	}
}

func TestSelectWithOrderBy(t *testing.T) {
	db := setupTestDB(t)

	execSQL(t, db, `CREATE TABLE test (id UInt64, name String) ENGINE = MergeTree() ORDER BY (id)`)
	execSQL(t, db, `INSERT INTO test VALUES (3, 'charlie'), (1, 'alice'), (2, 'bob')`)

	result := execSQL(t, db, `SELECT id, name FROM test ORDER BY id`)
	if len(result.Blocks) == 0 {
		t.Fatal("expected results")
	}
	block := result.Blocks[0]
	if block.NumRows() != 3 {
		t.Fatalf("expected 3 rows, got %d", block.NumRows())
	}

	// First row should be id=1 (sorted by ORDER BY in INSERT already, then ORDER BY in SELECT)
	col, _ := block.GetColumn("id")
	if col.Value(0).(uint64) != 1 {
		t.Fatalf("expected first id=1, got %v", col.Value(0))
	}
}

func TestSelectWithLimit(t *testing.T) {
	db := setupTestDB(t)

	execSQL(t, db, `CREATE TABLE test (id UInt64) ENGINE = MergeTree() ORDER BY (id)`)
	execSQL(t, db, `INSERT INTO test VALUES (1), (2), (3), (4), (5)`)

	result := execSQL(t, db, `SELECT id FROM test LIMIT 2`)
	totalRows := 0
	for _, block := range result.Blocks {
		totalRows += block.NumRows()
	}
	if totalRows != 2 {
		t.Fatalf("expected 2 rows with LIMIT 2, got %d", totalRows)
	}
}

func TestAggregateCount(t *testing.T) {
	db := setupTestDB(t)

	execSQL(t, db, `CREATE TABLE test (id UInt64, category String) ENGINE = MergeTree() ORDER BY (id)`)
	execSQL(t, db, `INSERT INTO test VALUES (1, 'a'), (2, 'b'), (3, 'a'), (4, 'b'), (5, 'a')`)

	// Count all
	result := execSQL(t, db, `SELECT count(*) FROM test`)
	if len(result.Blocks) == 0 || result.Blocks[0].NumRows() != 1 {
		t.Fatal("expected 1 row for count(*)")
	}
	col := result.Blocks[0].Columns[0]
	if col.Value(0).(uint64) != 5 {
		t.Fatalf("expected count=5, got %v", col.Value(0))
	}
}

func TestAggregateGroupBy(t *testing.T) {
	db := setupTestDB(t)

	execSQL(t, db, `CREATE TABLE test (id UInt64, category String, value Int64) ENGINE = MergeTree() ORDER BY (id)`)
	execSQL(t, db, `INSERT INTO test VALUES (1, 'a', 10), (2, 'b', 20), (3, 'a', 30), (4, 'b', 40), (5, 'a', 50)`)

	result := execSQL(t, db, `SELECT category, count(*), sum(value) FROM test GROUP BY category`)
	if len(result.Blocks) == 0 {
		t.Fatal("expected results")
	}
	block := result.Blocks[0]
	if block.NumRows() != 2 {
		t.Fatalf("expected 2 groups, got %d", block.NumRows())
	}
}

func TestDropTable(t *testing.T) {
	db := setupTestDB(t)

	execSQL(t, db, `CREATE TABLE test (id UInt64) ENGINE = MergeTree() ORDER BY (id)`)
	result := execSQL(t, db, `DROP TABLE test`)
	if result.Message != "OK" {
		t.Fatalf("expected OK, got %s", result.Message)
	}

	_, ok := db.GetTable("test")
	if ok {
		t.Fatal("table should not exist after DROP")
	}
}

func TestShowTables(t *testing.T) {
	db := setupTestDB(t)

	execSQL(t, db, `CREATE TABLE foo (id UInt64) ENGINE = MergeTree() ORDER BY (id)`)
	execSQL(t, db, `CREATE TABLE bar (id UInt64) ENGINE = MergeTree() ORDER BY (id)`)

	result := execSQL(t, db, `SHOW TABLES`)
	if len(result.Blocks) == 0 {
		t.Fatal("expected results")
	}
	totalRows := 0
	for _, block := range result.Blocks {
		totalRows += block.NumRows()
	}
	if totalRows != 2 {
		t.Fatalf("expected 2 tables, got %d", totalRows)
	}
}

func TestMultipleInserts(t *testing.T) {
	db := setupTestDB(t)

	execSQL(t, db, `CREATE TABLE test (id UInt64, name String) ENGINE = MergeTree() ORDER BY (id)`)
	execSQL(t, db, `INSERT INTO test VALUES (1, 'alice')`)
	execSQL(t, db, `INSERT INTO test VALUES (2, 'bob')`)
	execSQL(t, db, `INSERT INTO test VALUES (3, 'charlie')`)

	result := execSQL(t, db, `SELECT * FROM test ORDER BY id`)
	totalRows := 0
	for _, block := range result.Blocks {
		totalRows += block.NumRows()
	}
	if totalRows != 3 {
		t.Fatalf("expected 3 rows from 3 inserts, got %d", totalRows)
	}

	// Verify table has 3 parts
	table, _ := db.GetTable("test")
	parts := table.GetActiveParts()
	if len(parts) != 3 {
		t.Fatalf("expected 3 parts, got %d", len(parts))
	}
}

func TestPartitionedTable(t *testing.T) {
	db := setupTestDB(t)

	execSQL(t, db, `CREATE TABLE events (id UInt64, region String, value Int64) ENGINE = MergeTree() ORDER BY (id) PARTITION BY region`)
	execSQL(t, db, `INSERT INTO events VALUES (1, 'us', 100), (2, 'eu', 200), (3, 'us', 300), (4, 'eu', 400)`)

	// Should create 2 parts (one per partition: us and eu)
	table, _ := db.GetTable("events")
	parts := table.GetActiveParts()
	if len(parts) != 2 {
		t.Fatalf("expected 2 partitions, got %d parts", len(parts))
	}

	// Query all
	result := execSQL(t, db, `SELECT id, region, value FROM events ORDER BY id`)
	totalRows := 0
	for _, block := range result.Blocks {
		totalRows += block.NumRows()
	}
	if totalRows != 4 {
		t.Fatalf("expected 4 rows, got %d", totalRows)
	}
}

func TestAllDataTypes(t *testing.T) {
	db := setupTestDB(t)

	execSQL(t, db, `CREATE TABLE types_test (
		a UInt8, b UInt16, c UInt32, d UInt64,
		e Int8, f Int16, g Int32, h Int64,
		i Float32, j Float64, k String
	) ENGINE = MergeTree() ORDER BY (d)`)

	execSQL(t, db, `INSERT INTO types_test VALUES (1, 2, 3, 4, 5, 6, 7, 8, 1.5, 2.5, 'hello')`)

	result := execSQL(t, db, `SELECT * FROM types_test`)
	if len(result.Blocks) == 0 || result.Blocks[0].NumRows() != 1 {
		t.Fatal("expected 1 row")
	}
}

func TestMaterializedViewInsertTrigger(t *testing.T) {
	db := setupTestDB(t)

	execSQL(t, db, `CREATE TABLE events (user_id UInt64, amount Int64) ENGINE = MergeTree() ORDER BY (user_id)`)
	execSQL(t, db, `CREATE TABLE user_totals (user_id UInt64, total Float64) ENGINE = AggregatingMergeTree() ORDER BY (user_id)`)
	execSQL(t, db, `CREATE MATERIALIZED VIEW mv_user_totals TO user_totals AS SELECT user_id, sum(amount) AS total FROM events GROUP BY user_id`)

	execSQL(t, db, `INSERT INTO events VALUES (1, 10), (1, 5), (2, 7)`)
	execSQL(t, db, `INSERT INTO events VALUES (1, 2), (2, 3)`)

	// Before background merge, AggregatingMergeTree may still have multiple rows per key.
	result := execSQL(t, db, `SELECT user_id, sum(total) AS total FROM user_totals GROUP BY user_id ORDER BY user_id`)
	if len(result.Blocks) == 0 || result.Blocks[0].NumRows() != 2 {
		t.Fatalf("expected 2 aggregated rows, got %+v", result)
	}
	block := result.Blocks[0]
	totalCol, _ := block.GetColumn("total")
	if got := totalCol.Value(0).(float64); got != 17 {
		t.Fatalf("expected user 1 total 17, got %v", got)
	}
	if got := totalCol.Value(1).(float64); got != 10 {
		t.Fatalf("expected user 2 total 10, got %v", got)
	}
}

func TestNewAggregates(t *testing.T) {
	db := setupTestDB(t)
	execSQL(t, db, `CREATE TABLE t (k UInt64, v Int64, s String) ENGINE = MergeTree() ORDER BY (k)`)
	execSQL(t, db, `INSERT INTO t VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 30, 'a'), (4, 40, 'c'), (5, 50, 'a')`)

	res := execSQL(t, db, `SELECT uniq(s) AS u FROM t`)
	if len(res.Blocks) == 0 || res.Blocks[0].NumRows() != 1 {
		t.Fatal("expected single row for uniq")
	}
	if got := res.Blocks[0].Columns[0].Value(0).(uint64); got != 3 {
		t.Fatalf("expected uniq=3, got %d", got)
	}

	res = execSQL(t, db, `SELECT quantiles(0.5, v) AS q FROM t`)
	if got := res.Blocks[0].Columns[0].Value(0).(float64); got != 30 {
		t.Fatalf("expected quantile 30, got %v", got)
	}

	res = execSQL(t, db, `SELECT topK(2, s) AS t FROM t`)
	if got := res.Blocks[0].Columns[0].Value(0).(string); got != "a,b" {
		t.Fatalf("expected topK \"a,b\", got %q", got)
	}
}

func TestSumStateSumMergeFlow(t *testing.T) {
	db := setupTestDB(t)

	execSQL(t, db, `CREATE TABLE src (k UInt64, v Int64) ENGINE = MergeTree() ORDER BY (k)`)
	execSQL(t, db, `CREATE TABLE dst (k UInt64, s AggregateState) ENGINE = AggregatingMergeTree() ORDER BY (k)`)
	execSQL(t, db, `CREATE MATERIALIZED VIEW mv_sum_state TO dst AS SELECT k, sumState(v) AS s FROM src GROUP BY k`)

	execSQL(t, db, `INSERT INTO src VALUES (1, 10), (1, 5), (2, 7)`)
	execSQL(t, db, `INSERT INTO src VALUES (1, 2), (2, 3)`)

	res := execSQL(t, db, `SELECT k, sumMerge(s) AS total FROM dst GROUP BY k ORDER BY k`)
	if len(res.Blocks) == 0 || res.Blocks[0].NumRows() != 2 {
		t.Fatalf("expected 2 rows, got %+v", res)
	}

	block := res.Blocks[0]
	totalCol, _ := block.GetColumn("total")
	if got := totalCol.Value(0).(float64); got != 17 {
		t.Fatalf("expected key=1 total 17, got %v", got)
	}
	if got := totalCol.Value(1).(float64); got != 10 {
		t.Fatalf("expected key=2 total 10, got %v", got)
	}
}

func TestUniqStateUniqMergeFlow(t *testing.T) {
	db := setupTestDB(t)

	execSQL(t, db, `CREATE TABLE src_u (k UInt64, s String) ENGINE = MergeTree() ORDER BY (k)`)
	execSQL(t, db, `CREATE TABLE dst_u (k UInt64, st AggregateState) ENGINE = AggregatingMergeTree() ORDER BY (k)`)
	execSQL(t, db, `CREATE MATERIALIZED VIEW mv_uniq_state TO dst_u AS SELECT k, uniqState(s) AS st FROM src_u GROUP BY k`)

	execSQL(t, db, `INSERT INTO src_u VALUES (1, 'a'), (1, 'b'), (1, 'a'), (2, 'x')`)
	execSQL(t, db, `INSERT INTO src_u VALUES (1, 'c'), (2, 'x'), (2, 'y')`)

	res := execSQL(t, db, `SELECT k, uniqMerge(st) AS u FROM dst_u GROUP BY k ORDER BY k`)
	if len(res.Blocks) == 0 || res.Blocks[0].NumRows() != 2 {
		t.Fatalf("expected 2 rows, got %+v", res)
	}
	block := res.Blocks[0]
	uCol, _ := block.GetColumn("u")
	if got := uCol.Value(0).(uint64); got != 3 {
		t.Fatalf("expected key=1 uniq 3, got %d", got)
	}
	if got := uCol.Value(1).(uint64); got != 2 {
		t.Fatalf("expected key=2 uniq 2, got %d", got)
	}
}

func TestAggregateFunctionTypeSyntax(t *testing.T) {
	db := setupTestDB(t)

	execSQL(t, db, `CREATE TABLE src_af (k UInt64, v Int64) ENGINE = MergeTree() ORDER BY (k)`)
	execSQL(t, db, `CREATE TABLE dst_af (k UInt64, s AggregateFunction(sumState, Float64)) ENGINE = AggregatingMergeTree() ORDER BY (k)`)
	execSQL(t, db, `CREATE MATERIALIZED VIEW mv_af TO dst_af AS SELECT k, sumState(v) AS s FROM src_af GROUP BY k`)
	execSQL(t, db, `INSERT INTO src_af VALUES (1, 3), (1, 4), (2, 5)`)

	res := execSQL(t, db, `SELECT k, sumMerge(s) AS total FROM dst_af GROUP BY k ORDER BY k`)
	if len(res.Blocks) == 0 || res.Blocks[0].NumRows() != 2 {
		t.Fatalf("expected 2 rows, got %+v", res)
	}
	block := res.Blocks[0]
	totalCol, _ := block.GetColumn("total")
	if got := totalCol.Value(0).(float64); got != 7 {
		t.Fatalf("expected key=1 total 7, got %v", got)
	}
	if got := totalCol.Value(1).(float64); got != 5 {
		t.Fatalf("expected key=2 total 5, got %v", got)
	}
}

func TestPartitionByExpression(t *testing.T) {
	db := setupTestDB(t)

	execSQL(t, db, `CREATE TABLE events (id UInt64, ts DateTime, value Int64) ENGINE = MergeTree() ORDER BY (id) PARTITION BY toYYYYMM(ts)`)

	// Jan 2024: 1704067200 (2024-01-01 00:00:00 UTC), 1704153600 (2024-01-02)
	// Feb 2024: 1706745600 (2024-02-01 00:00:00 UTC), 1706832000 (2024-02-02)
	execSQL(t, db, `INSERT INTO events VALUES (1, 1704067200, 100), (2, 1704153600, 200), (3, 1706745600, 300), (4, 1706832000, 400)`)

	// Should create 2 partitions: 202401 and 202402
	table, _ := db.GetTable("events")
	parts := table.GetActiveParts()
	if len(parts) != 2 {
		t.Fatalf("expected 2 partitions, got %d", len(parts))
	}

	// Verify partition IDs
	pids := map[string]bool{}
	for _, p := range parts {
		pids[p.Info.PartitionID] = true
	}
	if !pids["202401"] || !pids["202402"] {
		t.Fatalf("expected partition IDs 202401 and 202402, got %v", pids)
	}

	// Verify all data is queryable
	result := execSQL(t, db, `SELECT id, value FROM events ORDER BY id`)
	totalRows := 0
	for _, block := range result.Blocks {
		totalRows += block.NumRows()
	}
	if totalRows != 4 {
		t.Fatalf("expected 4 rows, got %d", totalRows)
	}
}

func TestPartitionByIntDiv(t *testing.T) {
	db := setupTestDB(t)

	execSQL(t, db, `CREATE TABLE data (id UInt64, value Int64) ENGINE = MergeTree() ORDER BY (id) PARTITION BY intDiv(id, 1000)`)

	execSQL(t, db, `INSERT INTO data VALUES (1, 10), (500, 20), (999, 30), (1000, 40), (1500, 50), (2000, 60)`)

	// Should create 3 partitions: 0, 1, 2
	table, _ := db.GetTable("data")
	parts := table.GetActiveParts()
	if len(parts) != 3 {
		t.Fatalf("expected 3 partitions, got %d", len(parts))
	}

	// Verify all data queryable
	result := execSQL(t, db, `SELECT id, value FROM data ORDER BY id`)
	totalRows := 0
	for _, block := range result.Blocks {
		totalRows += block.NumRows()
	}
	if totalRows != 6 {
		t.Fatalf("expected 6 rows, got %d", totalRows)
	}
}

func TestPartitionByColumnBackwardCompat(t *testing.T) {
	db := setupTestDB(t)

	// Old-style PARTITION BY column_name should still work
	execSQL(t, db, `CREATE TABLE events2 (id UInt64, region String, value Int64) ENGINE = MergeTree() ORDER BY (id) PARTITION BY region`)

	execSQL(t, db, `INSERT INTO events2 VALUES (1, 'us', 100), (2, 'eu', 200), (3, 'us', 300)`)

	table, _ := db.GetTable("events2")
	parts := table.GetActiveParts()
	if len(parts) != 2 {
		t.Fatalf("expected 2 partitions, got %d", len(parts))
	}

	result := execSQL(t, db, `SELECT id FROM events2 ORDER BY id`)
	totalRows := 0
	for _, block := range result.Blocks {
		totalRows += block.NumRows()
	}
	if totalRows != 3 {
		t.Fatalf("expected 3 rows, got %d", totalRows)
	}
}

func TestLowCardinalityCreateInsertSelect(t *testing.T) {
	db := setupTestDB(t)

	execSQL(t, db, `CREATE TABLE lc_test (id UInt64, status LowCardinality(String), value Int64) ENGINE = MergeTree() ORDER BY (id)`)

	execSQL(t, db, `INSERT INTO lc_test VALUES (1, 'active', 100), (2, 'inactive', 200), (3, 'active', 300), (4, 'pending', 400), (5, 'active', 500)`)

	result := execSQL(t, db, `SELECT id, status, value FROM lc_test ORDER BY id`)
	totalRows := 0
	for _, block := range result.Blocks {
		totalRows += block.NumRows()
	}
	if totalRows != 5 {
		t.Fatalf("expected 5 rows, got %d", totalRows)
	}

	// Verify first row values
	if len(result.Blocks) > 0 {
		block := result.Blocks[0]
		idCol, _ := block.GetColumn("id")
		statusCol, _ := block.GetColumn("status")
		if idCol.Value(0).(uint64) != 1 {
			t.Fatalf("expected id=1, got %v", idCol.Value(0))
		}
		if statusCol.Value(0).(string) != "active" {
			t.Fatalf("expected status='active', got %v", statusCol.Value(0))
		}
	}
}

func TestLowCardinalityWithGroupBy(t *testing.T) {
	db := setupTestDB(t)

	execSQL(t, db, `CREATE TABLE lc_group (id UInt64, category LowCardinality(String), amount Int64) ENGINE = MergeTree() ORDER BY (id)`)
	execSQL(t, db, `INSERT INTO lc_group VALUES (1, 'a', 10), (2, 'b', 20), (3, 'a', 30), (4, 'b', 40), (5, 'a', 50)`)

	result := execSQL(t, db, `SELECT category, count(*), sum(amount) FROM lc_group GROUP BY category`)
	if len(result.Blocks) == 0 {
		t.Fatal("expected results")
	}
	block := result.Blocks[0]
	if block.NumRows() != 2 {
		t.Fatalf("expected 2 groups, got %d", block.NumRows())
	}
}

func TestLowCardinalityWithWhere(t *testing.T) {
	db := setupTestDB(t)

	execSQL(t, db, `CREATE TABLE lc_where (id UInt64, status LowCardinality(String)) ENGINE = MergeTree() ORDER BY (id)`)
	execSQL(t, db, `INSERT INTO lc_where VALUES (1, 'active'), (2, 'inactive'), (3, 'active'), (4, 'deleted'), (5, 'active')`)

	result := execSQL(t, db, `SELECT id FROM lc_where WHERE id > 2`)
	totalRows := 0
	for _, block := range result.Blocks {
		totalRows += block.NumRows()
	}
	if totalRows != 3 {
		t.Fatalf("expected 3 rows where id > 2, got %d", totalRows)
	}
}

func TestLowCardinalityMultipleInserts(t *testing.T) {
	db := setupTestDB(t)

	execSQL(t, db, `CREATE TABLE lc_multi (id UInt64, tag LowCardinality(String)) ENGINE = MergeTree() ORDER BY (id)`)
	execSQL(t, db, `INSERT INTO lc_multi VALUES (1, 'x')`)
	execSQL(t, db, `INSERT INTO lc_multi VALUES (2, 'y')`)
	execSQL(t, db, `INSERT INTO lc_multi VALUES (3, 'x')`)

	result := execSQL(t, db, `SELECT id, tag FROM lc_multi ORDER BY id`)
	totalRows := 0
	for _, block := range result.Blocks {
		totalRows += block.NumRows()
	}
	if totalRows != 3 {
		t.Fatalf("expected 3 rows, got %d", totalRows)
	}

	// Verify table has 3 parts
	table, _ := db.GetTable("lc_multi")
	parts := table.GetActiveParts()
	if len(parts) != 3 {
		t.Fatalf("expected 3 parts, got %d", len(parts))
	}
}

func TestLowCardinalityMixedColumns(t *testing.T) {
	db := setupTestDB(t)

	execSQL(t, db, `CREATE TABLE lc_mixed (id UInt64, name String, status LowCardinality(String), value Int64) ENGINE = MergeTree() ORDER BY (id)`)
	execSQL(t, db, `INSERT INTO lc_mixed VALUES (1, 'alice', 'active', 100), (2, 'bob', 'inactive', 200), (3, 'charlie', 'active', 300)`)

	result := execSQL(t, db, `SELECT * FROM lc_mixed ORDER BY id`)
	totalRows := 0
	for _, block := range result.Blocks {
		totalRows += block.NumRows()
	}
	if totalRows != 3 {
		t.Fatalf("expected 3 rows, got %d", totalRows)
	}

	// Verify mixed column types
	if len(result.Blocks) > 0 {
		block := result.Blocks[0]
		nameCol, _ := block.GetColumn("name")
		statusCol, _ := block.GetColumn("status")
		if nameCol.Value(0).(string) != "alice" {
			t.Fatalf("expected name='alice', got %v", nameCol.Value(0))
		}
		if statusCol.Value(0).(string) != "active" {
			t.Fatalf("expected status='active', got %v", statusCol.Value(0))
		}
	}
}

func TestExprToSQLRoundTrip(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"region", "region"},
		{"toyyyymm(date)", "toyyyymm(date)"},
		{"intdiv(id, 1000)", "intdiv(id, 1000)"},
	}

	for _, tt := range tests {
		expr, err := parser.ParseExpression(tt.input)
		if err != nil {
			t.Fatalf("ParseExpression(%q): %v", tt.input, err)
		}
		got := parser.ExprToSQL(expr)
		if got != tt.want {
			t.Fatalf("ExprToSQL round-trip: input=%q, got=%q, want=%q", tt.input, got, tt.want)
		}
	}
}
