package processor_test

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/harshithgowdakt/granuledb/internal/column"
	"github.com/harshithgowdakt/granuledb/internal/engine"
	"github.com/harshithgowdakt/granuledb/internal/parser"
	"github.com/harshithgowdakt/granuledb/internal/processor"
	"github.com/harshithgowdakt/granuledb/internal/storage"
	"github.com/harshithgowdakt/granuledb/internal/types"
)

func setupTestDB(t *testing.T) *storage.Database {
	t.Helper()
	dir, err := os.MkdirTemp("", "goosedb-proc-test-*")
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

// execDDL runs DDL/DML via engine.Execute (pull model, no SELECT).
func execDDL(t *testing.T, db *storage.Database, sql string) *engine.ExecuteResult {
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

// execSelectPush runs SELECT via the push-based processor pipeline.
func execSelectPush(t *testing.T, db *storage.Database, sql string) []*column.Block {
	t.Helper()
	stmt, err := parser.ParseSQL(sql)
	if err != nil {
		t.Fatalf("parse error for %q: %v", sql, err)
	}
	selStmt, ok := stmt.(*parser.SelectStmt)
	if !ok {
		t.Fatalf("expected SelectStmt, got %T", stmt)
	}

	result, err := processor.BuildPipeline(selStmt, db)
	if err != nil {
		t.Fatalf("BuildPipeline error for %q: %v", sql, err)
	}

	exec := processor.NewPipelineExecutor(result.Graph, 4)
	if err := exec.Execute(); err != nil {
		t.Fatalf("Execute error for %q: %v", sql, err)
	}

	return result.Output.ResultBlocks()
}

func countRows(blocks []*column.Block) int {
	total := 0
	for _, b := range blocks {
		total += b.NumRows()
	}
	return total
}

func TestProcessorSelectAll(t *testing.T) {
	db := setupTestDB(t)
	execDDL(t, db, `CREATE TABLE test (id UInt64, name String) ENGINE = MergeTree() ORDER BY (id)`)
	execDDL(t, db, `INSERT INTO test VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')`)

	blocks := execSelectPush(t, db, `SELECT * FROM test`)
	if countRows(blocks) != 3 {
		t.Fatalf("expected 3 rows, got %d", countRows(blocks))
	}
}

func TestProcessorSelectWithWhere(t *testing.T) {
	db := setupTestDB(t)
	execDDL(t, db, `CREATE TABLE test (id UInt64, value Int64) ENGINE = MergeTree() ORDER BY (id)`)
	execDDL(t, db, `INSERT INTO test VALUES (1, 100), (2, 200), (3, 300), (4, 400), (5, 500)`)

	blocks := execSelectPush(t, db, `SELECT id, value FROM test WHERE id > 2`)
	if countRows(blocks) != 3 {
		t.Fatalf("expected 3 rows where id > 2, got %d", countRows(blocks))
	}
}

func TestProcessorSelectWithOrderBy(t *testing.T) {
	db := setupTestDB(t)
	execDDL(t, db, `CREATE TABLE test (id UInt64, name String) ENGINE = MergeTree() ORDER BY (id)`)
	execDDL(t, db, `INSERT INTO test VALUES (3, 'charlie'), (1, 'alice'), (2, 'bob')`)

	blocks := execSelectPush(t, db, `SELECT id, name FROM test ORDER BY id`)
	if len(blocks) == 0 {
		t.Fatal("expected results")
	}
	block := blocks[0]
	if block.NumRows() != 3 {
		t.Fatalf("expected 3 rows, got %d", block.NumRows())
	}
	col, _ := block.GetColumn("id")
	if col.Value(0).(uint64) != 1 {
		t.Fatalf("expected first id=1, got %v", col.Value(0))
	}
	if col.Value(2).(uint64) != 3 {
		t.Fatalf("expected last id=3, got %v", col.Value(2))
	}
}

func TestProcessorSelectWithLimit(t *testing.T) {
	db := setupTestDB(t)
	execDDL(t, db, `CREATE TABLE test (id UInt64) ENGINE = MergeTree() ORDER BY (id)`)
	execDDL(t, db, `INSERT INTO test VALUES (1), (2), (3), (4), (5)`)

	blocks := execSelectPush(t, db, `SELECT id FROM test LIMIT 2`)
	if countRows(blocks) != 2 {
		t.Fatalf("expected 2 rows with LIMIT 2, got %d", countRows(blocks))
	}
}

func TestProcessorAggregateCount(t *testing.T) {
	db := setupTestDB(t)
	execDDL(t, db, `CREATE TABLE test (id UInt64, category String) ENGINE = MergeTree() ORDER BY (id)`)
	execDDL(t, db, `INSERT INTO test VALUES (1, 'a'), (2, 'b'), (3, 'a'), (4, 'b'), (5, 'a')`)

	blocks := execSelectPush(t, db, `SELECT count(*) FROM test`)
	if len(blocks) == 0 || blocks[0].NumRows() != 1 {
		t.Fatal("expected 1 row for count(*)")
	}
	col := blocks[0].Columns[0]
	if col.Value(0).(uint64) != 5 {
		t.Fatalf("expected count=5, got %v", col.Value(0))
	}
}

func TestProcessorAggregateGroupBy(t *testing.T) {
	db := setupTestDB(t)
	execDDL(t, db, `CREATE TABLE test (id UInt64, category String, value Int64) ENGINE = MergeTree() ORDER BY (id)`)
	execDDL(t, db, `INSERT INTO test VALUES (1, 'a', 10), (2, 'b', 20), (3, 'a', 30), (4, 'b', 40), (5, 'a', 50)`)

	blocks := execSelectPush(t, db, `SELECT category, count(*), sum(value) FROM test GROUP BY category`)
	if len(blocks) == 0 {
		t.Fatal("expected results")
	}
	if blocks[0].NumRows() != 2 {
		t.Fatalf("expected 2 groups, got %d", blocks[0].NumRows())
	}
}

func TestProcessorMultipleParts(t *testing.T) {
	db := setupTestDB(t)
	execDDL(t, db, `CREATE TABLE test (id UInt64, name String) ENGINE = MergeTree() ORDER BY (id)`)

	// 3 separate inserts = 3 parts
	execDDL(t, db, `INSERT INTO test VALUES (1, 'alice')`)
	execDDL(t, db, `INSERT INTO test VALUES (2, 'bob')`)
	execDDL(t, db, `INSERT INTO test VALUES (3, 'charlie')`)

	table, _ := db.GetTable("test")
	parts := table.GetActiveParts()
	if len(parts) != 3 {
		t.Fatalf("expected 3 parts, got %d", len(parts))
	}

	// Push pipeline should read all 3 parts in parallel
	blocks := execSelectPush(t, db, `SELECT * FROM test ORDER BY id`)
	if countRows(blocks) != 3 {
		t.Fatalf("expected 3 rows from 3 parts, got %d", countRows(blocks))
	}
	block := blocks[0]
	col, _ := block.GetColumn("id")
	if col.Value(0).(uint64) != 1 {
		t.Fatalf("expected first id=1, got %v", col.Value(0))
	}
}

func TestProcessorPortStates(t *testing.T) {
	out := processor.NewOutputPort()
	inp := processor.NewInputPort()

	// Before connect: both idle
	if out.CanPush() {
		t.Fatal("disconnected output should not be pushable")
	}
	if inp.HasData() {
		t.Fatal("disconnected input should not have data")
	}

	// Connect: initial state is NeedData
	processor.Connect(out, inp)

	if !out.CanPush() {
		t.Fatal("after connect, output should be pushable")
	}
	if inp.HasData() {
		t.Fatal("after connect, input should not have data yet")
	}

	// Push data
	chunk := processor.NewChunk(nil)
	if !out.Push(chunk) {
		t.Fatal("push should succeed when NeedData")
	}
	if out.CanPush() {
		t.Fatal("after push, output should not be pushable (HasData)")
	}
	if !inp.HasData() {
		t.Fatal("after push, input should have data")
	}

	// Pull data
	got := inp.Pull()
	if got != chunk {
		t.Fatal("pulled chunk should match pushed chunk")
	}
	if !out.CanPush() {
		t.Fatal("after pull, output should be pushable again")
	}

	// Finish
	out.SetFinished()
	if !inp.IsFinished() {
		t.Fatal("after output finish, input should see finished")
	}
}

func TestProcessorEmptyTable(t *testing.T) {
	db := setupTestDB(t)
	execDDL(t, db, `CREATE TABLE test (id UInt64) ENGINE = MergeTree() ORDER BY (id)`)

	// count(*) on empty table should return 0
	blocks := execSelectPush(t, db, `SELECT count(*) FROM test`)
	if len(blocks) == 0 || blocks[0].NumRows() != 1 {
		t.Fatal("expected 1 row for count(*) on empty table")
	}
	col := blocks[0].Columns[0]
	if col.Value(0).(uint64) != 0 {
		t.Fatalf("expected count=0, got %v", col.Value(0))
	}
}

func TestProcessorWhereAndAggregate(t *testing.T) {
	db := setupTestDB(t)
	execDDL(t, db, `CREATE TABLE hits (id UInt64, url String, views Int64) ENGINE = MergeTree() ORDER BY (id)`)
	execDDL(t, db, `INSERT INTO hits VALUES (1, '/home', 100), (2, '/about', 50), (3, '/contact', 30), (4, '/home', 200), (5, '/about', 75)`)

	blocks := execSelectPush(t, db, `SELECT url, count(*), sum(views) FROM hits WHERE views >= 50 GROUP BY url`)
	if len(blocks) == 0 {
		t.Fatal("expected results")
	}
	// Should have /home (2 rows: 100, 200), /about (2 rows: 50, 75). /contact filtered out.
	if blocks[0].NumRows() != 2 {
		t.Fatalf("expected 2 groups, got %d", blocks[0].NumRows())
	}
}

func TestProcessorIntegration(t *testing.T) {
	// Test that wiring SelectExecutor works with engine.Execute
	db := setupTestDB(t)
	execDDL(t, db, `CREATE TABLE test (id UInt64, name String) ENGINE = MergeTree() ORDER BY (id)`)
	execDDL(t, db, `INSERT INTO test VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')`)

	// Wire push executor
	oldExecutor := engine.SelectExecutor
	engine.SelectExecutor = func(stmt *parser.SelectStmt, db *storage.Database) (*engine.ExecuteResult, error) {
		result, err := processor.BuildPipeline(stmt, db)
		if err != nil {
			return nil, err
		}
		exec := processor.NewPipelineExecutor(result.Graph, 4)
		if err := exec.Execute(); err != nil {
			return nil, err
		}
		blocks := result.Output.ResultBlocks()
		outNames := result.OutNames
		if len(blocks) > 0 && blocks[0].NumColumns() > 0 && len(outNames) != blocks[0].NumColumns() {
			outNames = blocks[0].ColumnNames
		}
		return &engine.ExecuteResult{Blocks: blocks, ColumnNames: outNames}, nil
	}
	defer func() { engine.SelectExecutor = oldExecutor }()

	// Now engine.Execute for SELECT should use push pipeline
	stmt, _ := parser.ParseSQL(`SELECT * FROM test`)
	result, err := engine.Execute(stmt, db)
	if err != nil {
		t.Fatalf("execute error: %v", err)
	}
	totalRows := 0
	for _, block := range result.Blocks {
		totalRows += block.NumRows()
	}
	if totalRows != 3 {
		t.Fatalf("expected 3 rows, got %d", totalRows)
	}
	if !strings.Contains(strings.Join(result.ColumnNames, ","), "id") {
		t.Fatalf("expected column names to contain 'id', got %v", result.ColumnNames)
	}
}

// ── Parallel Execution Tests ──────────────────────────────────────────

// setupTableWithGranuleSize creates a table with a custom granule size
// so we can create multi-granule parts for testing streaming source.
func setupTableWithGranuleSize(t *testing.T, granuleSize int) (*storage.Database, *storage.MergeTreeTable) {
	t.Helper()
	dir, err := os.MkdirTemp("", "goosedb-parallel-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	db, err := storage.NewDatabase(dir)
	if err != nil {
		t.Fatal(err)
	}

	schema := storage.TableSchema{
		Columns: []storage.ColumnDef{
			{Name: "id", DataType: types.TypeUInt64},
			{Name: "category", DataType: types.TypeString},
			{Name: "value", DataType: types.TypeInt64},
		},
		OrderBy:     []string{"id"},
		GranuleSize: granuleSize,
	}

	if err := db.CreateTable("test", schema); err != nil {
		t.Fatal(err)
	}

	table, _ := db.GetTable("test")
	return db, table
}

// insertRows inserts N rows into the table directly (bypassing SQL parsing).
func insertRows(t *testing.T, db *storage.Database, tableName string, n int) {
	t.Helper()
	table, ok := db.GetTable(tableName)
	if !ok {
		t.Fatalf("table %s not found", tableName)
	}

	idCol := column.NewColumnWithCapacity(types.TypeUInt64, n)
	catCol := column.NewColumnWithCapacity(types.TypeString, n)
	valCol := column.NewColumnWithCapacity(types.TypeInt64, n)

	for i := 0; i < n; i++ {
		idCol.Append(uint64(i + 1))
		catCol.Append(fmt.Sprintf("cat_%d", i%10))
		valCol.Append(int64(i * 10))
	}

	block := column.NewBlock(
		[]string{"id", "category", "value"},
		[]column.Column{idCol, catCol, valCol},
	)

	if err := table.Insert(block); err != nil {
		t.Fatalf("insert failed: %v", err)
	}
}

// TestStreamingSource verifies that the Source emits multiple chunks
// for a multi-granule part (morsel-driven streaming).
func TestStreamingSource(t *testing.T) {
	db, _ := setupTableWithGranuleSize(t, 10) // 10 rows per granule
	insertRows(t, db, "test", 50)             // 50 rows = 5 granules

	// Verify we have 1 part with 5 granules.
	table, _ := db.GetTable("test")
	parts := table.GetActiveParts()
	if len(parts) != 1 {
		t.Fatalf("expected 1 part, got %d", len(parts))
	}
	if parts[0].NumGranules != 5 {
		t.Fatalf("expected 5 granules, got %d", parts[0].NumGranules)
	}

	// Run a SELECT * — the streaming source should emit 5 separate chunks.
	blocks := execSelectPush(t, db, `SELECT * FROM test ORDER BY id`)
	total := countRows(blocks)
	if total != 50 {
		t.Fatalf("expected 50 rows, got %d", total)
	}

	// Verify first and last values.
	block := blocks[0]
	idCol, _ := block.GetColumn("id")
	if idCol.Value(0).(uint64) != 1 {
		t.Fatalf("expected first id=1, got %v", idCol.Value(0))
	}
}

// TestParallelFilter verifies per-part filters run correctly with
// multiple parts and produce correct results.
func TestParallelFilter(t *testing.T) {
	db, _ := setupTableWithGranuleSize(t, 10)

	// Insert 3 separate batches = 3 parts.
	insertRows(t, db, "test", 30) // part 1: id 1-30
	insertRows(t, db, "test", 30) // part 2: id 1-30
	insertRows(t, db, "test", 30) // part 3: id 1-30

	table, _ := db.GetTable("test")
	parts := table.GetActiveParts()
	if len(parts) != 3 {
		t.Fatalf("expected 3 parts, got %d", len(parts))
	}

	// Filter: value >= 100. Each part has rows with value 0,10,...,290.
	// Rows with value >= 100: those with i >= 10, so 20 rows per part.
	blocks := execSelectPush(t, db, `SELECT id, value FROM test WHERE value >= 100`)
	total := countRows(blocks)
	// Each part: 30 rows, values 0,10,...,290. value >= 100 → i >= 10 → 20 rows.
	expected := 20 * 3
	if total != expected {
		t.Fatalf("expected %d rows with value >= 100, got %d", expected, total)
	}
}

// TestParallelAggregation verifies two-phase parallel aggregation
// produces correct results across multiple parts.
func TestParallelAggregation(t *testing.T) {
	db := setupTestDB(t)
	execDDL(t, db, `CREATE TABLE test (id UInt64, category String, value Int64) ENGINE = MergeTree() ORDER BY (id)`)

	// Insert into 3 parts.
	execDDL(t, db, `INSERT INTO test VALUES (1, 'a', 10), (2, 'b', 20)`)
	execDDL(t, db, `INSERT INTO test VALUES (3, 'a', 30), (4, 'b', 40)`)
	execDDL(t, db, `INSERT INTO test VALUES (5, 'a', 50)`)

	table, _ := db.GetTable("test")
	if len(table.GetActiveParts()) != 3 {
		t.Fatalf("expected 3 parts, got %d", len(table.GetActiveParts()))
	}

	// GROUP BY with count, sum across 3 parts.
	blocks := execSelectPush(t, db, `SELECT category, count(*), sum(value) FROM test GROUP BY category`)
	if len(blocks) == 0 {
		t.Fatal("expected results")
	}
	block := blocks[0]
	if block.NumRows() != 2 {
		t.Fatalf("expected 2 groups, got %d", block.NumRows())
	}

	// Verify totals (order may vary).
	catCol, _ := block.GetColumn("category")
	cntCol, _ := block.GetColumn("count(*)")
	sumCol, _ := block.GetColumn("sum(value)")

	totals := map[string][2]float64{} // category -> [count, sum]
	for i := 0; i < block.NumRows(); i++ {
		cat := catCol.Value(i).(string)
		var cnt float64
		switch v := cntCol.Value(i).(type) {
		case uint64:
			cnt = float64(v)
		case float64:
			cnt = v
		}
		var sum float64
		switch v := sumCol.Value(i).(type) {
		case float64:
			sum = v
		case int64:
			sum = float64(v)
		}
		totals[cat] = [2]float64{cnt, sum}
	}

	// category 'a': count=3, sum=10+30+50=90
	if totals["a"][0] != 3 || totals["a"][1] != 90 {
		t.Fatalf("category 'a': expected count=3 sum=90, got count=%v sum=%v", totals["a"][0], totals["a"][1])
	}
	// category 'b': count=2, sum=20+40=60
	if totals["b"][0] != 2 || totals["b"][1] != 60 {
		t.Fatalf("category 'b': expected count=2 sum=60, got count=%v sum=%v", totals["b"][0], totals["b"][1])
	}
}

// TestParallelAggregationMinMax verifies min/max aggregate merging across parts.
func TestParallelAggregationMinMax(t *testing.T) {
	db := setupTestDB(t)
	execDDL(t, db, `CREATE TABLE test (id UInt64, value Int64) ENGINE = MergeTree() ORDER BY (id)`)

	execDDL(t, db, `INSERT INTO test VALUES (1, 100), (2, 50)`)
	execDDL(t, db, `INSERT INTO test VALUES (3, 200), (4, 25)`)
	execDDL(t, db, `INSERT INTO test VALUES (5, 150), (6, 75)`)

	blocks := execSelectPush(t, db, `SELECT min(value), max(value) FROM test`)
	if len(blocks) == 0 || blocks[0].NumRows() != 1 {
		t.Fatal("expected 1 row")
	}
	block := blocks[0]

	minCol := block.Columns[0]
	maxCol := block.Columns[1]

	minVal := minCol.Value(0).(float64)
	maxVal := maxCol.Value(0).(float64)

	if minVal != 25 {
		t.Fatalf("expected min=25, got %v", minVal)
	}
	if maxVal != 200 {
		t.Fatalf("expected max=200, got %v", maxVal)
	}
}

// TestParallelAggregationAvg verifies avg merging across parts.
func TestParallelAggregationAvg(t *testing.T) {
	db := setupTestDB(t)
	execDDL(t, db, `CREATE TABLE test (id UInt64, value Int64) ENGINE = MergeTree() ORDER BY (id)`)

	// Part 1: values 10, 20 (avg=15)
	execDDL(t, db, `INSERT INTO test VALUES (1, 10), (2, 20)`)
	// Part 2: values 30 (avg=30)
	execDDL(t, db, `INSERT INTO test VALUES (3, 30)`)

	blocks := execSelectPush(t, db, `SELECT avg(value) FROM test`)
	if len(blocks) == 0 || blocks[0].NumRows() != 1 {
		t.Fatal("expected 1 row")
	}

	avgVal := blocks[0].Columns[0].Value(0).(float64)
	// Overall avg = (10+20+30)/3 = 20
	if avgVal != 20 {
		t.Fatalf("expected avg=20, got %v", avgVal)
	}
}

// TestParallelWhereAndAggregate tests combined WHERE + GROUP BY
// across multiple parts with parallel execution.
func TestParallelWhereAndAggregate(t *testing.T) {
	db := setupTestDB(t)
	execDDL(t, db, `CREATE TABLE hits (id UInt64, url String, views Int64) ENGINE = MergeTree() ORDER BY (id)`)

	// 3 parts.
	execDDL(t, db, `INSERT INTO hits VALUES (1, '/home', 100), (2, '/about', 50)`)
	execDDL(t, db, `INSERT INTO hits VALUES (3, '/contact', 30), (4, '/home', 200)`)
	execDDL(t, db, `INSERT INTO hits VALUES (5, '/about', 75)`)

	blocks := execSelectPush(t, db, `SELECT url, count(*), sum(views) FROM hits WHERE views >= 50 GROUP BY url`)
	if len(blocks) == 0 {
		t.Fatal("expected results")
	}
	// After filter: /home(100), /about(50), /home(200), /about(75) = 4 rows
	// Groups: /home(count=2, sum=300), /about(count=2, sum=125)
	if blocks[0].NumRows() != 2 {
		t.Fatalf("expected 2 groups, got %d", blocks[0].NumRows())
	}
}

// TestStreamingSourceMultiGranule verifies the Output receives multiple
// chunks from a single multi-granule part when using the streaming source.
func TestStreamingSourceMultiGranule(t *testing.T) {
	db, _ := setupTableWithGranuleSize(t, 5) // 5 rows per granule
	insertRows(t, db, "test", 25)            // 25 rows = 5 granules

	stmt, err := parser.ParseSQL(`SELECT * FROM test`)
	if err != nil {
		t.Fatal(err)
	}
	selStmt := stmt.(*parser.SelectStmt)

	result, err := processor.BuildPipeline(selStmt, db)
	if err != nil {
		t.Fatal(err)
	}

	exec := processor.NewPipelineExecutor(result.Graph, 4)
	if err := exec.Execute(); err != nil {
		t.Fatal(err)
	}

	// With 5 granules and batchSize=1, we should get multiple output chunks.
	chunks := result.Output.Chunks
	if len(chunks) < 2 {
		t.Fatalf("expected multiple output chunks from streaming source, got %d", len(chunks))
	}

	total := 0
	for _, c := range chunks {
		if c != nil {
			total += c.NumRows()
		}
	}
	if total != 25 {
		t.Fatalf("expected 25 total rows, got %d", total)
	}
}
