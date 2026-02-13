package processor_test

import (
	"os"
	"strings"
	"testing"

	"github.com/harshithgowda/goose-db/internal/column"
	"github.com/harshithgowda/goose-db/internal/engine"
	"github.com/harshithgowda/goose-db/internal/parser"
	"github.com/harshithgowda/goose-db/internal/processor"
	"github.com/harshithgowda/goose-db/internal/storage"
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
