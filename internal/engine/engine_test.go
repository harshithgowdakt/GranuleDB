package engine_test

import (
	"os"
	"strings"
	"testing"

	"github.com/harshithgowda/goose-db/internal/engine"
	"github.com/harshithgowda/goose-db/internal/parser"
	"github.com/harshithgowda/goose-db/internal/storage"
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
