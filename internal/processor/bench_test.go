package processor_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/harshithgowdakt/granuledb/internal/column"
	"github.com/harshithgowdakt/granuledb/internal/parser"
	"github.com/harshithgowdakt/granuledb/internal/processor"
	"github.com/harshithgowdakt/granuledb/internal/storage"
	"github.com/harshithgowdakt/granuledb/internal/types"
)

// benchSetup creates a database with a table and inserts rows across numParts parts.
func benchSetup(b *testing.B, rowsPerPart, numParts int) *storage.Database {
	b.Helper()
	dir, err := os.MkdirTemp("", "granuledb-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { os.RemoveAll(dir) })

	db, err := storage.NewDatabase(dir)
	if err != nil {
		b.Fatal(err)
	}

	schema := storage.TableSchema{
		Columns: []storage.ColumnDef{
			{Name: "id", DataType: types.TypeUInt64},
			{Name: "category", DataType: types.TypeString},
			{Name: "value", DataType: types.TypeInt64},
		},
		OrderBy: []string{"id"},
	}
	if err := db.CreateTable("bench", schema); err != nil {
		b.Fatal(err)
	}

	table, _ := db.GetTable("bench")
	baseID := uint64(0)
	for p := 0; p < numParts; p++ {
		idCol := column.NewColumnWithCapacity(types.TypeUInt64, rowsPerPart)
		catCol := column.NewColumnWithCapacity(types.TypeString, rowsPerPart)
		valCol := column.NewColumnWithCapacity(types.TypeInt64, rowsPerPart)
		for i := 0; i < rowsPerPart; i++ {
			baseID++
			idCol.Append(baseID)
			catCol.Append(fmt.Sprintf("cat_%d", i%100))
			valCol.Append(int64(i * 10))
		}
		block := column.NewBlock(
			[]string{"id", "category", "value"},
			[]column.Column{idCol, catCol, valCol},
		)
		if err := table.Insert(block); err != nil {
			b.Fatal(err)
		}
	}

	return db
}

func benchExec(b *testing.B, db *storage.Database, sql string, numWorkers int) {
	b.Helper()
	stmt, err := parser.ParseSQL(sql)
	if err != nil {
		b.Fatal(err)
	}
	selStmt := stmt.(*parser.SelectStmt)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := processor.BuildPipeline(selStmt, db)
		if err != nil {
			b.Fatal(err)
		}
		exec := processor.NewPipelineExecutor(result.Graph, numWorkers)
		if err := exec.Execute(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkParallelScan measures scan throughput with parallel source processors.
func BenchmarkParallelScan(b *testing.B) {
	db := benchSetup(b, 10000, 4) // 4 parts, 10K rows each = 40K rows
	benchExec(b, db, `SELECT * FROM bench`, 0)
}

// BenchmarkParallelFilter measures filter throughput with per-part parallel filters.
func BenchmarkParallelFilter(b *testing.B) {
	db := benchSetup(b, 10000, 4)
	benchExec(b, db, `SELECT id, value FROM bench WHERE value >= 500`, 0)
}

// BenchmarkParallelAggregate measures two-phase parallel aggregation throughput.
func BenchmarkParallelAggregate(b *testing.B) {
	db := benchSetup(b, 10000, 4)
	benchExec(b, db, `SELECT category, count(*), sum(value) FROM bench GROUP BY category`, 0)
}

// BenchmarkParallelScan_1Worker vs BenchmarkParallelScan_4Workers shows speedup.
func BenchmarkParallelScan_1Worker(b *testing.B) {
	db := benchSetup(b, 10000, 4)
	benchExec(b, db, `SELECT * FROM bench`, 1)
}

func BenchmarkParallelScan_4Workers(b *testing.B) {
	db := benchSetup(b, 10000, 4)
	benchExec(b, db, `SELECT * FROM bench`, 4)
}

func BenchmarkParallelAggregate_1Worker(b *testing.B) {
	db := benchSetup(b, 10000, 4)
	benchExec(b, db, `SELECT category, count(*), sum(value) FROM bench GROUP BY category`, 1)
}

func BenchmarkParallelAggregate_4Workers(b *testing.B) {
	db := benchSetup(b, 10000, 4)
	benchExec(b, db, `SELECT category, count(*), sum(value) FROM bench GROUP BY category`, 4)
}
