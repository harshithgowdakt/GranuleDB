package storage

import "github.com/harshithgowda/goose-db/internal/types"

// ColumnDef defines a column in a table schema.
type ColumnDef struct {
	Name             string
	DataType         types.DataType
	IsLowCardinality bool
}

// TableSchema defines the schema and engine settings for a MergeTree table.
type TableSchema struct {
	Columns     []ColumnDef
	OrderBy     []string // primary key column names (ORDER BY clause)
	PartitionBy string   // single column name or empty
	GranuleSize int      // rows per granule, default 8192
}

// GetColumnDef returns the ColumnDef for a column name.
func (s *TableSchema) GetColumnDef(name string) (ColumnDef, bool) {
	for _, c := range s.Columns {
		if c.Name == name {
			return c, true
		}
	}
	return ColumnDef{}, false
}

// ColumnNames returns all column names in order.
func (s *TableSchema) ColumnNames() []string {
	names := make([]string, len(s.Columns))
	for i, c := range s.Columns {
		names[i] = c.Name
	}
	return names
}

// EffectiveGranuleSize returns the granule size, defaulting to 8192.
func (s *TableSchema) EffectiveGranuleSize() int {
	if s.GranuleSize <= 0 {
		return DefaultGranuleSize
	}
	return s.GranuleSize
}
