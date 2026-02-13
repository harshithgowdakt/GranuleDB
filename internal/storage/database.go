package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/harshithgowda/goose-db/internal/types"
)

// Database manages all tables and the base data directory.
type Database struct {
	DataDir string
	tables  map[string]*MergeTreeTable
	mu      sync.RWMutex
}

// NewDatabase creates a new database rooted at dataDir.
func NewDatabase(dataDir string) (*Database, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("creating data dir: %w", err)
	}
	db := &Database{
		DataDir: dataDir,
		tables:  make(map[string]*MergeTreeTable),
	}
	if err := db.LoadMetadata(); err != nil {
		return nil, fmt.Errorf("loading metadata: %w", err)
	}
	return db, nil
}

// CreateTable creates a new table.
func (db *Database) CreateTable(name string, schema TableSchema) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if _, exists := db.tables[name]; exists {
		return fmt.Errorf("table %s already exists", name)
	}

	tableDir := filepath.Join(db.DataDir, name)
	if err := os.MkdirAll(tableDir, 0755); err != nil {
		return err
	}

	// Save schema metadata
	if err := saveTableSchema(tableDir, name, &schema); err != nil {
		return err
	}

	db.tables[name] = NewMergeTreeTable(name, schema, tableDir)
	return nil
}

// GetTable returns a table by name.
func (db *Database) GetTable(name string) (*MergeTreeTable, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	t, ok := db.tables[name]
	return t, ok
}

// DropTable removes a table and its data.
func (db *Database) DropTable(name string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	t, ok := db.tables[name]
	if !ok {
		return fmt.Errorf("table %s does not exist", name)
	}

	if err := os.RemoveAll(t.DataDir); err != nil {
		return err
	}
	delete(db.tables, name)
	return nil
}

// TableNames returns all table names.
func (db *Database) TableNames() []string {
	db.mu.RLock()
	defer db.mu.RUnlock()
	names := make([]string, 0, len(db.tables))
	for n := range db.tables {
		names = append(names, n)
	}
	return names
}

// AllTables returns all tables.
func (db *Database) AllTables() []*MergeTreeTable {
	db.mu.RLock()
	defer db.mu.RUnlock()
	tables := make([]*MergeTreeTable, 0, len(db.tables))
	for _, t := range db.tables {
		tables = append(tables, t)
	}
	return tables
}

// LoadMetadata scans the data directory on startup, reconstructing table and part metadata.
func (db *Database) LoadMetadata() error {
	entries, err := os.ReadDir(db.DataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		tableName := entry.Name()
		tableDir := filepath.Join(db.DataDir, tableName)

		schema, err := loadTableSchema(tableDir)
		if err != nil {
			continue // skip directories without valid schema
		}

		table := NewMergeTreeTable(tableName, *schema, tableDir)

		// Scan for parts
		partEntries, err := os.ReadDir(tableDir)
		if err != nil {
			continue
		}
		for _, pe := range partEntries {
			if !pe.IsDir() || strings.HasPrefix(pe.Name(), "tmp_") {
				continue
			}

			partInfo, err := parsePartDirName(pe.Name())
			if err != nil {
				continue
			}

			// Read count.txt
			countPath := filepath.Join(tableDir, pe.Name(), "count.txt")
			countData, err := os.ReadFile(countPath)
			if err != nil {
				continue
			}
			numRows, err := strconv.ParseUint(strings.TrimSpace(string(countData)), 10, 64)
			if err != nil {
				continue
			}

			granuleSize := schema.EffectiveGranuleSize()
			numGranules := (int(numRows) + granuleSize - 1) / granuleSize

			part := &Part{
				Info:        *partInfo,
				State:       PartActive,
				NumRows:     numRows,
				BasePath:    filepath.Join(tableDir, pe.Name()),
				NumGranules: numGranules,
			}
			table.AddPart(part)
		}

		db.tables[tableName] = table
	}
	return nil
}

// parsePartDirName parses "partition_min_max_level" into PartInfo.
func parsePartDirName(name string) (*PartInfo, error) {
	// Format: partitionID_minBlock_maxBlock_level
	// Find the last 3 underscores to split, since partitionID can contain underscores
	parts := strings.Split(name, "_")
	if len(parts) < 4 {
		return nil, fmt.Errorf("invalid part dir name: %s", name)
	}

	// Last 3 components are minBlock, maxBlock, level
	level, err := strconv.ParseUint(parts[len(parts)-1], 10, 32)
	if err != nil {
		return nil, err
	}
	maxBlock, err := strconv.ParseUint(parts[len(parts)-2], 10, 64)
	if err != nil {
		return nil, err
	}
	minBlock, err := strconv.ParseUint(parts[len(parts)-3], 10, 64)
	if err != nil {
		return nil, err
	}
	partitionID := strings.Join(parts[:len(parts)-3], "_")

	return &PartInfo{
		PartitionID: partitionID,
		MinBlock:    minBlock,
		MaxBlock:    maxBlock,
		Level:       uint32(level),
	}, nil
}

// tableSchemaJSON is the JSON representation of a table schema saved to disk.
type tableSchemaJSON struct {
	Name    string `json:"name"`
	Columns []struct {
		Name     string `json:"name"`
		DataType string `json:"data_type"`
	} `json:"columns"`
	OrderBy     []string `json:"order_by"`
	PartitionBy string   `json:"partition_by,omitempty"`
	GranuleSize int      `json:"granule_size"`
}

func saveTableSchema(tableDir, name string, schema *TableSchema) error {
	j := tableSchemaJSON{
		Name:        name,
		OrderBy:     schema.OrderBy,
		PartitionBy: schema.PartitionBy,
		GranuleSize: schema.EffectiveGranuleSize(),
	}
	for _, c := range schema.Columns {
		j.Columns = append(j.Columns, struct {
			Name     string `json:"name"`
			DataType string `json:"data_type"`
		}{
			Name:     c.Name,
			DataType: c.DataType.Name(),
		})
	}
	data, err := json.MarshalIndent(j, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(tableDir, "schema.json"), data, 0644)
}

func loadTableSchema(tableDir string) (*TableSchema, error) {
	data, err := os.ReadFile(filepath.Join(tableDir, "schema.json"))
	if err != nil {
		return nil, err
	}
	var j tableSchemaJSON
	if err := json.Unmarshal(data, &j); err != nil {
		return nil, err
	}
	schema := &TableSchema{
		OrderBy:     j.OrderBy,
		PartitionBy: j.PartitionBy,
		GranuleSize: j.GranuleSize,
	}
	for _, c := range j.Columns {
		dt, err := types.ParseDataType(c.DataType)
		if err != nil {
			return nil, err
		}
		schema.Columns = append(schema.Columns, ColumnDef{Name: c.Name, DataType: dt})
	}
	return schema, nil
}
