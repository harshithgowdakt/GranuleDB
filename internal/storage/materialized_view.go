package storage

import "fmt"

// MaterializedView defines an insert-triggered view that transforms rows
// from SourceTable and inserts results into TargetTable.
type MaterializedView struct {
	Name        string
	SourceTable string
	TargetTable string
	SelectSQL   string
}

// CreateMaterializedView registers a materialized view in metadata.
// Note: view metadata is currently in-memory only.
func (db *Database) CreateMaterializedView(mv MaterializedView) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if _, ok := db.mviewsByName[mv.Name]; ok {
		return fmt.Errorf("materialized view %s already exists", mv.Name)
	}
	if _, ok := db.tables[mv.SourceTable]; !ok {
		return fmt.Errorf("source table %s does not exist", mv.SourceTable)
	}
	if _, ok := db.tables[mv.TargetTable]; !ok {
		return fmt.Errorf("target table %s does not exist", mv.TargetTable)
	}

	db.mviewsByName[mv.Name] = mv
	db.mviewsBySource[mv.SourceTable] = append(db.mviewsBySource[mv.SourceTable], mv)
	return nil
}

// MaterializedViewsForSource returns all materialized views attached to sourceTable.
func (db *Database) MaterializedViewsForSource(sourceTable string) []MaterializedView {
	db.mu.RLock()
	defer db.mu.RUnlock()

	src := db.mviewsBySource[sourceTable]
	out := make([]MaterializedView, len(src))
	copy(out, src)
	return out
}
