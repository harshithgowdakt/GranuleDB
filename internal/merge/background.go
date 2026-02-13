package merge

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/harshithgowda/goose-db/internal/compression"
	"github.com/harshithgowda/goose-db/internal/storage"
)

// BackgroundMerger runs in a goroutine, periodically checking for merge opportunities.
type BackgroundMerger struct {
	db       *storage.Database
	selector *SimpleMergeSelector
	interval time.Duration
}

// NewBackgroundMerger creates a new background merger.
func NewBackgroundMerger(db *storage.Database) *BackgroundMerger {
	return &BackgroundMerger{
		db:       db,
		selector: NewSimpleMergeSelector(),
		interval: 5 * time.Second,
	}
}

// Run starts the background merge loop. It blocks until ctx is cancelled.
func (bm *BackgroundMerger) Run(ctx context.Context) {
	ticker := time.NewTicker(bm.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			bm.tryMerge()
		}
	}
}

func (bm *BackgroundMerger) tryMerge() {
	for _, table := range bm.db.AllTables() {
		activeParts := table.GetActiveParts()
		if len(activeParts) < 3 {
			continue
		}

		toMerge := bm.selector.SelectPartsToMerge(activeParts)
		if toMerge == nil {
			continue
		}

		codec := &compression.LZ4Codec{}
		executor := NewMergeExecutor(&table.Schema, codec)

		newPart, err := executor.Merge(table.DataDir, toMerge)
		if err != nil {
			log.Printf("[merge] table %s: merge failed: %v", table.Name, err)
			continue
		}

		log.Printf("[merge] table %s: merged %d parts into %s (%d rows)",
			table.Name, len(toMerge), newPart.Info.DirName(), newPart.NumRows)

		// Atomic swap
		table.ReplaceParts(toMerge, newPart)

		// Schedule cleanup of outdated part directories
		for _, old := range toMerge {
			go func(p *storage.Part) {
				time.Sleep(10 * time.Second) // grace period for in-flight reads
				os.RemoveAll(p.BasePath)
			}(old)
		}
	}
}
