package storage

import (
	"fmt"
	"time"
)

// PartState represents the lifecycle state of a data part.
type PartState uint8

const (
	PartTemporary PartState = iota // tmp_ prefix, being written
	PartActive                     // visible to queries
	PartOutdated                   // replaced by merge, pending deletion
	PartDeleting                   // being deleted
)

// PartInfo identifies a part following ClickHouse naming: partition_minBlock_maxBlock_level.
type PartInfo struct {
	PartitionID string
	MinBlock    uint64
	MaxBlock    uint64
	Level       uint32
}

// DirName returns the directory name for this part.
func (pi PartInfo) DirName() string {
	return fmt.Sprintf("%s_%d_%d_%d", pi.PartitionID, pi.MinBlock, pi.MaxBlock, pi.Level)
}

// TmpDirName returns the temporary directory name.
func (pi PartInfo) TmpDirName() string {
	return "tmp_" + pi.DirName()
}

// Contains returns true if this part's block range fully covers another part's range.
func (pi PartInfo) Contains(other PartInfo) bool {
	return pi.PartitionID == other.PartitionID &&
		pi.MinBlock <= other.MinBlock &&
		pi.MaxBlock >= other.MaxBlock &&
		pi.Level > other.Level
}

// Part represents a single data part on disk.
type Part struct {
	Info      PartInfo
	State     PartState
	NumRows   uint64
	SizeBytes uint64
	CreatedAt time.Time
	BasePath  string // absolute path to the part directory

	// Cached metadata (loaded lazily)
	NumGranules int
}

func (p *Part) String() string {
	return fmt.Sprintf("Part{%s, rows=%d, state=%d}", p.Info.DirName(), p.NumRows, p.State)
}
