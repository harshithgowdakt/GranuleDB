package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/harshithgowdakt/granuledb/internal/compression"
	"github.com/harshithgowdakt/granuledb/internal/storage"
	"github.com/harshithgowdakt/granuledb/internal/types"
)

type markJSON struct {
	Granule                 int    `json:"granule"`
	OffsetInCompressedFile  uint64 `json:"offset_in_compressed_file"`
	OffsetInDecompressedBlk uint64 `json:"offset_in_decompressed_block"`
}

type binBlockJSON struct {
	Granule          int    `json:"granule"`
	Offset           int    `json:"offset"`
	MethodByte       uint8  `json:"method_byte"`
	CompressedBytes  uint32 `json:"compressed_bytes_with_header"`
	UncompressedSize uint32 `json:"uncompressed_bytes"`
}

type binJSON struct {
	FileSize int64          `json:"file_size"`
	Blocks   []binBlockJSON `json:"blocks"`
}

type primaryGranuleJSON struct {
	Granule int               `json:"granule"`
	Keys    map[string]string `json:"keys"`
}

type minmaxJSON struct {
	Column string `json:"column"`
	Type   string `json:"type"`
	Min    string `json:"min"`
	Max    string `json:"max"`
}

type dumpJSON struct {
	Table      string                `json:"table"`
	Part       string                `json:"part"`
	CountTxt   string                `json:"count_txt"`
	ColumnsTxt string                `json:"columns_txt"`
	Marks      map[string][]markJSON `json:"marks"`
	BinSummary map[string]binJSON    `json:"bin_summary"`
	PrimaryIdx []primaryGranuleJSON  `json:"primary_idx"`
	MinMaxIdx  map[string]minmaxJSON `json:"minmax_idx"`
}

func main() {
	dataDir := flag.String("data-dir", "./granuledb-data", "Database data directory")
	tableName := flag.String("table", "", "Table name")
	partName := flag.String("part", "", "Part directory name (e.g. all_1_1_0)")
	flag.Parse()

	if *tableName == "" {
		fatalf("missing required -table")
	}

	db, err := storage.NewDatabase(*dataDir)
	if err != nil {
		fatalf("open database: %v", err)
	}

	table, ok := db.GetTable(*tableName)
	if !ok {
		fatalf("table %q not found", *tableName)
	}

	parts := table.GetActiveParts()
	if *partName == "" {
		names := make([]string, 0, len(parts))
		for _, p := range parts {
			names = append(names, p.Info.DirName())
		}
		out, _ := json.MarshalIndent(map[string]any{
			"table":        *tableName,
			"active_parts": names,
		}, "", "  ")
		fmt.Println(string(out))
		return
	}

	var part *storage.Part
	for _, p := range parts {
		if p.Info.DirName() == *partName {
			part = p
			break
		}
	}
	if part == nil {
		fatalf("part %q not found among active parts", *partName)
	}

	reader := storage.NewPartReader(part, &table.Schema)
	out := dumpJSON{
		Table:      *tableName,
		Part:       *partName,
		Marks:      make(map[string][]markJSON),
		BinSummary: make(map[string]binJSON),
		MinMaxIdx:  make(map[string]minmaxJSON),
	}

	countBytes, err := os.ReadFile(filepath.Join(part.BasePath, "count.txt"))
	if err == nil {
		out.CountTxt = strings.TrimSpace(string(countBytes))
	}
	columnsBytes, err := os.ReadFile(filepath.Join(part.BasePath, "columns.txt"))
	if err == nil {
		out.ColumnsTxt = string(columnsBytes)
	}

	entries, err := os.ReadDir(part.BasePath)
	if err != nil {
		fatalf("read part dir: %v", err)
	}

	for _, ent := range entries {
		name := ent.Name()
		full := filepath.Join(part.BasePath, name)

		if strings.HasSuffix(name, ".mrk") {
			colName := strings.TrimSuffix(name, ".mrk")
			marks, err := storage.ReadMarksFromFile(full)
			if err != nil {
				continue
			}
			j := make([]markJSON, 0, len(marks))
			for i, m := range marks {
				j = append(j, markJSON{
					Granule:                 i,
					OffsetInCompressedFile:  m.OffsetInCompressedFile,
					OffsetInDecompressedBlk: m.OffsetInDecompressedBlock,
				})
			}
			out.Marks[colName] = j
			continue
		}

		if strings.HasSuffix(name, ".bin") {
			colName := strings.TrimSuffix(name, ".bin")
			data, err := os.ReadFile(full)
			if err != nil {
				continue
			}
			bj := binJSON{FileSize: int64(len(data))}

			marks := out.Marks[colName]
			for i, m := range marks {
				off := int(m.OffsetInCompressedFile)
				if off < 0 || off >= len(data) {
					continue
				}
				block := data[off:]
				csz, usz, err := compression.ReadBlockHeader(block)
				if err != nil {
					continue
				}
				method := uint8(0)
				if len(block) > 0 {
					method = block[0]
				}
				bj.Blocks = append(bj.Blocks, binBlockJSON{
					Granule:          i,
					Offset:           off,
					MethodByte:       method,
					CompressedBytes:  csz,
					UncompressedSize: usz,
				})
			}
			out.BinSummary[colName] = bj
			continue
		}

		if strings.HasPrefix(name, "minmax_") && strings.HasSuffix(name, ".idx") {
			col := strings.TrimSuffix(strings.TrimPrefix(name, "minmax_"), ".idx")
			colDef, ok := table.Schema.GetColumnDef(col)
			if !ok {
				continue
			}
			mm, err := storage.ReadMinMaxIndex(full, col, colDef.DataType)
			if err != nil {
				continue
			}
			out.MinMaxIdx[col] = minmaxJSON{
				Column: mm.ColumnName,
				Type:   mm.DataType.Name(),
				Min:    types.ValueToString(mm.DataType, mm.Min),
				Max:    types.ValueToString(mm.DataType, mm.Max),
			}
		}
	}

	if idx, err := reader.LoadPrimaryIndex(); err == nil && idx != nil {
		out.PrimaryIdx = make([]primaryGranuleJSON, 0, idx.NumGranules)
		for g := 0; g < idx.NumGranules; g++ {
			keys := make(map[string]string, len(idx.KeyColumns))
			for i, keyCol := range idx.KeyColumns {
				keys[keyCol] = types.ValueToString(idx.KeyTypes[i], idx.Values[g][i])
			}
			out.PrimaryIdx = append(out.PrimaryIdx, primaryGranuleJSON{
				Granule: g,
				Keys:    keys,
			})
		}
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(out); err != nil {
		fatalf("encode json: %v", err)
	}
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
