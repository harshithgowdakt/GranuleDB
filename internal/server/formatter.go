package server

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/harshithgowda/goose-db/internal/column"
	"github.com/harshithgowda/goose-db/internal/types"
)

// OutputFormat specifies the result format.
type OutputFormat string

const (
	FormatTabSeparated OutputFormat = "TabSeparated"
	FormatJSON         OutputFormat = "JSON"
	FormatCSV          OutputFormat = "CSV"
)

// ParseFormat parses a format string (case-insensitive).
func ParseFormat(s string) OutputFormat {
	switch strings.ToLower(s) {
	case "json":
		return FormatJSON
	case "csv":
		return FormatCSV
	default:
		return FormatTabSeparated
	}
}

// FormatBlocks writes result blocks in the specified format.
func FormatBlocks(w io.Writer, blocks []*column.Block, colNames []string, format OutputFormat) error {
	switch format {
	case FormatJSON:
		return formatJSON(w, blocks, colNames)
	case FormatCSV:
		return formatCSV(w, blocks, colNames)
	default:
		return formatTabSeparated(w, blocks, colNames)
	}
}

func formatTabSeparated(w io.Writer, blocks []*column.Block, colNames []string) error {
	// Header
	fmt.Fprintln(w, strings.Join(colNames, "\t"))

	// Rows
	for _, block := range blocks {
		for row := range block.NumRows() {
			var vals []string
			for c := range block.NumColumns() {
				col := block.Columns[c]
				v := col.Value(row)
				vals = append(vals, formatValue(col.DataType(), v))
			}
			fmt.Fprintln(w, strings.Join(vals, "\t"))
		}
	}
	return nil
}

func formatCSV(w io.Writer, blocks []*column.Block, colNames []string) error {
	// Header
	fmt.Fprintln(w, strings.Join(quoteCSV(colNames), ","))

	// Rows
	for _, block := range blocks {
		for row := range block.NumRows() {
			var vals []string
			for c := range block.NumColumns() {
				col := block.Columns[c]
				v := col.Value(row)
				s := formatValue(col.DataType(), v)
				if col.DataType() == types.TypeString {
					s = `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
				}
				vals = append(vals, s)
			}
			fmt.Fprintln(w, strings.Join(vals, ","))
		}
	}
	return nil
}

func formatJSON(w io.Writer, blocks []*column.Block, colNames []string) error {
	type resultJSON struct {
		Meta []map[string]string   `json:"meta"`
		Data []map[string]interface{} `json:"data"`
		Rows int                    `json:"rows"`
	}

	result := resultJSON{}

	// Meta
	if len(blocks) > 0 && blocks[0].NumColumns() > 0 {
		for i, name := range colNames {
			meta := map[string]string{"name": name}
			if i < blocks[0].NumColumns() {
				meta["type"] = blocks[0].Columns[i].DataType().Name()
			}
			result.Meta = append(result.Meta, meta)
		}
	}

	// Data
	totalRows := 0
	for _, block := range blocks {
		for row := range block.NumRows() {
			rowMap := make(map[string]interface{})
			for c, name := range colNames {
				if c < block.NumColumns() {
					col := block.Columns[c]
					rowMap[name] = col.Value(row)
				}
			}
			result.Data = append(result.Data, rowMap)
			totalRows++
		}
	}
	result.Rows = totalRows

	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(result)
}

func formatValue(dt types.DataType, v types.Value) string {
	if v == nil {
		return "NULL"
	}
	switch dt {
	case types.TypeFloat32:
		return fmt.Sprintf("%g", v.(float32))
	case types.TypeFloat64:
		return fmt.Sprintf("%g", v.(float64))
	default:
		return fmt.Sprintf("%v", v)
	}
}

func quoteCSV(vals []string) []string {
	result := make([]string, len(vals))
	for i, v := range vals {
		if strings.ContainsAny(v, ",\"\n") {
			result[i] = `"` + strings.ReplaceAll(v, `"`, `""`) + `"`
		} else {
			result[i] = v
		}
	}
	return result
}
