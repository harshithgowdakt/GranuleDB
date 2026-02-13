package server

import (
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/harshithgowdakt/granuledb/internal/engine"
	"github.com/harshithgowdakt/granuledb/internal/parser"
	"github.com/harshithgowdakt/granuledb/internal/storage"
)

// QueryHandler handles HTTP query requests.
type QueryHandler struct {
	db *storage.Database
}

// NewQueryHandler creates a new query handler.
func NewQueryHandler(db *storage.Database) *QueryHandler {
	return &QueryHandler{db: db}
}

// HandleQuery processes SQL queries received via HTTP.
func (h *QueryHandler) HandleQuery(w http.ResponseWriter, r *http.Request) {
	// Extract query text
	query := r.URL.Query().Get("query")
	if query == "" {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "failed to read request body", http.StatusBadRequest)
			return
		}
		query = strings.TrimSpace(string(body))
	}

	if query == "" {
		http.Error(w, "empty query", http.StatusBadRequest)
		return
	}

	// Determine output format
	format := ParseFormat(r.URL.Query().Get("format"))

	// Parse
	stmt, err := parser.ParseSQL(query)
	if err != nil {
		http.Error(w, fmt.Sprintf("parse error: %v", err), http.StatusBadRequest)
		return
	}

	// Execute
	result, err := engine.Execute(stmt, h.db)
	if err != nil {
		http.Error(w, fmt.Sprintf("execution error: %v", err), http.StatusInternalServerError)
		return
	}

	// Format response
	if len(result.Blocks) > 0 {
		switch format {
		case FormatJSON:
			w.Header().Set("Content-Type", "application/json")
		case FormatCSV:
			w.Header().Set("Content-Type", "text/csv")
		default:
			w.Header().Set("Content-Type", "text/tab-separated-values")
		}
		if err := FormatBlocks(w, result.Blocks, result.ColumnNames, format); err != nil {
			http.Error(w, fmt.Sprintf("format error: %v", err), http.StatusInternalServerError)
		}
	} else {
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintln(w, result.Message)
	}
}

// HandlePing responds with "Ok." for health checks.
func (h *QueryHandler) HandlePing(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintln(w, "Ok.")
}
