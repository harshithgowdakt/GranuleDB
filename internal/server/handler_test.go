package server_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/harshithgowdakt/granuledb/internal/server"
	"github.com/harshithgowdakt/granuledb/internal/storage"
)

func setupTestServer(t *testing.T) *server.QueryHandler {
	t.Helper()
	dir, err := os.MkdirTemp("", "granuledb-http-test-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	db, err := storage.NewDatabase(dir)
	if err != nil {
		t.Fatal(err)
	}
	return server.NewQueryHandler(db)
}

func doQuery(t *testing.T, handler *server.QueryHandler, query string) string {
	t.Helper()
	req := httptest.NewRequest("POST", "/", strings.NewReader(query))
	w := httptest.NewRecorder()
	handler.HandleQuery(w, req)
	resp := w.Result()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("HTTP %d for query %q: %s", resp.StatusCode, query, string(body))
	}
	return string(body)
}

func TestHTTPCreateInsertSelect(t *testing.T) {
	h := setupTestServer(t)

	// Create
	resp := doQuery(t, h, `CREATE TABLE test (id UInt64, name String) ENGINE = MergeTree() ORDER BY (id)`)
	if !strings.Contains(resp, "OK") {
		t.Fatalf("expected OK, got %s", resp)
	}

	// Insert
	resp = doQuery(t, h, `INSERT INTO test VALUES (1, 'alice'), (2, 'bob')`)
	if !strings.Contains(resp, "2 rows") {
		t.Fatalf("expected 2 rows, got %s", resp)
	}

	// Select
	resp = doQuery(t, h, `SELECT * FROM test ORDER BY id`)
	if !strings.Contains(resp, "alice") || !strings.Contains(resp, "bob") {
		t.Fatalf("expected alice and bob in result, got:\n%s", resp)
	}
}

func TestHTTPPing(t *testing.T) {
	h := setupTestServer(t)
	req := httptest.NewRequest("GET", "/ping", nil)
	w := httptest.NewRecorder()
	h.HandlePing(w, req)
	resp := w.Result()
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "Ok.") {
		t.Fatalf("expected Ok., got %s", string(body))
	}
}

func TestHTTPJSONFormat(t *testing.T) {
	h := setupTestServer(t)

	doQuery(t, h, `CREATE TABLE test (id UInt64) ENGINE = MergeTree() ORDER BY (id)`)
	doQuery(t, h, `INSERT INTO test VALUES (1), (2), (3)`)

	// Select with JSON format
	req := httptest.NewRequest("POST", "/?format=JSON", strings.NewReader(`SELECT * FROM test`))
	w := httptest.NewRecorder()
	h.HandleQuery(w, req)
	resp := w.Result()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("HTTP %d: %s", resp.StatusCode, string(body))
	}
	if !strings.Contains(string(body), `"rows"`) {
		t.Fatalf("expected JSON with rows field, got:\n%s", string(body))
	}
}

func TestHTTPQueryParam(t *testing.T) {
	h := setupTestServer(t)

	doQuery(t, h, `CREATE TABLE test (id UInt64) ENGINE = MergeTree() ORDER BY (id)`)
	doQuery(t, h, `INSERT INTO test VALUES (42)`)

	// Query via URL parameter
	req := httptest.NewRequest("GET", "/?query=SELECT+*+FROM+test", nil)
	w := httptest.NewRecorder()
	h.HandleQuery(w, req)
	resp := w.Result()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("HTTP %d: %s", resp.StatusCode, string(body))
	}
	if !strings.Contains(string(body), "42") {
		t.Fatalf("expected 42 in result, got:\n%s", string(body))
	}
}
