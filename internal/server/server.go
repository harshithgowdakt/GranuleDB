package server

import (
	"context"
	"fmt"
	"log"
	"net/http"

	"github.com/harshithgowdakt/granuledb/internal/merge"
	"github.com/harshithgowdakt/granuledb/internal/storage"
)

// Server is the GooseDB HTTP server.
type Server struct {
	db      *storage.Database
	merger  *merge.BackgroundMerger
	addr    string
	handler *QueryHandler
}

// NewServer creates a new server.
func NewServer(db *storage.Database, addr string) *Server {
	return &Server{
		db:      db,
		merger:  merge.NewBackgroundMerger(db),
		addr:    addr,
		handler: NewQueryHandler(db),
	}
}

// Start starts the HTTP server and background merger.
func (s *Server) Start(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handler.HandleQuery)
	mux.HandleFunc("/ping", s.handler.HandlePing)

	// Start background merger
	go s.merger.Run(ctx)

	srv := &http.Server{
		Addr:    s.addr,
		Handler: mux,
	}

	// Graceful shutdown
	go func() {
		<-ctx.Done()
		srv.Shutdown(context.Background())
	}()

	log.Printf("GooseDB server listening on %s", s.addr)
	fmt.Printf("GooseDB server listening on %s\n", s.addr)
	return srv.ListenAndServe()
}
