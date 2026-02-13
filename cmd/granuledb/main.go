package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/harshithgowdakt/granuledb/internal/engine"
	"github.com/harshithgowdakt/granuledb/internal/parser"
	"github.com/harshithgowdakt/granuledb/internal/processor"
	"github.com/harshithgowdakt/granuledb/internal/server"
	"github.com/harshithgowdakt/granuledb/internal/storage"
)

func init() {
	// Wire push-based processor pipeline for SELECT queries.
	engine.SelectExecutor = func(stmt *parser.SelectStmt, db *storage.Database) (*engine.ExecuteResult, error) {
		result, err := processor.BuildPipeline(stmt, db)
		if err != nil {
			return nil, err
		}
		exec := processor.NewPipelineExecutor(result.Graph, 0)
		if err := exec.Execute(); err != nil {
			return nil, err
		}
		blocks := result.Output.ResultBlocks()
		outNames := result.OutNames
		if len(blocks) > 0 && blocks[0].NumColumns() > 0 && len(outNames) != blocks[0].NumColumns() {
			outNames = blocks[0].ColumnNames
		}
		return &engine.ExecuteResult{Blocks: blocks, ColumnNames: outNames}, nil
	}
}

func main() {
	dataDir := flag.String("data-dir", "./granuledb-data", "Data directory path")
	addr := flag.String("addr", ":8123", "HTTP server address")
	flag.Parse()

	// Create database
	db, err := storage.NewDatabase(*dataDir)
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}

	fmt.Printf("granuledb - A simplified ClickHouse-like columnar database\n")
	fmt.Printf("Data directory: %s\n", *dataDir)

	// Setup graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\nShutting down...")
		cancel()
	}()

	// Start server
	srv := server.NewServer(db, *addr)
	if err := srv.Start(ctx); err != nil {
		if err.Error() != "http: Server closed" {
			log.Fatalf("Server error: %v", err)
		}
	}
}
