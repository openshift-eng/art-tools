package main

// Source: https://github.com/psturc/go-coverage-http
//
// NOTE: This file is injected into arbitrary Go "package main" directories by
// doozer's coverage instrumentation.  All imports are aliased with a "_cov"
// prefix and all top-level identifiers use a "_cov" prefix to avoid name
// collisions with identifiers declared by the host package (e.g. many
// projects declare ``var log = …`` at the package level).

import (
	_covBytes "bytes"
	_covBase64 "encoding/base64"
	_covJSON "encoding/json"
	_covFmt "fmt"
	_covLog "log"
	_covHTTP "net/http"
	_covOS "os"
	_covRuntime "runtime/coverage"
	_covTime "time"
)

// _covResponse represents the JSON response from the coverage endpoint
type _covResponse struct {
	MetaFilename     string `json:"meta_filename"`
	MetaData         string `json:"meta_data"` // base64 encoded
	CountersFilename string `json:"counters_filename"`
	CountersData     string `json:"counters_data"` // base64 encoded
	Timestamp        int64  `json:"timestamp"`
}

func init() {
	// Start coverage server in a separate goroutine
	go _covStartServer()
}

// _covStartServer starts a dedicated HTTP server for coverage collection
func _covStartServer() {
	// Get coverage port from environment variable, default to 9095
	coveragePort := _covOS.Getenv("COVERAGE_PORT")
	if coveragePort == "" {
		coveragePort = "9095"
	}

	// Create a new ServeMux for the coverage server (isolated from main app)
	mux := _covHTTP.NewServeMux()
	mux.HandleFunc("/coverage", _covHandler)
	mux.HandleFunc("/health", func(w _covHTTP.ResponseWriter, r *_covHTTP.Request) {
		w.WriteHeader(_covHTTP.StatusOK)
		_covFmt.Fprintf(w, "coverage server healthy")
	})

	addr := ":" + coveragePort
	_covLog.Printf("[COVERAGE] Starting coverage server on %s", addr)
	_covLog.Printf("[COVERAGE] Endpoints: GET %s/coverage, GET %s/health", addr, addr)

	// Start the server (this will block, but we're in a goroutine)
	if err := _covHTTP.ListenAndServe(addr, mux); err != nil {
		_covLog.Printf("[COVERAGE] ERROR: Coverage server failed: %v", err)
	}
}

// _covHandler collects coverage data and returns it via HTTP as JSON
func _covHandler(w _covHTTP.ResponseWriter, r *_covHTTP.Request) {
	_covLog.Println("[COVERAGE] Collecting coverage data...")

	// Collect metadata
	var metaBuf _covBytes.Buffer
	if err := _covRuntime.WriteMeta(&metaBuf); err != nil {
		_covHTTP.Error(w, _covFmt.Sprintf("Failed to collect metadata: %v", err), _covHTTP.StatusInternalServerError)
		return
	}
	metaData := metaBuf.Bytes()

	// Collect counters
	var counterBuf _covBytes.Buffer
	if err := _covRuntime.WriteCounters(&counterBuf); err != nil {
		_covHTTP.Error(w, _covFmt.Sprintf("Failed to collect counters: %v", err), _covHTTP.StatusInternalServerError)
		return
	}
	counterData := counterBuf.Bytes()

	// Extract hash from metadata to create proper filenames
	var hash string
	if len(metaData) >= 32 {
		hashBytes := metaData[16:32]
		hash = _covFmt.Sprintf("%x", hashBytes)
	} else {
		hash = "unknown"
	}

	// Generate proper filenames
	timestamp := _covTime.Now().UnixNano()
	metaFilename := _covFmt.Sprintf("covmeta.%s", hash)
	counterFilename := _covFmt.Sprintf("covcounters.%s.%d.%d", hash, _covOS.Getpid(), timestamp)

	_covLog.Printf("[COVERAGE] Collected %d bytes metadata, %d bytes counters",
		len(metaData), len(counterData))

	// Return coverage data as JSON
	response := _covResponse{
		MetaFilename:     metaFilename,
		MetaData:         _covBase64.StdEncoding.EncodeToString(metaData),
		CountersFilename: counterFilename,
		CountersData:     _covBase64.StdEncoding.EncodeToString(counterData),
		Timestamp:        timestamp,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := _covJSON.NewEncoder(w).Encode(response); err != nil {
		_covLog.Printf("[COVERAGE] Error encoding response: %v", err)
		_covHTTP.Error(w, "Failed to encode response", _covHTTP.StatusInternalServerError)
		return
	}

	_covLog.Println("[COVERAGE] Coverage data sent successfully")
}
