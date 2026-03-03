// Package main provides database path validation and runtime file-health monitoring.
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// monitorableDBPath returns a filesystem path when the SQLite DSN is a regular file path.
// For special DSNs (for example :memory: or file: URIs), monitoring is skipped.
func monitorableDBPath(raw string) (string, bool) {
	path := strings.TrimSpace(raw)
	if path == "" {
		return "", false
	}
	lower := strings.ToLower(path)
	if lower == ":memory:" || strings.HasPrefix(lower, "file:") {
		return "", false
	}
	return filepath.Clean(path), true
}

func ensureDatabaseParentDir(dbPath string) error {
	path, ok := monitorableDBPath(dbPath)
	if !ok {
		return nil
	}
	dir := filepath.Dir(path)
	if dir == "." || dir == "" {
		return nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create database directory %q: %w", dir, err)
	}
	return nil
}

func ensureDatabaseFilePresent(dbPath string) error {
	path, ok := monitorableDBPath(dbPath)
	if !ok {
		return nil
	}
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("database file %q is not accessible: %w", path, err)
	}
	if info.IsDir() {
		return fmt.Errorf("database path %q is a directory", path)
	}
	return nil
}

func startDatabaseFileMonitor(dbPath string, interval time.Duration, onMissing func(error)) func() {
	if _, ok := monitorableDBPath(dbPath); !ok {
		return func() {}
	}
	if interval <= 0 {
		interval = time.Second
	}

	done := make(chan struct{})
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				if err := ensureDatabaseFilePresent(dbPath); err != nil {
					if onMissing != nil {
						onMissing(err)
					}
					return
				}
			}
		}
	}()

	return func() {
		close(done)
	}
}
