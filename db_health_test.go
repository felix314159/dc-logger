package main

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestOpenAndInitDB_CreatesParentDirectory(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "database", "database.db")

	db, err := openAndInitDB(dbPath)
	if err != nil {
		t.Fatalf("openAndInitDB failed: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	info, err := os.Stat(dbPath)
	if err != nil {
		t.Fatalf("expected db file to exist at %q: %v", dbPath, err)
	}
	if info.IsDir() {
		t.Fatalf("expected db path %q to be a file, got directory", dbPath)
	}
}

func TestStartDatabaseFileMonitor_ReportsDeletion(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "database.db")
	db, err := openAndInitDB(dbPath)
	if err != nil {
		t.Fatalf("openAndInitDB failed: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	missing := make(chan error, 1)
	stop := startDatabaseFileMonitor(dbPath, 5*time.Millisecond, func(err error) {
		missing <- err
	})
	t.Cleanup(stop)

	if err := os.Remove(dbPath); err != nil {
		if runtime.GOOS == "windows" {
			t.Skipf("windows does not allow unlinking open sqlite file handles: %v", err)
		}
		t.Fatalf("remove db file failed: %v", err)
	}

	select {
	case err := <-missing:
		if err == nil {
			t.Fatal("expected non-nil monitor error")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected db monitor to report file deletion")
	}
}

func TestEnsureDatabaseFilePresent_SkipsMemoryDSN(t *testing.T) {
	if err := ensureDatabaseFilePresent(":memory:"); err != nil {
		t.Fatalf("expected :memory: db path to be ignored, got %v", err)
	}
}
