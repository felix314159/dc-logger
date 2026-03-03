package main

import (
	"path/filepath"
	"testing"
	"time"
)

func insertTestMessageForFTS(t *testing.T, dbPath string, messageID string) {
	t.Helper()

	db, err := openAndInitDB(dbPath)
	if err != nil {
		t.Fatalf("openAndInitDB failed: %v", err)
	}
	defer db.Close()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.Exec(
		insertMessageQuery,
		messageID,
		"guild-1",
		"channel-1",
		"user-1",
		now,
		"hello "+messageID,
		"",
		"",
		"",
		"",
		"",
	); err != nil {
		t.Fatalf("insert message failed: %v", err)
	}
}

func countFTSRowsByPrefix(t *testing.T, dbPath string, prefix string) int {
	t.Helper()

	db, err := openAndInitDB(dbPath)
	if err != nil {
		t.Fatalf("openAndInitDB failed: %v", err)
	}
	defer db.Close()

	var n int
	if err := db.QueryRow(
		`SELECT count(*) FROM message_search_fts WHERE row_key LIKE ?;`,
		prefix+"%",
	).Scan(&n); err != nil {
		t.Fatalf("count fts rows failed: %v", err)
	}
	return n
}

func execAgainstDB(t *testing.T, dbPath string, query string, args ...any) {
	t.Helper()
	db, err := openAndInitDB(dbPath)
	if err != nil {
		t.Fatalf("openAndInitDB failed: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(query, args...); err != nil {
		t.Fatalf("exec failed: %v", err)
	}
}

func TestOpenAndInitDB_FTSBootstrapWhenFTSIsEmpty(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "database.db")
	insertTestMessageForFTS(t, dbPath, "m1")
	insertTestMessageForFTS(t, dbPath, "m2")

	execAgainstDB(t, dbPath, `DELETE FROM message_search_fts;`)
	if got := countFTSRowsByPrefix(t, dbPath, "m:"); got != 2 {
		t.Fatalf("expected startup to bootstrap FTS rows when table is empty, got %d", got)
	}
}

func TestOpenAndInitDB_SkipsFullFTSBackfillWhenFTSAlreadyPopulated(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "database.db")
	insertTestMessageForFTS(t, dbPath, "m1")
	insertTestMessageForFTS(t, dbPath, "m2")

	execAgainstDB(t, dbPath, `DELETE FROM message_search_fts WHERE row_key = ?;`, "m:m2")
	if got := countFTSRowsByPrefix(t, dbPath, "m:"); got != 1 {
		t.Fatalf("expected FTS rows to remain partially populated after startup, got %d", got)
	}
}
