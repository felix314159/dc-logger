package main

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bwmarrin/discordgo"
)

var guildDBFilenameUnsafePattern = regexp.MustCompile(`[^A-Za-z0-9._-]+`)

type guildDatabase struct {
	guildID   string
	guildName string
	path      string
	db        *sql.DB
	stmts     *preparedStatements
}

type guildDatabaseRegistry struct {
	basePath string

	mu          sync.RWMutex
	entries     map[string]*guildDatabase
	monitorStop func()
}

func newGuildDatabaseRegistry(basePath string) *guildDatabaseRegistry {
	return &guildDatabaseRegistry{
		basePath: basePath,
		entries:  make(map[string]*guildDatabase),
	}
}

func (r *guildDatabaseRegistry) openGuild(s *discordgo.Session, g *discordgo.Guild) (*guildDatabase, error) {
	if r == nil || g == nil || strings.TrimSpace(g.ID) == "" {
		return nil, fmt.Errorf("missing guild")
	}
	guildID := strings.TrimSpace(g.ID)

	r.mu.RLock()
	existing := r.entries[guildID]
	r.mu.RUnlock()
	if existing != nil {
		return existing, nil
	}

	guildName := resolveGuildDisplayName(s, nil, g)
	path := guildDatabasePath(r.basePath, guildName, guildID)
	if existingPath, err := existingGuildDatabasePath(r.basePath, guildID, path); err != nil {
		return nil, err
	} else if existingPath != "" {
		path = existingPath
	}
	db, err := openAndInitDB(path)
	if err != nil {
		return nil, err
	}
	stmts, err := prepareStatements(db)
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	entry := &guildDatabase{
		guildID:   guildID,
		guildName: guildName,
		path:      path,
		db:        db,
		stmts:     stmts,
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if existing := r.entries[guildID]; existing != nil {
		closePreparedStatements(stmts)
		_ = db.Close()
		return existing, nil
	}
	r.entries[guildID] = entry
	return entry, nil
}

func (r *guildDatabaseRegistry) get(guildID string) *guildDatabase {
	if r == nil {
		return nil
	}
	guildID = strings.TrimSpace(guildID)
	if guildID == "" {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.entries[guildID]
}

func (r *guildDatabaseRegistry) close() {
	if r == nil {
		return
	}
	r.stopFileMonitor()

	r.mu.Lock()
	defer r.mu.Unlock()
	for guildID, entry := range r.entries {
		if entry == nil {
			continue
		}
		closePreparedStatements(entry.stmts)
		if entry.db != nil {
			if err := entry.db.Close(); err != nil {
				logDBErr("close guild database failed (guild=%s path=%s): %v", guildID, entry.path, err)
			}
		}
	}
	r.entries = make(map[string]*guildDatabase)
}

func (r *guildDatabaseRegistry) startFileMonitor(interval time.Duration, onMissing func(error)) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.monitorStop != nil {
		return
	}

	stops := make([]func(), 0, len(r.entries))
	for _, entry := range r.entries {
		if entry == nil || strings.TrimSpace(entry.path) == "" {
			continue
		}
		dbPath := entry.path
		stops = append(stops, startDatabaseFileMonitor(dbPath, interval, onMissing))
	}
	r.monitorStop = func() {
		for _, stop := range stops {
			if stop != nil {
				stop()
			}
		}
	}
}

func (r *guildDatabaseRegistry) stopFileMonitor() {
	if r == nil {
		return
	}
	r.mu.Lock()
	stop := r.monitorStop
	r.monitorStop = nil
	r.mu.Unlock()
	if stop != nil {
		stop()
	}
}

func guildDatabasePath(basePath, guildName, guildID string) string {
	dir := guildDatabaseDir(basePath)
	name := sanitizeGuildDatabaseName(guildName)
	if name == "" {
		name = "server"
	}
	return filepath.Join(dir, fmt.Sprintf("db_%s_%s.db", name, strings.TrimSpace(guildID)))
}

func guildDatabaseDir(basePath string) string {
	dir := basePath
	if strings.TrimSpace(dir) == "" {
		dir = "."
	}
	if ext := filepath.Ext(dir); ext != "" {
		dir = filepath.Dir(dir)
	}
	return dir
}

func existingGuildDatabasePath(basePath, guildID, preferredPath string) (string, error) {
	guildID = strings.TrimSpace(guildID)
	if guildID == "" {
		return "", nil
	}

	dir := guildDatabaseDir(basePath)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", fmt.Errorf("read database directory %q: %w", dir, err)
	}

	preferredName := filepath.Base(preferredPath)
	suffix := "_" + guildID + ".db"
	matches := make([]string, 0, 1)
	for _, entry := range entries {
		if entry == nil || entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasPrefix(name, "db_") || !strings.HasSuffix(name, suffix) {
			continue
		}
		path := filepath.Join(dir, name)
		if name == preferredName {
			return path, nil
		}
		matches = append(matches, path)
	}
	if len(matches) == 0 {
		return "", nil
	}
	sort.Strings(matches)
	return matches[0], nil
}

func sanitizeGuildDatabaseName(name string) string {
	name = strings.TrimSpace(name)
	name = strings.ReplaceAll(name, " ", "_")
	name = guildDBFilenameUnsafePattern.ReplaceAllString(name, "_")
	name = strings.Trim(name, "._-")
	for strings.Contains(name, "__") {
		name = strings.ReplaceAll(name, "__", "_")
	}
	return name
}
