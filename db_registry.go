package main

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"regexp"
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
	dir := basePath
	if strings.TrimSpace(dir) == "" {
		dir = "."
	}
	if ext := filepath.Ext(dir); ext != "" {
		dir = filepath.Dir(dir)
	}
	name := sanitizeGuildDatabaseName(guildName)
	if name == "" {
		name = "server"
	}
	return filepath.Join(dir, fmt.Sprintf("db_%s_%s.db", name, strings.TrimSpace(guildID)))
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
