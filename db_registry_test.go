package main

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
)

func TestGuildDatabaseRegistry_StoresGuildsSeparately(t *testing.T) {
	registry := newGuildDatabaseRegistry(filepath.Join(t.TempDir(), "database.db"))
	t.Cleanup(registry.close)

	alpha, err := registry.openGuild(nil, &discordgo.Guild{ID: "guild-alpha", Name: "Alpha Server"})
	if err != nil {
		t.Fatalf("open alpha guild database failed: %v", err)
	}
	beta, err := registry.openGuild(nil, &discordgo.Guild{ID: "guild-beta", Name: "Beta Server"})
	if err != nil {
		t.Fatalf("open beta guild database failed: %v", err)
	}

	handleMessageCreate(nil, alpha.db, alpha.stmts, &discordgo.MessageCreate{
		Message: &discordgo.Message{
			ID:        "1001",
			GuildID:   "guild-alpha",
			ChannelID: "channel-alpha",
			Content:   "alpha",
			Timestamp: time.Now().UTC(),
			Author:    &discordgo.User{ID: "user-alpha", Username: "alice"},
		},
	})
	handleMessageCreate(nil, beta.db, beta.stmts, &discordgo.MessageCreate{
		Message: &discordgo.Message{
			ID:        "2001",
			GuildID:   "guild-beta",
			ChannelID: "channel-beta",
			Content:   "beta",
			Timestamp: time.Now().UTC(),
			Author:    &discordgo.User{ID: "user-beta", Username: "bob"},
		},
	})

	if got := filepath.Base(alpha.path); got != "db_Alpha_Server_guild-alpha.db" {
		t.Fatalf("unexpected alpha database filename: got %q", got)
	}
	if got := filepath.Base(beta.path); got != "db_Beta_Server_guild-beta.db" {
		t.Fatalf("unexpected beta database filename: got %q", got)
	}
	if got := mustCount(t, alpha.db, countMessagesByGuildIDQuery, "guild-alpha"); got != 1 {
		t.Fatalf("unexpected alpha message count in alpha db: got %d want 1", got)
	}
	if got := mustCount(t, alpha.db, countMessagesByGuildIDQuery, "guild-beta"); got != 0 {
		t.Fatalf("unexpected beta message count in alpha db: got %d want 0", got)
	}
	if got := mustCount(t, beta.db, countMessagesByGuildIDQuery, "guild-beta"); got != 1 {
		t.Fatalf("unexpected beta message count in beta db: got %d want 1", got)
	}
	if got := mustCount(t, beta.db, countMessagesByGuildIDQuery, "guild-alpha"); got != 0 {
		t.Fatalf("unexpected alpha message count in beta db: got %d want 0", got)
	}
}

func TestGuildDatabaseRegistry_ReusesExistingDatabaseAfterGuildRename(t *testing.T) {
	basePath := filepath.Join(t.TempDir(), "database.db")
	registry := newGuildDatabaseRegistry(basePath)

	original, err := registry.openGuild(nil, &discordgo.Guild{ID: "guild-rename", Name: "Old Server"})
	if err != nil {
		t.Fatalf("open original guild database failed: %v", err)
	}
	handleMessageCreate(nil, original.db, original.stmts, &discordgo.MessageCreate{
		Message: &discordgo.Message{
			ID:        "1001",
			GuildID:   "guild-rename",
			ChannelID: "channel-rename",
			Content:   "before rename",
			Timestamp: time.Now().UTC(),
			Author:    &discordgo.User{ID: "user-rename", Username: "alice"},
		},
	})
	originalPath := original.path
	registry.close()

	renamedRegistry := newGuildDatabaseRegistry(basePath)
	t.Cleanup(renamedRegistry.close)
	renamed, err := renamedRegistry.openGuild(nil, &discordgo.Guild{ID: "guild-rename", Name: "New Server"})
	if err != nil {
		t.Fatalf("open renamed guild database failed: %v", err)
	}

	if renamed.path != originalPath {
		t.Fatalf("registry opened a different database after rename: got %q want %q", renamed.path, originalPath)
	}
	if got := filepath.Base(renamed.path); got != "db_Old_Server_guild-rename.db" {
		t.Fatalf("unexpected renamed database filename: got %q", got)
	}
	if got := mustCount(t, renamed.db, countMessagesByGuildIDQuery, "guild-rename"); got != 1 {
		t.Fatalf("renamed guild history count mismatch: got %d want 1", got)
	}
}
