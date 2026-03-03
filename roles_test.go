package main

import (
	"testing"

	"github.com/bwmarrin/discordgo"
)

func TestSyncMemberRolesFromGuildAPI_PaginatesAndPersists(t *testing.T) {
	db, stmts := openTestDB(t)
	_ = db

	prevFetcher := guildMembersFetcher
	t.Cleanup(func() { guildMembersFetcher = prevFetcher })

	guildMembersFetcher = func(
		s *discordgo.Session,
		guildID, after string,
		limit int,
	) ([]*discordgo.Member, error) {
		switch after {
		case "":
			return []*discordgo.Member{
				{
					GuildID: guildID,
					User: &discordgo.User{
						ID:            "u1",
						Username:      "alice",
						Discriminator: "0001",
					},
					Roles: []string{"r1", "r2"},
				},
			}, nil
		case "u1":
			return []*discordgo.Member{
				{
					GuildID: guildID,
					User: &discordgo.User{
						ID:            "u2",
						Username:      "bob",
						Discriminator: "0002",
					},
					Roles: []string{},
				},
				{
					GuildID: guildID,
					User: &discordgo.User{
						ID:            "bot1",
						Username:      "bot",
						Discriminator: "0000",
						Bot:           true,
					},
					Roles: []string{"r3"},
				},
			}, nil
		default:
			return []*discordgo.Member{}, nil
		}
	}

	if err := syncMemberRolesFromGuildAPI(&discordgo.Session{}, stmts, "g1", "2026-02-20T00:00:00Z"); err != nil {
		t.Fatalf("syncMemberRolesFromGuildAPI failed: %v", err)
	}

	if got := mustCount(t, db, countGuildMembersByGuildUserQuery, "g1", "u1"); got != 1 {
		t.Fatalf("expected guild member row for u1, got %d", got)
	}
	if got := mustCount(t, db, countGuildMembersByGuildUserQuery, "g1", "u2"); got != 1 {
		t.Fatalf("expected guild member row for u2, got %d", got)
	}
	if got := mustCount(t, db, countGuildMembersByGuildUserQuery, "g1", "bot1"); got != 0 {
		t.Fatalf("expected no guild member row for bot user, got %d", got)
	}

	if got := mustCount(t, db, countMemberRolesByGuildUserRoleQuery, "g1", "u1", "r1"); got != 1 {
		t.Fatalf("expected member role row g1/u1/r1, got %d", got)
	}
	if got := mustCount(t, db, countMemberRolesByGuildUserRoleQuery, "g1", "u1", "r2"); got != 1 {
		t.Fatalf("expected member role row g1/u1/r2, got %d", got)
	}
	if got := mustCount(t, db, countMemberRolesByGuildUserRoleQuery, "g1", "u2", "r1"); got != 0 {
		t.Fatalf("expected no role row for g1/u2/r1, got %d", got)
	}
}
