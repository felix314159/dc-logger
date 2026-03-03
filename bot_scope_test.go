package main

import (
	"net/http"
	"testing"

	"example.org/dc-logger/internal/config"
	"github.com/bwmarrin/discordgo"
)

func TestParseGuildSyncFilter_MultiGuild(t *testing.T) {
	filter := parseGuildSyncFilter("g1, g2, g1")
	if filter.allowAll {
		t.Fatal("expected allowAll=false")
	}
	if !filter.allows("g1") || !filter.allows("g2") {
		t.Fatal("expected g1 and g2 to be allowed")
	}
	if filter.allows("g3") {
		t.Fatal("expected g3 to be disallowed")
	}
}

func TestParseGuildSyncFilter_Wildcard(t *testing.T) {
	filter := parseGuildSyncFilter(" * ")
	if !filter.allowAll {
		t.Fatal("expected allowAll=true")
	}
	if !filter.allows("any-guild") {
		t.Fatal("expected any guild to be allowed")
	}
}

func TestLoadGuildSyncFilter_Default(t *testing.T) {
	t.Setenv(config.EnvDiscordSyncGuildIDs, "")
	filter := loadGuildSyncFilter()
	if !filter.allowAll {
		t.Fatal("expected default filter to allow all guilds")
	}
	if !filter.allows("some-other-guild") {
		t.Fatal("expected unrelated guild to be allowed by default")
	}
}

func TestLoadGuildSyncFilter_Override(t *testing.T) {
	t.Setenv(config.EnvDiscordSyncGuildIDs, "guild-x")
	filter := loadGuildSyncFilter()
	if !filter.allows("guild-x") {
		t.Fatal("expected override guild to be allowed")
	}
	if filter.allows("guild-y") {
		t.Fatal("expected non-configured guild to be disallowed when override is set")
	}
}

func TestSuggestedGuildIDEnvValue(t *testing.T) {
	got := suggestedGuildIDEnvValue([]*discordgo.Guild{
		{ID: "2"},
		nil,
		{ID: "1"},
		{ID: "2"},
		{ID: ""},
	})
	if got != "1,2" {
		t.Fatalf("suggestedGuildIDEnvValue mismatch: got %q want %q", got, "1,2")
	}
}

func TestDescribeTrackedReadyGuilds(t *testing.T) {
	got := describeTrackedReadyGuilds(nil, nil, []*discordgo.Guild{
		{ID: "2", Name: "Beta"},
		{ID: "1", Name: "Alpha"},
		{ID: "3", Name: ""},
	})
	want := "3 (3), Alpha (1), Beta (2)"
	if got != want {
		t.Fatalf("describeTrackedReadyGuilds mismatch: got %q want %q", got, want)
	}
}

func TestDescribeTrackedReadyGuilds_UsesMappedGuildNameFallback(t *testing.T) {
	db, stmts := openTestDB(t)

	now := "2026-02-20T00:00:00Z"
	if err := upsertNameMappingRow(stmts.upsertIDNameMapping, nameMappingEntityGuild, "111111111111111111", "111111111111111111", "LOREM", now); err != nil {
		t.Fatalf("seed guild mapping failed: %v", err)
	}

	got := describeTrackedReadyGuilds(nil, db, []*discordgo.Guild{
		{ID: "111111111111111111", Name: ""},
	})
	want := "LOREM (111111111111111111)"
	if got != want {
		t.Fatalf("describeTrackedReadyGuilds fallback mismatch: got %q want %q", got, want)
	}
}

func TestResolveGuildDisplayName_UsesGuildDetailsFetcherFallback(t *testing.T) {
	prevFetcher := guildDetailsFetcher
	t.Cleanup(func() { guildDetailsFetcher = prevFetcher })

	guildDetailsFetcher = func(s *discordgo.Session, guildID string) (*discordgo.Guild, error) {
		return &discordgo.Guild{ID: guildID, Name: "LOREM"}, nil
	}

	got := resolveGuildDisplayName(nil, nil, &discordgo.Guild{ID: "111111111111111111", Name: ""})
	if got != "LOREM" {
		t.Fatalf("resolveGuildDisplayName mismatch: got %q want %q", got, "LOREM")
	}
}

func TestPreflightGuildReadAccess_PassesWhenAccessible(t *testing.T) {
	prevFetcher := channelMessagesFetcher
	t.Cleanup(func() { channelMessagesFetcher = prevFetcher })

	channelMessagesFetcher = func(
		s *discordgo.Session,
		channelID string,
		limit int,
		beforeID, afterID, aroundID string,
	) ([]*discordgo.Message, error) {
		return []*discordgo.Message{}, nil
	}

	ok := preflightGuildReadAccess(
		nil,
		nil,
		[]*discordgo.Guild{{ID: "g1", Name: "GuildOne"}},
		map[string][]*discordgo.Channel{
			"g1": {
				{ID: "c1", Name: "general", Type: discordgo.ChannelTypeGuildText},
				{ID: "cat", Name: "General", Type: discordgo.ChannelTypeGuildCategory},
			},
		},
	)
	if !ok {
		t.Fatal("expected preflight to pass")
	}
}

func TestPreflightGuildReadAccess_FailsOnMissingAccess(t *testing.T) {
	prevFetcher := channelMessagesFetcher
	t.Cleanup(func() { channelMessagesFetcher = prevFetcher })

	missingAccessErr := &discordgo.RESTError{
		Response: &http.Response{
			StatusCode: http.StatusForbidden,
			Status:     "403 Forbidden",
		},
		ResponseBody: []byte(`{"message":"Missing Access","code":50001}`),
		Message: &discordgo.APIErrorMessage{
			Code:    discordgo.ErrCodeMissingAccess,
			Message: "Missing Access",
		},
	}

	channelMessagesFetcher = func(
		s *discordgo.Session,
		channelID string,
		limit int,
		beforeID, afterID, aroundID string,
	) ([]*discordgo.Message, error) {
		if channelID == "c-missing" {
			return nil, missingAccessErr
		}
		return []*discordgo.Message{}, nil
	}

	ok := preflightGuildReadAccess(
		nil,
		nil,
		[]*discordgo.Guild{{ID: "g1", Name: "GuildOne"}},
		map[string][]*discordgo.Channel{
			"g1": {
				{ID: "c-ok", Name: "general", Type: discordgo.ChannelTypeGuildText},
				{ID: "c-missing", Name: "announcements", Type: discordgo.ChannelTypeGuildText},
			},
		},
	)
	if ok {
		t.Fatal("expected preflight to fail on missing access")
	}
}
