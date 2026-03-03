// Package main tests startup backfill budget parsing and runtime metrics aggregation.
package main

import (
	"testing"
	"time"

	"example.org/dc-logger/internal/config"
	"github.com/bwmarrin/discordgo"
)

func TestLoadBackfillConfig_ParsesEnv(t *testing.T) {
	t.Setenv(config.EnvDiscordBackfillMaxPagesPerRun, "123")
	t.Setenv(config.EnvDiscordBackfillMaxMinutes, "7")
	t.Setenv(config.EnvDiscordArchivedDiscoveryTTLHours, "12")

	cfg := loadBackfillConfig()
	if cfg.maxPagesPerRun != 123 {
		t.Fatalf("maxPagesPerRun mismatch: got %d want %d", cfg.maxPagesPerRun, 123)
	}
	if cfg.maxDuration != 7*time.Minute {
		t.Fatalf("maxDuration mismatch: got %s want %s", cfg.maxDuration, 7*time.Minute)
	}
	if cfg.archivedDiscoveryTTL != 12*time.Hour {
		t.Fatalf("archivedDiscoveryTTL mismatch: got %s want %s", cfg.archivedDiscoveryTTL, 12*time.Hour)
	}
}

func TestLoadBackfillConfig_InvalidEnvFallsBackToDefault(t *testing.T) {
	t.Setenv(config.EnvDiscordBackfillMaxPagesPerRun, "bad")
	t.Setenv(config.EnvDiscordBackfillMaxMinutes, "-1")
	t.Setenv(config.EnvDiscordArchivedDiscoveryTTLHours, "bad")

	cfg := loadBackfillConfig()
	if cfg.maxPagesPerRun != 0 {
		t.Fatalf("expected default maxPagesPerRun=0, got %d", cfg.maxPagesPerRun)
	}
	if cfg.maxDuration != 0 {
		t.Fatalf("expected default maxDuration=0, got %s", cfg.maxDuration)
	}
	if cfg.archivedDiscoveryTTL != 24*time.Hour {
		t.Fatalf("expected default archivedDiscoveryTTL=24h, got %s", cfg.archivedDiscoveryTTL)
	}
}

func TestBackfillRun_PageBudgetStopsFurtherRequests(t *testing.T) {
	metrics := newBackfillMetrics()
	run := newBackfillRun(backfillConfig{maxPagesPerRun: 2}, metrics)

	if ok := run.consumeBackfillPage(); !ok {
		t.Fatal("first consumeBackfillPage unexpectedly false")
	}
	if ok := run.consumeBackfillPage(); !ok {
		t.Fatal("second consumeBackfillPage unexpectedly false")
	}
	if ok := run.consumeBackfillPage(); ok {
		t.Fatal("third consumeBackfillPage unexpectedly true")
	}
	if !run.budgetReached() {
		t.Fatal("expected budgetReached=true")
	}
	if run.reason() != "max_pages_per_run" {
		t.Fatalf("budget reason mismatch: got %q", run.reason())
	}

	snap := metrics.snapshot()
	if snap.RequestCount != 2 {
		t.Fatalf("request count mismatch: got %d want %d", snap.RequestCount, 2)
	}
}

func TestBackfillRun_DurationBudgetStopsFurtherRequests(t *testing.T) {
	run := newBackfillRun(backfillConfig{maxDuration: time.Minute}, newBackfillMetrics())
	run.deadline = time.Now().UTC().Add(-time.Second)

	if ok := run.consumeBackfillPage(); ok {
		t.Fatal("consumeBackfillPage unexpectedly true after deadline")
	}
	if run.reason() != "max_backfill_minutes" {
		t.Fatalf("budget reason mismatch: got %q", run.reason())
	}
}

func TestBackfillMetrics_RateLimitAggregation(t *testing.T) {
	metrics := newBackfillMetrics()
	metrics.noteRateLimit(&discordgo.RateLimit{
		TooManyRequests: &discordgo.TooManyRequests{
			RetryAfter: 1500 * time.Millisecond,
		},
		URL: "/channels/x/messages",
	})
	metrics.noteRateLimit(&discordgo.RateLimit{
		TooManyRequests: &discordgo.TooManyRequests{
			RetryAfter: 500 * time.Millisecond,
		},
		URL: "/channels/y/messages",
	})
	metrics.addChannelsTotal(10)
	metrics.addChannelsDone(4)

	snap := metrics.snapshot()
	if snap.RateLimitCount != 2 {
		t.Fatalf("rate limit count mismatch: got %d want %d", snap.RateLimitCount, 2)
	}
	if snap.RetryAfterTotal != 2*time.Second {
		t.Fatalf("retry_after_total mismatch: got %s want %s", snap.RetryAfterTotal, 2*time.Second)
	}
	if snap.ChannelsRemaining != 6 {
		t.Fatalf("channels remaining mismatch: got %d want %d", snap.ChannelsRemaining, 6)
	}
}
