// Package main defines startup backfill budgeting and runtime metrics helpers.
package main

import (
	"log"
	"strconv"
	"sync/atomic"
	"time"

	"example.org/dc-logger/internal/config"
	"github.com/bwmarrin/discordgo"
)

type backfillConfig struct {
	maxPagesPerRun       int
	maxDuration          time.Duration
	archivedDiscoveryTTL time.Duration
}

func loadBackfillConfig() backfillConfig {
	cfg := backfillConfig{
		maxPagesPerRun: parseNonNegativeIntEnv(config.EnvDiscordBackfillMaxPagesPerRun, config.DefaultBackfillMaxPagesPerRun),
		maxDuration: time.Duration(
			parseNonNegativeIntEnv(config.EnvDiscordBackfillMaxMinutes, config.DefaultBackfillMaxMinutes),
		) * time.Minute,
		archivedDiscoveryTTL: time.Duration(
			parseNonNegativeIntEnv(config.EnvDiscordArchivedDiscoveryTTLHours, config.DefaultArchivedDiscoveryTTLHours),
		) * time.Hour,
	}
	return cfg
}

func parseNonNegativeIntEnv(key string, def int) int {
	raw := getenvDefault(key, strconv.Itoa(def))
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		log.Printf("invalid %s=%q; using default=%d", key, raw, def)
		return def
	}
	return n
}

type backfillRun struct {
	cfg          backfillConfig
	metrics      *backfillMetrics
	startedAt    time.Time
	deadline     time.Time
	pagesUsed    int
	budgetReason string
}

func newBackfillRun(cfg backfillConfig, metrics *backfillMetrics) *backfillRun {
	now := time.Now().UTC()
	run := &backfillRun{
		cfg:       cfg,
		metrics:   metrics,
		startedAt: now,
	}
	if cfg.maxDuration > 0 {
		run.deadline = now.Add(cfg.maxDuration)
	}
	return run
}

func (r *backfillRun) noteRequest() {
	if r == nil || r.metrics == nil {
		return
	}
	r.metrics.addRequests(1)
}

func (r *backfillRun) consumeBackfillPage() bool {
	if r == nil {
		return true
	}
	if r.cfg.maxPagesPerRun > 0 && r.pagesUsed >= r.cfg.maxPagesPerRun {
		r.budgetReason = "max_pages_per_run"
		return false
	}
	if !r.deadline.IsZero() && time.Now().UTC().After(r.deadline) {
		r.budgetReason = "max_backfill_minutes"
		return false
	}
	r.pagesUsed++
	r.noteRequest()
	return true
}

func (r *backfillRun) budgetReached() bool {
	return r != nil && r.budgetReason != ""
}

func (r *backfillRun) reason() string {
	if r == nil {
		return ""
	}
	return r.budgetReason
}

func (r *backfillRun) usedPages() int {
	if r == nil {
		return 0
	}
	return r.pagesUsed
}

type backfillMetrics struct {
	startedAt        time.Time
	requestCount     atomic.Int64
	rateLimitCount   atomic.Int64
	retryAfterNanos  atomic.Int64
	channelsTotal    atomic.Int64
	channelsDone     atomic.Int64
	messagesInserted atomic.Int64
}

type backfillMetricsSnapshot struct {
	RequestCount      int64
	RateLimitCount    int64
	RetryAfterTotal   time.Duration
	ChannelsTotal     int64
	ChannelsDone      int64
	ChannelsRemaining int64
	MessagesInserted  int64
	Elapsed           time.Duration
}

func newBackfillMetrics() *backfillMetrics {
	return &backfillMetrics{startedAt: time.Now().UTC()}
}

func (m *backfillMetrics) addRequests(n int64) {
	if m == nil || n == 0 {
		return
	}
	m.requestCount.Add(n)
}

func (m *backfillMetrics) addChannelsTotal(n int64) {
	if m == nil || n == 0 {
		return
	}
	m.channelsTotal.Add(n)
}

func (m *backfillMetrics) addChannelsDone(n int64) {
	if m == nil || n == 0 {
		return
	}
	m.channelsDone.Add(n)
}

func (m *backfillMetrics) addMessagesInserted(n int64) {
	if m == nil || n == 0 {
		return
	}
	m.messagesInserted.Add(n)
}

func (m *backfillMetrics) noteRateLimit(rl *discordgo.RateLimit) {
	if m == nil {
		return
	}
	m.rateLimitCount.Add(1)
	if rl != nil && rl.TooManyRequests != nil {
		m.retryAfterNanos.Add(int64(rl.RetryAfter))
	}
}

func (m *backfillMetrics) snapshot() backfillMetricsSnapshot {
	if m == nil {
		return backfillMetricsSnapshot{}
	}
	total := m.channelsTotal.Load()
	done := m.channelsDone.Load()
	remaining := total - done
	if remaining < 0 {
		remaining = 0
	}
	return backfillMetricsSnapshot{
		RequestCount:      m.requestCount.Load(),
		RateLimitCount:    m.rateLimitCount.Load(),
		RetryAfterTotal:   time.Duration(m.retryAfterNanos.Load()),
		ChannelsTotal:     total,
		ChannelsDone:      done,
		ChannelsRemaining: remaining,
		MessagesInserted:  m.messagesInserted.Load(),
		Elapsed:           time.Since(m.startedAt),
	}
}
