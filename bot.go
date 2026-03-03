// Package main contains Discord gateway session setup, event handlers, and shutdown flow.
package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"example.org/dc-logger/internal/config"

	"github.com/bwmarrin/discordgo"
)

const (
	discordSnowflakeEpochMS = int64(1420070400000)
	auditLogFreshnessWindow = 15 * time.Second
	databaseFileMonitorTick = 250 * time.Millisecond
)

var trackedEventLoggingEnabled atomic.Bool

var (
	messageLogUserMentionPattern = regexp.MustCompile(`<@!?([^>\s]+)>`)
	messageLogRoleMentionPattern = regexp.MustCompile(`<@&([^>\s]+)>`)
)

type guildSyncFilter struct {
	allowAll bool
	allowed  map[string]struct{}
	ids      []string
}

func loadGuildSyncFilter() guildSyncFilter {
	raw := strings.TrimSpace(getenvDefault(config.EnvDiscordSyncGuildIDs, config.DefaultSyncGuildIDs))
	filter := parseGuildSyncFilter(raw)
	if filter.allowAll || len(filter.allowed) > 0 {
		return filter
	}
	return parseGuildSyncFilter(config.DefaultSyncGuildIDs)
}

func parseGuildSyncFilter(raw string) guildSyncFilter {
	filter := guildSyncFilter{
		allowed: make(map[string]struct{}),
	}

	for _, part := range strings.Split(raw, ",") {
		guildID := strings.TrimSpace(part)
		if guildID == "" {
			continue
		}
		if guildID == "*" {
			filter.allowAll = true
			filter.allowed = map[string]struct{}{}
			filter.ids = []string{"*"}
			return filter
		}
		if _, exists := filter.allowed[guildID]; exists {
			continue
		}
		filter.allowed[guildID] = struct{}{}
		filter.ids = append(filter.ids, guildID)
	}
	return filter
}

func (f guildSyncFilter) allows(guildID string) bool {
	guildID = strings.TrimSpace(guildID)
	if guildID == "" {
		return false
	}
	if f.allowAll {
		return true
	}
	_, ok := f.allowed[guildID]
	return ok
}

func (f guildSyncFilter) describe() string {
	if f.allowAll {
		return "*"
	}
	if len(f.ids) == 0 {
		return "(none)"
	}
	return strings.Join(f.ids, ",")
}

func resolveGuildDisplayName(s *discordgo.Session, db *sql.DB, g *discordgo.Guild) string {
	if g == nil {
		return ""
	}
	guildID := strings.TrimSpace(g.ID)
	if guildID == "" {
		return ""
	}

	if guildName := strings.TrimSpace(g.Name); guildName != "" {
		return guildName
	}
	if s != nil && s.State != nil {
		if stateGuild, err := s.State.Guild(guildID); err == nil && stateGuild != nil {
			if guildName := strings.TrimSpace(stateGuild.Name); guildName != "" {
				return guildName
			}
		}
	}
	if fetchedGuild, err := guildDetailsFetcher(s, guildID); err == nil && fetchedGuild != nil {
		if guildName := strings.TrimSpace(fetchedGuild.Name); guildName != "" {
			return guildName
		}
	}
	if mapped := currentMappedName(db, nameMappingEntityGuild, guildID, guildID); mapped != "" {
		return mapped
	}
	return guildID
}

func describeTrackedReadyGuilds(s *discordgo.Session, db *sql.DB, guilds []*discordgo.Guild) string {
	if len(guilds) == 0 {
		return "(none)"
	}

	labels := make([]string, 0, len(guilds))
	for _, g := range guilds {
		if g == nil {
			continue
		}
		guildID := strings.TrimSpace(g.ID)
		if guildID == "" {
			continue
		}
		guildName := resolveGuildDisplayName(s, db, g)
		labels = append(labels, fmt.Sprintf("%s (%s)", guildName, guildID))
	}
	if len(labels) == 0 {
		return "(none)"
	}
	sort.Strings(labels)
	return strings.Join(labels, ", ")
}

func suggestedGuildIDEnvValue(guilds []*discordgo.Guild) string {
	if len(guilds) == 0 {
		return ""
	}

	seen := make(map[string]struct{}, len(guilds))
	ids := make([]string, 0, len(guilds))
	for _, g := range guilds {
		if g == nil {
			continue
		}
		guildID := strings.TrimSpace(g.ID)
		if guildID == "" {
			continue
		}
		if _, ok := seen[guildID]; ok {
			continue
		}
		seen[guildID] = struct{}{}
		ids = append(ids, guildID)
	}
	if len(ids) == 0 {
		return ""
	}
	sort.Strings(ids)
	return strings.Join(ids, ",")
}

func channelKindLabel(c *discordgo.Channel) string {
	if c == nil {
		return "unknown"
	}
	switch c.Type {
	case discordgo.ChannelTypeGuildText:
		return "text"
	case discordgo.ChannelTypeGuildNews:
		return "announcement"
	case discordgo.ChannelTypeGuildVoice:
		return "voice"
	case discordgo.ChannelTypeGuildCategory:
		return "category"
	case discordgo.ChannelTypeGuildForum:
		return "forum"
	case discordgo.ChannelTypeGuildMedia:
		return "media"
	case discordgo.ChannelTypeGuildPublicThread:
		return "public_thread"
	case discordgo.ChannelTypeGuildPrivateThread:
		return "private_thread"
	case discordgo.ChannelTypeGuildNewsThread:
		return "news_thread"
	default:
		return fmt.Sprintf("type(%d)", int(c.Type))
	}
}

func channelDisplayName(db *sql.DB, guildID string, c *discordgo.Channel) string {
	if c == nil {
		return ""
	}
	name := strings.TrimSpace(c.Name)
	if name == "" {
		name = currentMappedName(db, nameMappingEntityChannel, c.ID, guildID)
	}
	if name == "" {
		name = c.ID
	}
	return name
}

func preflightChannelPermissionLabel(db *sql.DB, guildID string, c *discordgo.Channel) string {
	if c == nil {
		return "#unknown"
	}
	name := strings.TrimSpace(channelDisplayName(db, guildID, c))
	if name == "" {
		name = strings.TrimSpace(c.ID)
	}
	if name == "" {
		return "#unknown"
	}
	if strings.HasPrefix(name, "#") {
		return name
	}
	return "#" + name
}

func sortChannelsForPreview(db *sql.DB, guildID string, channels []*discordgo.Channel) {
	sort.Slice(channels, func(i, j int) bool {
		ci := channels[i]
		cj := channels[j]
		if ci == nil || cj == nil {
			return ci != nil
		}
		ni := strings.ToLower(channelDisplayName(db, guildID, ci))
		nj := strings.ToLower(channelDisplayName(db, guildID, cj))
		if ni != nj {
			return ni < nj
		}
		return ci.ID < cj.ID
	})
}

func logChannelSyncPreview(s *discordgo.Session, db *sql.DB, guilds []*discordgo.Guild) map[string][]*discordgo.Channel {
	channelsByGuild := make(map[string][]*discordgo.Channel, len(guilds))

	log.Println("detecting accessible channels before sync confirmation")
	for _, g := range guilds {
		if g == nil || strings.TrimSpace(g.ID) == "" {
			continue
		}
		guildName := resolveGuildDisplayName(s, db, g)
		channels, err := guildChannelsFetcher(s, g.ID)
		if err != nil {
			log.Printf("channel discovery failed for guild=%s (%s): %v", guildName, g.ID, err)
			continue
		}
		channelsByGuild[g.ID] = channels

		categoryIDs := make(map[string]struct{})
		var aggregators []*discordgo.Channel
		childByParent := make(map[string][]*discordgo.Channel)
		var ungrouped []*discordgo.Channel

		for _, c := range channels {
			if c == nil || strings.TrimSpace(c.ID) == "" {
				continue
			}
			if c.Type == discordgo.ChannelTypeGuildCategory {
				aggregators = append(aggregators, c)
				categoryIDs[c.ID] = struct{}{}
				continue
			}
			parentID := strings.TrimSpace(c.ParentID)
			if parentID == "" {
				ungrouped = append(ungrouped, c)
				continue
			}
			childByParent[parentID] = append(childByParent[parentID], c)
		}

		sortChannelsForPreview(db, g.ID, aggregators)
		log.Printf("detected channels guild=%s (%s) total=%d aggregators=%d", guildName, g.ID, len(channels), len(aggregators))

		for _, aggregator := range aggregators {
			if aggregator == nil {
				continue
			}
			log.Printf(
				"detected aggregator id=%s name=%s",
				aggregator.ID,
				channelDisplayName(db, g.ID, aggregator),
			)

			children := childByParent[aggregator.ID]
			sortChannelsForPreview(db, g.ID, children)
			for _, c := range children {
				if c == nil {
					continue
				}
				log.Printf(
					"                detected channel id=%s name=%s kind=%s",
					c.ID,
					channelDisplayName(db, g.ID, c),
					channelKindLabel(c),
				)
			}
		}

		for parentID, children := range childByParent {
			if _, isAggregator := categoryIDs[parentID]; isAggregator {
				continue
			}
			ungrouped = append(ungrouped, children...)
		}

		if len(ungrouped) > 0 {
			sortChannelsForPreview(db, g.ID, ungrouped)
			log.Printf("detected ungrouped channels guild=%s (%s) count=%d", guildName, g.ID, len(ungrouped))
			for _, c := range ungrouped {
				if c == nil {
					continue
				}
				parentID := strings.TrimSpace(c.ParentID)
				log.Printf(
					"                detected channel id=%s name=%s kind=%s parent_id=%s",
					c.ID,
					channelDisplayName(db, g.ID, c),
					channelKindLabel(c),
					parentID,
				)
			}
		}
	}
	return channelsByGuild
}

func preflightGuildReadAccess(s *discordgo.Session, db *sql.DB, guilds []*discordgo.Guild, channelsByGuild map[string][]*discordgo.Channel) bool {
	log.Println("running preflight read-access check before sync")
	allClear := true
	type accessFailure struct {
		channel *discordgo.Channel
		err     error
	}

	for _, g := range guilds {
		if g == nil || strings.TrimSpace(g.ID) == "" {
			continue
		}
		guildID := g.ID
		guildName := resolveGuildDisplayName(s, db, g)
		channels := channelsByGuild[guildID]

		var candidates []*discordgo.Channel
		for _, c := range channels {
			if c == nil {
				continue
			}
			if isDirectBackfillChannel(c) {
				candidates = append(candidates, c)
			}
		}
		sortChannelsForPreview(db, guildID, candidates)
		log.Printf("preflight checking channel permissions guild=%s (%s) channels=%d", guildName, guildID, len(candidates))

		var missing []accessFailure
		var hardErrors []string
		for _, c := range candidates {
			if c == nil || strings.TrimSpace(c.ID) == "" {
				continue
			}
			channelLabel := preflightChannelPermissionLabel(db, guildID, c)
			_, err := channelMessagesFetcher(s, c.ID, 1, "", "", "")
			if err == nil {
				log.Printf("have permission to read %s: True", channelLabel)
				continue
			}
			if isDiscordMissingAccessError(err) {
				log.Printf("have permission to read %s: False", channelLabel)
				missing = append(missing, accessFailure{channel: c, err: err})
				continue
			}
			log.Printf("have permission to read %s: False (check error: %v)", channelLabel, err)
			hardErrors = append(hardErrors, fmt.Sprintf("id=%s name=%s err=%v", c.ID, channelDisplayName(db, guildID, c), err))
		}

		log.Printf(
			"preflight summary guild=%s (%s) message_channels=%d accessible=%d missing_access=%d hard_errors=%d",
			guildName,
			guildID,
			len(candidates),
			len(candidates)-len(missing)-len(hardErrors),
			len(missing),
			len(hardErrors),
		)

		for _, miss := range missing {
			c := miss.channel
			if c == nil {
				continue
			}
			log.Printf(
				"preflight missing access channel id=%s name=%s kind=%s check=ChannelMessages(limit=1) error=%v",
				c.ID,
				channelDisplayName(db, guildID, c),
				channelKindLabel(c),
				miss.err,
			)
		}
		for _, msg := range hardErrors {
			log.Printf("preflight channel check error %s", msg)
		}

		if len(missing) > 0 || len(hardErrors) > 0 {
			allClear = false
		}
	}

	if !allClear {
		log.Println("preflight failed: bot can list channels but cannot read messages in one or more channels")
		log.Println("preflight diagnosis: Discord API returned Missing Access on ChannelMessages; fix channel-level permission overwrites (View Channels + Read Message History) and thread visibility")
	}
	return allClear
}

var guildAuditLogFetcher = func(
	s *discordgo.Session,
	guildID, userID string,
	actionType discordgo.AuditLogAction,
	limit int,
) (*discordgo.GuildAuditLog, error) {
	if s == nil || strings.TrimSpace(guildID) == "" || limit <= 0 {
		return nil, nil
	}
	return s.GuildAuditLog(guildID, userID, "", int(actionType), limit)
}

var guildDetailsFetcher = func(s *discordgo.Session, guildID string) (*discordgo.Guild, error) {
	if s == nil || strings.TrimSpace(guildID) == "" {
		return nil, nil
	}
	return s.Guild(guildID)
}

type memberRemovalCause string

const (
	memberRemovalCauseLeft   memberRemovalCause = "left"
	memberRemovalCauseKicked memberRemovalCause = "kicked"
	memberRemovalCauseBanned memberRemovalCause = "banned"
)

func runBot(token, dbPath string, db *sql.DB, stmts *preparedStatements) error {
	if err := ensureDatabaseFilePresent(dbPath); err != nil {
		return fmt.Errorf("database file check failed: %w", err)
	}

	startupFatal := make(chan error, 1)
	dg, err := newDiscordSession(token)
	if err != nil {
		return fmt.Errorf("failed to create session: %w", err)
	}
	defer dg.Close()

	registerHandlers(dg, db, dbPath, stmts, startupFatal)

	if err := dg.Open(); err != nil {
		return fmt.Errorf("failed to open websocket: %w", err)
	}
	stopDBMonitor := startDatabaseFileMonitor(dbPath, databaseFileMonitorTick, func(err error) {
		log.Printf("database monitor detected missing file; aborting sync: %v", err)
		notifyStartupFatal(startupFatal, fmt.Errorf("database file missing during runtime: %w", err))
	})
	defer stopDBMonitor()

	log.Println("bot is running. press Ctrl+C to stop.")
	if err := waitForShutdown(startupFatal); err != nil {
		log.Println("shutting down")
		return err
	}
	log.Println("shutting down")
	return nil
}

func newDiscordSession(token string) (*discordgo.Session, error) {
	dg, err := discordgo.New("Bot " + token)
	if err != nil {
		return nil, err
	}
	dg.Identify.Intents = discordgo.IntentsGuilds |
		discordgo.IntentsGuildBans |
		discordgo.IntentsGuildMembers |
		discordgo.IntentsGuildMessages |
		discordgo.IntentsMessageContent
	return dg, nil
}

func registerHandlers(
	dg *discordgo.Session,
	db *sql.DB,
	dbPath string,
	stmts *preparedStatements,
	startupFatal chan<- error,
) {
	cfg := loadBackfillConfig()
	guildFilter := loadGuildSyncFilter()
	var syncEnabled atomic.Bool
	syncEnabled.Store(false)
	var activeBackfillMetrics atomic.Pointer[backfillMetrics]
	setTrackedEventLoggingEnabled(false)

	dg.AddHandler(func(s *discordgo.Session, rl *discordgo.RateLimit) {
		m := activeBackfillMetrics.Load()
		if m == nil {
			return
		}
		m.noteRateLimit(rl)
		snap := m.snapshot()

		retryAfter := time.Duration(0)
		url := ""
		if rl != nil {
			url = rl.URL
			if rl.TooManyRequests != nil {
				retryAfter = rl.RetryAfter
			}
		}

		log.Printf(
			"discord rate-limit url=%s retry_after=%s total_429=%d total_retry_after=%s",
			url,
			retryAfter,
			snap.RateLimitCount,
			snap.RetryAfterTotal,
		)
	})

	dg.AddHandler(func(s *discordgo.Session, r *discordgo.Ready) {
		log.Printf("connected as %s#%s (user_id=%s)", r.User.Username, r.User.Discriminator, r.User.ID)
		log.Printf("logging to sqlite: %s", dbPath)
		log.Printf("guild sync filter: %s", guildFilter.describe())
		log.Printf(
			"startup backfill config: max_pages_per_run=%d max_backfill_minutes=%d",
			cfg.maxPagesPerRun,
			int(cfg.maxDuration/time.Minute),
		)

		var trackedReadyGuilds []*discordgo.Guild
		for _, g := range r.Guilds {
			if g == nil || !guildFilter.allows(g.ID) {
				continue
			}
			trackedReadyGuilds = append(trackedReadyGuilds, g)
		}
		log.Printf("ready guilds: total=%d tracked=%d", len(r.Guilds), len(trackedReadyGuilds))
		log.Printf("syncing guilds: %s", describeTrackedReadyGuilds(s, db, trackedReadyGuilds))
		if guildFilter.allowAll {
			if suggested := suggestedGuildIDEnvValue(trackedReadyGuilds); suggested != "" {
				log.Printf(
					"setup hint: currently syncing all guilds (%s=\"*\"); to pin specific guilds, set %s=%q",
					config.EnvDiscordSyncGuildIDs,
					config.EnvDiscordSyncGuildIDs,
					suggested,
				)
			}
		}
		if len(trackedReadyGuilds) == 0 {
			log.Println("no tracked guilds matched filter; sync remains disabled")
			return
		}
		if err := ensureDatabaseFilePresent(dbPath); err != nil {
			log.Printf("sync aborted: %v", err)
			notifyStartupFatal(startupFatal, fmt.Errorf("database file missing before preflight: %w", err))
			return
		}
		channelsByGuild := logChannelSyncPreview(s, db, trackedReadyGuilds)
		if !preflightGuildReadAccess(s, db, trackedReadyGuilds, channelsByGuild) {
			log.Println("sync aborted by preflight; terminating process")
			notifyStartupFatal(startupFatal, fmt.Errorf("startup preflight failed: missing channel read access"))
			return
		}
		if err := ensureDatabaseFilePresent(dbPath); err != nil {
			log.Printf("sync aborted: %v", err)
			notifyStartupFatal(startupFatal, fmt.Errorf("database file missing before startup sync: %w", err))
			return
		}
		syncEnabled.Store(true)
		// Enable realtime event logs immediately so incoming messages are visible
		// while startup backfill is still running.
		setTrackedEventLoggingEnabled(true)
		log.Println("preflight passed; starting startup backfill")

		now := time.Now().UTC().Format(time.RFC3339Nano)
		for _, g := range trackedReadyGuilds {
			if g == nil || g.ID == "" {
				continue
			}
			guildName := resolveGuildDisplayName(s, db, g)
			if err := upsertGuildName(stmts.upsertIDNameMapping, g.ID, guildName, now); err != nil {
				log.Printf("guild name mapping upsert failed (guild=%s): %v", g.ID, err)
			}
			upsertRolesFromGuild(stmts, g, now)
			syncMemberRolesFromGuild(stmts, g, now)
			if err := syncMemberRolesFromGuildAPI(s, stmts, g.ID, now); err != nil {
				log.Printf("guild member-role snapshot sync failed (guild=%s): %v", g.ID, err)
			}
		}

		// Backfill in goroutine so gateway handlers stay responsive.
		go func() {
			metrics := newBackfillMetrics()
			activeBackfillMetrics.Store(metrics)
			defer activeBackfillMetrics.Store(nil)

			run := newBackfillRun(cfg, metrics)
			status := "complete"

			for _, g := range trackedReadyGuilds {
				if g == nil || g.ID == "" {
					continue
				}
				guildName := resolveGuildDisplayName(s, db, g)
				log.Printf("startup backfill begin guild=%s (%s)", guildName, g.ID)

				if err := backfillGuild(s, db, stmts, g.ID, run); err != nil {
					if errors.Is(err, errBackfillBudgetReached) {
						status = "paused_budget"
						break
					}
					log.Printf("backfill guild %s failed: %v", g.ID, err)
				}
			}

			if run.budgetReached() {
				status = "paused_budget"
			}

			budgetReason := run.reason()
			if budgetReason == "" {
				budgetReason = "none"
			}

			snap := metrics.snapshot()
			log.Printf(
				"startup backfill %s requests=%d messages_inserted=%d rate_limits=%d retry_after_total=%s channels_done=%d channels_total=%d channels_remaining=%d pages_used=%d elapsed=%s budget_reason=%s",
				status,
				snap.RequestCount,
				snap.MessagesInserted,
				snap.RateLimitCount,
				snap.RetryAfterTotal,
				snap.ChannelsDone,
				snap.ChannelsTotal,
				snap.ChannelsRemaining,
				run.usedPages(),
				snap.Elapsed,
				budgetReason,
			)
			log.Println("startup complete! backfill finished; realtime logging remains active")
			fmt.Println(strings.Repeat(string('-'), 158))
		}()
	})

	dg.AddHandler(func(s *discordgo.Session, m *discordgo.MessageCreate) {
		if m == nil {
			return
		}
		if !syncEnabled.Load() {
			logSkippedMessageCreate(m, "sync_disabled")
			return
		}
		if !guildFilter.allows(m.GuildID) {
			logSkippedMessageCreate(m, "guild_filter_rejected")
			return
		}
		handleMessageCreate(s, db, stmts, m)
	})
	dg.AddHandler(func(s *discordgo.Session, m *discordgo.MessageUpdate) {
		if m == nil || !syncEnabled.Load() || !guildFilter.allows(m.GuildID) {
			return
		}
		handleMessageUpdate(s, stmts, m)
	})
	dg.AddHandler(func(s *discordgo.Session, m *discordgo.MessageDelete) {
		if m == nil || !syncEnabled.Load() || !guildFilter.allows(m.GuildID) {
			return
		}
		handleMessageDelete(stmts, m)
	})
	dg.AddHandler(func(s *discordgo.Session, g *discordgo.GuildCreate) {
		if g == nil || !syncEnabled.Load() || !guildFilter.allows(g.ID) {
			return
		}
		handleGuildCreate(stmts, g)
	})
	dg.AddHandler(func(s *discordgo.Session, g *discordgo.GuildUpdate) {
		if g == nil || !syncEnabled.Load() || !guildFilter.allows(g.ID) {
			return
		}
		handleGuildUpdate(db, stmts, g)
	})
	dg.AddHandler(func(s *discordgo.Session, r *discordgo.GuildRoleCreate) {
		if r == nil || !syncEnabled.Load() || !guildFilter.allows(r.GuildID) {
			return
		}
		handleGuildRoleCreate(stmts, r)
	})
	dg.AddHandler(func(s *discordgo.Session, r *discordgo.GuildRoleUpdate) {
		if r == nil || !syncEnabled.Load() || !guildFilter.allows(r.GuildID) {
			return
		}
		handleGuildRoleUpdate(db, stmts, r)
	})
	dg.AddHandler(func(s *discordgo.Session, r *discordgo.GuildRoleDelete) {
		if r == nil || !syncEnabled.Load() || !guildFilter.allows(r.GuildID) {
			return
		}
		handleGuildRoleDelete(stmts, r)
	})
	dg.AddHandler(func(s *discordgo.Session, m *discordgo.GuildMemberAdd) {
		if m == nil || !syncEnabled.Load() || !guildFilter.allows(m.GuildID) {
			return
		}
		handleGuildMemberAdd(stmts, m)
	})
	dg.AddHandler(func(s *discordgo.Session, m *discordgo.GuildMemberUpdate) {
		if m == nil || !syncEnabled.Load() || !guildFilter.allows(m.GuildID) {
			return
		}
		handleGuildMemberUpdate(db, stmts, m)
	})
	dg.AddHandler(func(s *discordgo.Session, m *discordgo.GuildMemberRemove) {
		if m == nil || !syncEnabled.Load() || !guildFilter.allows(m.GuildID) {
			return
		}
		handleGuildMemberRemove(s, stmts, m)
	})
	dg.AddHandler(func(s *discordgo.Session, b *discordgo.GuildBanAdd) {
		if b == nil || !syncEnabled.Load() || !guildFilter.allows(b.GuildID) {
			return
		}
		handleGuildBanAdd(s, stmts, b)
	})
	dg.AddHandler(func(s *discordgo.Session, c *discordgo.ChannelCreate) {
		if c == nil || !syncEnabled.Load() || !guildFilter.allows(c.GuildID) {
			return
		}
		handleChannelCreate(stmts, c)
	})
	dg.AddHandler(func(s *discordgo.Session, c *discordgo.ChannelUpdate) {
		if c == nil || !syncEnabled.Load() || !guildFilter.allows(c.GuildID) {
			return
		}
		handleChannelUpdate(stmts, c)
	})
	dg.AddHandler(func(s *discordgo.Session, c *discordgo.ChannelDelete) {
		if c == nil || !syncEnabled.Load() || !guildFilter.allows(c.GuildID) {
			return
		}
		handleChannelDelete(stmts, c)
	})
	dg.AddHandler(func(s *discordgo.Session, t *discordgo.ThreadCreate) {
		if t == nil || !syncEnabled.Load() || !guildFilter.allows(t.GuildID) {
			return
		}
		handleThreadCreate(stmts, t)
	})
	dg.AddHandler(func(s *discordgo.Session, t *discordgo.ThreadUpdate) {
		if t == nil || !syncEnabled.Load() || !guildFilter.allows(t.GuildID) {
			return
		}
		handleThreadUpdate(stmts, t)
	})
	dg.AddHandler(func(s *discordgo.Session, t *discordgo.ThreadDelete) {
		if t == nil || !syncEnabled.Load() || !guildFilter.allows(t.GuildID) {
			return
		}
		handleThreadDelete(stmts, t)
	})
}

func handleMessageCreate(s *discordgo.Session, db *sql.DB, stmts *preparedStatements, m *discordgo.MessageCreate) {
	if m == nil || m.Message == nil {
		return
	}
	if m.Author != nil && m.Author.Bot {
		logSkippedMessageCreate(m, "author_is_bot")
		return
	}
	if m.GuildID == "" {
		logSkippedMessageCreate(m, "dm_message")
		return // ignore DMs for now
	}

	// Ignore structural empty message events (for example thread system messages),
	// but keep attachment-only user messages (e.g. long text sent as .txt).
	// We still advance channel state so backfill does not reprocess skipped IDs forever.
	if m.Content == "" && len(m.Attachments) == 0 {
		logSkippedMessageCreate(m, "structural_empty_message")
		if err := updateHighWaterMark(db, stmts.upsertState, m.GuildID, m.ChannelID, m.ID); err != nil {
			log.Printf("state update failed (channel=%s): %v", m.ChannelID, err)
		}
		return
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)

	if err := upsertGuildFromState(s, stmts.upsertIDNameMapping, m.GuildID, now); err != nil {
		log.Printf("guild name mapping upsert failed (guild=%s): %v", m.GuildID, err)
	}

	// Upsert user metadata (latest known).
	if m.Author != nil {
		if err := upsertUserRow(stmts.upsertUser, stmts.upsertIDNameMapping, m.GuildID, m.Author, now); err != nil {
			log.Printf("user upsert failed (author=%s): %v", m.Author.ID, err)
		}
	}

	// Upsert channel/thread metadata best-effort.
	// We avoid extra API calls: try session state first; if not found, store minimal row.
	if err := upsertChannelFromMessage(s, stmts.upsertChannel, stmts.upsertIDNameMapping, m.GuildID, m.ChannelID, now); err != nil {
		log.Printf("channel upsert failed (channel=%s): %v", m.ChannelID, err)
	}

	authorID := ""
	senderName := ""
	if m.Author != nil {
		authorID = m.Author.ID
		if n := strings.TrimSpace(m.Author.GlobalName); n != "" {
			senderName = n
		} else if n := strings.TrimSpace(m.Author.Username); n != "" {
			senderName = n
		}
		if senderName == "" {
			senderName = authorID
		}
		if err := upsertGuildMemberRow(stmts.upsertGuildMember, m.GuildID, authorID, now); err != nil {
			log.Printf("guild member upsert failed (guild=%s user=%s): %v", m.GuildID, authorID, err)
		}
	}

	createdAt := normalizeTimestamp(m.Timestamp)
	hasTextContent := m.Content != ""
	rel := deriveMessageRelationship(s, m.Message, "")

	if hasTextContent {
		if _, err := stmts.insertMsg.Exec(
			m.ID,
			m.GuildID,
			m.ChannelID,
			authorID,
			createdAt,
			m.Content,
			rel.referencedMessageID,
			rel.referencedChannelID,
			rel.referencedGuildID,
			rel.threadID,
			rel.threadParentID,
		); err != nil {
			log.Printf("db insert failed (msg=%s): %v", m.ID, err)
			return
		}
	}

	attachmentLogContent := ""
	if len(m.Attachments) > 0 {
		_, attachmentLogContent = upsertMessageAttachments(
			stmts.upsertAttachment,
			m.Attachments,
			m.GuildID,
			m.ChannelID,
			m.ID,
			authorID,
			createdAt,
			now,
			rel,
		)
	}

	recordUserPings(stmts, m.GuildID, m.ChannelID, m.ID, authorID, createdAt, m.Mentions)
	recordRolePings(stmts, s, m.GuildID, m.ChannelID, m.ID, authorID, createdAt, m.MentionRoles)

	if err := updateHighWaterMark(db, stmts.upsertState, m.GuildID, m.ChannelID, m.ID); err != nil {
		log.Printf("state update failed (channel=%s): %v", m.ChannelID, err)
	}

	sentPayload := map[string]any{
		"content":           m.Content,
		"attachments_count": len(m.Attachments),
		"embeds_count":      len(m.Embeds),
		"sender_name":       senderName,
	}
	if err := recordLifecycleEvent(
		stmts.insertEvent,
		eventMessageSent,
		m.GuildID,
		m.ChannelID,
		m.ID,
		authorID,
		createdAt,
		sentPayload,
	); err != nil {
		log.Printf("event insert failed (type=%s msg=%s): %v", eventMessageSent, m.ID, err)
	}

	logContent := messageSentLogContent(m.Content, attachmentLogContent)
	logMessageSentEvent(s, db, m.GuildID, m.ChannelID, m.ID, senderName, logContent, createdAt)
}

func logSkippedMessageCreate(m *discordgo.MessageCreate, reason string) {
	if m == nil || m.Message == nil {
		return
	}
	authorID := ""
	authorIsBot := false
	if m.Author != nil {
		authorID = m.Author.ID
		authorIsBot = m.Author.Bot
	}
	logTrackedEvent(
		eventMessageSkipped,
		m.GuildID,
		m.ChannelID,
		m.ID,
		authorID,
		map[string]any{
			"skip_reason":       reason,
			"author_is_bot":     authorIsBot,
			"content":           m.Content,
			"attachments_count": len(m.Attachments),
			"embeds_count":      len(m.Embeds),
		},
	)
}

func handleMessageUpdate(s *discordgo.Session, stmts *preparedStatements, m *discordgo.MessageUpdate) {
	if m == nil || m.Message == nil || m.GuildID == "" {
		return
	}
	if m.Author != nil && m.Author.Bot {
		return
	}
	if m.BeforeUpdate != nil && m.BeforeUpdate.Author != nil && m.BeforeUpdate.Author.Bot {
		return
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)

	if err := upsertGuildFromState(s, stmts.upsertIDNameMapping, m.GuildID, now); err != nil {
		log.Printf("guild name mapping upsert failed (guild=%s): %v", m.GuildID, err)
	}
	authorID := ""
	if m.Author != nil {
		authorID = m.Author.ID
		if err := upsertUserRow(stmts.upsertUser, stmts.upsertIDNameMapping, m.GuildID, m.Author, now); err != nil {
			log.Printf("user upsert failed (author=%s): %v", m.Author.ID, err)
		}
	}

	if err := upsertChannelFromMessage(s, stmts.upsertChannel, stmts.upsertIDNameMapping, m.GuildID, m.ChannelID, now); err != nil {
		log.Printf("channel upsert failed (channel=%s): %v", m.ChannelID, err)
	}

	editedAt := ""
	if m.EditedTimestamp != nil && !m.EditedTimestamp.IsZero() {
		editedAt = m.EditedTimestamp.UTC().Format(time.RFC3339Nano)
		if _, err := stmts.markMessageEdited.Exec(m.Content, editedAt, m.ID); err != nil {
			log.Printf("mark message edited failed (msg=%s): %v", m.ID, err)
		}
	}

	payload := map[string]any{
		"content":           m.Content,
		"edited_at":         editedAt,
		"had_cached_before": m.BeforeUpdate != nil,
	}
	if m.BeforeUpdate != nil {
		payload["before_content"] = m.BeforeUpdate.Content
	}

	inserted, err := insertMessageUpdatedLifecycleEventDedup(
		stmts.insertMessageUpdatedEventDedup,
		m.GuildID,
		m.ChannelID,
		m.ID,
		authorID,
		now,
		m.Content,
		editedAt,
		payload,
	)
	if err != nil {
		log.Printf("event insert failed (type=%s msg=%s): %v", eventMessageUpdated, m.ID, err)
		return
	}
	if !inserted {
		return
	}

	logTrackedEvent(
		eventMessageUpdated,
		m.GuildID,
		m.ChannelID,
		m.ID,
		authorID,
		payload,
	)
}

func insertMessageUpdatedLifecycleEventDedup(
	stmt *sql.Stmt,
	guildID, channelID, messageID, actorID, occurredAt, content, editedAt string,
	payload map[string]any,
) (bool, error) {
	if stmt == nil {
		return false, fmt.Errorf("nil message_updated dedup statement")
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return false, err
	}
	res, err := stmt.Exec(
		string(eventMessageUpdated),
		guildID,
		channelID,
		messageID,
		actorID,
		occurredAt,
		string(payloadJSON),
		guildID,
		channelID,
		messageID,
		actorID,
		content,
		editedAt,
	)
	if err != nil {
		return false, err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return rows > 0, nil
}

func handleMessageDelete(stmts *preparedStatements, m *discordgo.MessageDelete) {
	if m == nil || m.Message == nil || m.GuildID == "" {
		return
	}
	if m.Author != nil && m.Author.Bot {
		return
	}
	if m.BeforeDelete != nil && m.BeforeDelete.Author != nil && m.BeforeDelete.Author.Bot {
		return
	}
	deletedAt := time.Now().UTC().Format(time.RFC3339Nano)

	if _, err := stmts.markMessageDeleted.Exec(deletedAt, m.ID); err != nil {
		log.Printf("mark message deleted failed (msg=%s): %v", m.ID, err)
	}
	if _, err := stmts.markAttachmentsDeletedByMessage.Exec(deletedAt, deletedAt, m.ID); err != nil {
		log.Printf("mark attachments deleted failed (msg=%s): %v", m.ID, err)
	}

	actorID := ""
	payload := map[string]any{
		"had_cached_before": m.BeforeDelete != nil,
	}
	if m.BeforeDelete != nil {
		payload["before_content"] = m.BeforeDelete.Content
		if m.BeforeDelete.Author != nil {
			actorID = m.BeforeDelete.Author.ID
			payload["before_author_id"] = m.BeforeDelete.Author.ID
		}
	}

	if err := recordLifecycleEvent(
		stmts.insertEvent,
		eventMessageDeleted,
		m.GuildID,
		m.ChannelID,
		m.ID,
		actorID,
		deletedAt,
		payload,
	); err != nil {
		log.Printf("event insert failed (type=%s msg=%s): %v", eventMessageDeleted, m.ID, err)
	}

	logTrackedEvent(
		eventMessageDeleted,
		m.GuildID,
		m.ChannelID,
		m.ID,
		actorID,
		payload,
	)
}

func handleGuildCreate(stmts *preparedStatements, g *discordgo.GuildCreate) {
	if g == nil || g.Guild == nil || g.ID == "" {
		return
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	if err := upsertGuildName(stmts.upsertIDNameMapping, g.ID, g.Name, now); err != nil {
		log.Printf("guild name mapping upsert failed (guild=%s): %v", g.ID, err)
	}
	upsertRolesFromGuild(stmts, g.Guild, now)
	syncMemberRolesFromGuild(stmts, g.Guild, now)
}

func handleGuildUpdate(db *sql.DB, stmts *preparedStatements, g *discordgo.GuildUpdate) {
	if g == nil || g.Guild == nil || g.ID == "" {
		return
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	beforeName := currentMappedName(db, nameMappingEntityGuild, g.ID, g.ID)
	if err := upsertGuildName(stmts.upsertIDNameMapping, g.ID, g.Name, now); err != nil {
		log.Printf("guild name mapping upsert failed (guild=%s): %v", g.ID, err)
	}
	upsertRolesFromGuild(stmts, g.Guild, now)
	syncMemberRolesFromGuild(stmts, g.Guild, now)
	afterName := strings.TrimSpace(g.Name)
	if beforeName != "" && afterName != "" && beforeName != afterName {
		payload := map[string]any{
			"before_name": beforeName,
			"after_name":  afterName,
		}
		if err := recordLifecycleEvent(
			stmts.insertEvent,
			eventGuildRenamed,
			g.ID,
			"",
			"",
			"",
			now,
			payload,
		); err != nil {
			log.Printf("event insert failed (type=%s guild=%s): %v", eventGuildRenamed, g.ID, err)
		}
		logTrackedEvent(eventGuildRenamed, g.ID, "", "", "", payload)
	}
}

func handleGuildRoleCreate(stmts *preparedStatements, r *discordgo.GuildRoleCreate) {
	if r == nil || r.GuildRole == nil || r.Role == nil || strings.TrimSpace(r.GuildID) == "" {
		return
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if err := upsertRoleRow(stmts.upsertRole, stmts.upsertIDNameMapping, r.GuildID, r.Role, now); err != nil {
		log.Printf("role upsert failed (guild=%s role=%s): %v", r.GuildID, r.Role.ID, err)
	}
}

func handleGuildRoleUpdate(db *sql.DB, stmts *preparedStatements, r *discordgo.GuildRoleUpdate) {
	if r == nil || r.GuildRole == nil || r.Role == nil || strings.TrimSpace(r.GuildID) == "" {
		return
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	beforeName := currentMappedName(db, nameMappingEntityRole, r.Role.ID, r.GuildID)
	if err := upsertRoleRow(stmts.upsertRole, stmts.upsertIDNameMapping, r.GuildID, r.Role, now); err != nil {
		log.Printf("role upsert failed (guild=%s role=%s): %v", r.GuildID, r.Role.ID, err)
	}
	afterName := strings.TrimSpace(r.Role.Name)
	if beforeName == "" || afterName == "" || beforeName == afterName {
		return
	}

	payload := map[string]any{
		"role_id":      r.Role.ID,
		"before_name":  beforeName,
		"after_name":   afterName,
		"role_mention": "<@&" + r.Role.ID + ">",
	}
	if err := recordLifecycleEvent(
		stmts.insertEvent,
		eventRoleRenamed,
		r.GuildID,
		"",
		"",
		"",
		now,
		payload,
	); err != nil {
		log.Printf("event insert failed (type=%s role=%s): %v", eventRoleRenamed, r.Role.ID, err)
	}
	logTrackedEvent(eventRoleRenamed, r.GuildID, "", "", "", payload)
}

func handleGuildRoleDelete(stmts *preparedStatements, r *discordgo.GuildRoleDelete) {
	if r == nil || strings.TrimSpace(r.GuildID) == "" || strings.TrimSpace(r.RoleID) == "" {
		return
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := stmts.markRoleDeleted.Exec(now, now, r.RoleID, r.GuildID); err != nil {
		log.Printf("mark role deleted failed (guild=%s role=%s): %v", r.GuildID, r.RoleID, err)
	}

	payload := map[string]any{
		"role_id":      r.RoleID,
		"role_mention": "<@&" + r.RoleID + ">",
	}
	if err := recordLifecycleEvent(
		stmts.insertEvent,
		eventRoleDeleted,
		r.GuildID,
		"",
		"",
		"",
		now,
		payload,
	); err != nil {
		log.Printf("event insert failed (type=%s role=%s): %v", eventRoleDeleted, r.RoleID, err)
	}
	logTrackedEvent(eventRoleDeleted, r.GuildID, "", "", "", payload)
}

func handleGuildMemberAdd(stmts *preparedStatements, m *discordgo.GuildMemberAdd) {
	if m == nil || m.Member == nil || m.User == nil || strings.TrimSpace(m.GuildID) == "" || m.User.Bot {
		return
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if err := upsertUserRow(stmts.upsertUser, stmts.upsertIDNameMapping, m.GuildID, m.User, now); err != nil {
		log.Printf("user upsert failed (author=%s): %v", m.User.ID, err)
	}
	if err := upsertGuildMemberRow(stmts.upsertGuildMember, m.GuildID, m.User.ID, now); err != nil {
		log.Printf("guild member upsert failed (guild=%s user=%s): %v", m.GuildID, m.User.ID, err)
	}
	if err := setMemberRolesSnapshot(stmts, m.GuildID, m.User.ID, m.Roles, now); err != nil {
		log.Printf("member role snapshot failed (guild=%s user=%s): %v", m.GuildID, m.User.ID, err)
	}

	payload := userLifecyclePayload(m.User)
	if nick := strings.TrimSpace(m.Nick); nick != "" {
		payload["nick"] = nick
	}
	if len(m.Roles) > 0 {
		payload["role_ids"] = normalizeUniqueIDs(m.Roles)
	}
	if err := recordLifecycleEvent(
		stmts.insertEvent,
		eventUserJoined,
		m.GuildID,
		"",
		"",
		m.User.ID,
		now,
		payload,
	); err != nil {
		log.Printf("event insert failed (type=%s user=%s): %v", eventUserJoined, m.User.ID, err)
	}
	logTrackedEvent(eventUserJoined, m.GuildID, "", "", m.User.ID, payload)
}

func handleGuildMemberRemove(s *discordgo.Session, stmts *preparedStatements, m *discordgo.GuildMemberRemove) {
	if m == nil || m.Member == nil || m.User == nil || strings.TrimSpace(m.GuildID) == "" || m.User.Bot {
		return
	}
	now := time.Now().UTC()
	if _, err := stmts.deleteMemberRolesByUser.Exec(m.GuildID, m.User.ID); err != nil {
		log.Printf("member role cleanup failed (guild=%s user=%s): %v", m.GuildID, m.User.ID, err)
	}
	if err := deleteGuildMemberRow(stmts.deleteGuildMember, m.GuildID, m.User.ID); err != nil {
		log.Printf("guild member cleanup failed (guild=%s user=%s): %v", m.GuildID, m.User.ID, err)
	}

	cause, auditEntry := inferMemberRemovalCause(s, m.GuildID, m.User.ID, now)
	if cause == memberRemovalCauseBanned {
		return
	}

	eventType := eventUserLeft
	actorID := m.User.ID
	payload := userLifecyclePayload(m.User)
	payload["removal_cause"] = string(cause)

	if cause == memberRemovalCauseKicked {
		eventType = eventUserKicked
		actorID = ""
		if auditEntry != nil {
			if modID := strings.TrimSpace(auditEntry.UserID); modID != "" {
				actorID = modID
				payload["moderator_id"] = modID
			}
			if reason := strings.TrimSpace(auditEntry.Reason); reason != "" {
				payload["reason"] = reason
			}
		}
	}

	if err := recordLifecycleEvent(
		stmts.insertEvent,
		eventType,
		m.GuildID,
		"",
		"",
		actorID,
		now.Format(time.RFC3339Nano),
		payload,
	); err != nil {
		log.Printf("event insert failed (type=%s user=%s): %v", eventType, m.User.ID, err)
	}
	logTrackedEvent(eventType, m.GuildID, "", "", actorID, payload)
}

func handleGuildBanAdd(s *discordgo.Session, stmts *preparedStatements, b *discordgo.GuildBanAdd) {
	if b == nil || b.User == nil || strings.TrimSpace(b.GuildID) == "" || strings.TrimSpace(b.User.ID) == "" {
		return
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if err := deleteGuildMemberRow(stmts.deleteGuildMember, b.GuildID, b.User.ID); err != nil {
		log.Printf("guild member cleanup failed on ban (guild=%s user=%s): %v", b.GuildID, b.User.ID, err)
	}
	if _, err := stmts.deleteMemberRolesByUser.Exec(b.GuildID, b.User.ID); err != nil {
		log.Printf("member role cleanup failed on ban (guild=%s user=%s): %v", b.GuildID, b.User.ID, err)
	}

	payload := userLifecyclePayload(b.User)
	actorID := ""
	if auditEntry := fetchRecentAuditLogEntry(
		s,
		b.GuildID,
		b.User.ID,
		discordgo.AuditLogActionMemberBanAdd,
		time.Now().UTC(),
	); auditEntry != nil {
		if modID := strings.TrimSpace(auditEntry.UserID); modID != "" {
			actorID = modID
			payload["moderator_id"] = modID
		}
		if reason := strings.TrimSpace(auditEntry.Reason); reason != "" {
			payload["reason"] = reason
		}
	}

	if err := recordLifecycleEvent(
		stmts.insertEvent,
		eventUserBanned,
		b.GuildID,
		"",
		"",
		actorID,
		now,
		payload,
	); err != nil {
		log.Printf("event insert failed (type=%s user=%s): %v", eventUserBanned, b.User.ID, err)
	}
	logTrackedEvent(eventUserBanned, b.GuildID, "", "", actorID, payload)
}

func handleGuildMemberUpdate(db *sql.DB, stmts *preparedStatements, m *discordgo.GuildMemberUpdate) {
	if m == nil || m.Member == nil || m.User == nil || m.GuildID == "" {
		return
	}
	if m.User.Bot {
		return
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	if err := upsertUserRow(stmts.upsertUser, stmts.upsertIDNameMapping, m.GuildID, m.User, now); err != nil {
		log.Printf("user upsert failed (author=%s): %v", m.User.ID, err)
	}
	if err := upsertGuildMemberRow(stmts.upsertGuildMember, m.GuildID, m.User.ID, now); err != nil {
		log.Printf("guild member upsert failed (guild=%s user=%s): %v", m.GuildID, m.User.ID, err)
	}
	if err := setMemberRolesSnapshot(stmts, m.GuildID, m.User.ID, m.Roles, now); err != nil {
		log.Printf("member role snapshot failed (guild=%s user=%s): %v", m.GuildID, m.User.ID, err)
	}

	var (
		addedRoleIDs   []string
		removedRoleIDs []string
	)
	if m.BeforeUpdate != nil {
		addedRoleIDs, removedRoleIDs = roleDiff(m.BeforeUpdate.Roles, m.Roles)
	}

	for _, roleID := range addedRoleIDs {
		roleName := currentMappedName(db, nameMappingEntityRole, roleID, m.GuildID)
		if roleName == "" {
			roleName = roleID
		}
		payload := map[string]any{
			"role_id":    roleID,
			"role_name":  roleName,
			"user_id":    m.User.ID,
			"username":   strings.TrimSpace(m.User.Username),
			"role_event": "assigned",
		}
		if err := recordLifecycleEvent(
			stmts.insertEvent,
			eventRoleAssigned,
			m.GuildID,
			"",
			"",
			m.User.ID,
			now,
			payload,
		); err != nil {
			log.Printf("event insert failed (type=%s user=%s role=%s): %v", eventRoleAssigned, m.User.ID, roleID, err)
		}
		logTrackedEvent(eventRoleAssigned, m.GuildID, "", "", m.User.ID, payload)
	}

	for _, roleID := range removedRoleIDs {
		roleName := currentMappedName(db, nameMappingEntityRole, roleID, m.GuildID)
		if roleName == "" {
			roleName = roleID
		}
		payload := map[string]any{
			"role_id":    roleID,
			"role_name":  roleName,
			"user_id":    m.User.ID,
			"username":   strings.TrimSpace(m.User.Username),
			"role_event": "revoked",
		}
		if err := recordLifecycleEvent(
			stmts.insertEvent,
			eventRoleRevoked,
			m.GuildID,
			"",
			"",
			m.User.ID,
			now,
			payload,
		); err != nil {
			log.Printf("event insert failed (type=%s user=%s role=%s): %v", eventRoleRevoked, m.User.ID, roleID, err)
		}
		logTrackedEvent(eventRoleRevoked, m.GuildID, "", "", m.User.ID, payload)
	}

	if m.BeforeUpdate == nil || m.BeforeUpdate.User == nil {
		return
	}

	beforeUsername := strings.TrimSpace(m.BeforeUpdate.User.Username)
	afterUsername := strings.TrimSpace(m.User.Username)
	if beforeUsername == "" || afterUsername == "" || beforeUsername == afterUsername {
		return
	}

	payload := map[string]any{
		"before_username": beforeUsername,
		"after_username":  afterUsername,
	}
	if beforeGlobal := strings.TrimSpace(m.BeforeUpdate.User.GlobalName); beforeGlobal != "" {
		payload["before_global_name"] = beforeGlobal
	}
	if afterGlobal := strings.TrimSpace(m.User.GlobalName); afterGlobal != "" {
		payload["after_global_name"] = afterGlobal
	}
	if beforeNick := strings.TrimSpace(m.BeforeUpdate.Nick); beforeNick != "" {
		payload["before_nick"] = beforeNick
	}
	if afterNick := strings.TrimSpace(m.Nick); afterNick != "" {
		payload["after_nick"] = afterNick
	}

	if err := recordLifecycleEvent(
		stmts.insertEvent,
		eventUsernameChanged,
		m.GuildID,
		"",
		"",
		m.User.ID,
		now,
		payload,
	); err != nil {
		log.Printf("event insert failed (type=%s user=%s): %v", eventUsernameChanged, m.User.ID, err)
	}
	logTrackedEvent(eventUsernameChanged, m.GuildID, "", "", m.User.ID, payload)
}

func handleChannelCreate(stmts *preparedStatements, c *discordgo.ChannelCreate) {
	if c == nil || c.Channel == nil || c.GuildID == "" || c.Channel.IsThread() {
		return
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	if err := upsertChannelRow(stmts.upsertChannel, stmts.upsertIDNameMapping, c.GuildID, c.Channel, now); err != nil {
		log.Printf("channel upsert failed (channel=%s): %v", c.ID, err)
	}

	if err := recordLifecycleEvent(
		stmts.insertEvent,
		eventChannelCreated,
		c.GuildID,
		c.ID,
		"",
		"",
		now,
		channelEventPayload(c.Channel),
	); err != nil {
		log.Printf("event insert failed (type=%s channel=%s): %v", eventChannelCreated, c.ID, err)
	}

	logTrackedEvent(
		eventChannelCreated,
		c.GuildID,
		c.ID,
		"",
		"",
		channelEventPayload(c.Channel),
	)
}

func handleChannelUpdate(stmts *preparedStatements, c *discordgo.ChannelUpdate) {
	if c == nil || c.Channel == nil || c.GuildID == "" || c.Channel.IsThread() {
		return
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	if err := upsertChannelRow(stmts.upsertChannel, stmts.upsertIDNameMapping, c.GuildID, c.Channel, now); err != nil {
		log.Printf("channel upsert failed (channel=%s): %v", c.ID, err)
	}

	if c.BeforeUpdate == nil {
		return
	}
	beforeName := strings.TrimSpace(c.BeforeUpdate.Name)
	afterName := strings.TrimSpace(c.Name)
	if beforeName == "" || afterName == "" || beforeName == afterName {
		return
	}

	payload := map[string]any{
		"before_name": beforeName,
		"after_name":  afterName,
		"type":        int(c.Type),
	}
	if err := recordLifecycleEvent(
		stmts.insertEvent,
		eventChannelRenamed,
		c.GuildID,
		c.ID,
		"",
		"",
		now,
		payload,
	); err != nil {
		log.Printf("event insert failed (type=%s channel=%s): %v", eventChannelRenamed, c.ID, err)
	}
	logTrackedEvent(eventChannelRenamed, c.GuildID, c.ID, "", "", payload)
}

func handleChannelDelete(stmts *preparedStatements, c *discordgo.ChannelDelete) {
	if c == nil || c.Channel == nil || c.GuildID == "" || c.Channel.IsThread() {
		return
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := stmts.markChannelDeleted.Exec(now, now, c.ID); err != nil {
		log.Printf("mark channel deleted failed (channel=%s): %v", c.ID, err)
	}

	if err := recordLifecycleEvent(
		stmts.insertEvent,
		eventChannelDeleted,
		c.GuildID,
		c.ID,
		"",
		"",
		now,
		channelEventPayload(c.Channel),
	); err != nil {
		log.Printf("event insert failed (type=%s channel=%s): %v", eventChannelDeleted, c.ID, err)
	}

	logTrackedEvent(
		eventChannelDeleted,
		c.GuildID,
		c.ID,
		"",
		"",
		channelEventPayload(c.Channel),
	)
}

func handleThreadCreate(stmts *preparedStatements, t *discordgo.ThreadCreate) {
	if t == nil || t.Channel == nil || t.GuildID == "" {
		return
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	if err := upsertChannelRow(stmts.upsertChannel, stmts.upsertIDNameMapping, t.GuildID, t.Channel, now); err != nil {
		log.Printf("thread upsert failed (thread=%s): %v", t.ID, err)
	}

	payload := channelEventPayload(t.Channel)
	payload["newly_created"] = t.NewlyCreated
	if err := recordLifecycleEvent(
		stmts.insertEvent,
		eventThreadCreated,
		t.GuildID,
		t.ID,
		"",
		"",
		now,
		payload,
	); err != nil {
		log.Printf("event insert failed (type=%s thread=%s): %v", eventThreadCreated, t.ID, err)
	}

	logTrackedEvent(
		eventThreadCreated,
		t.GuildID,
		t.ID,
		"",
		"",
		payload,
	)
}

func handleThreadUpdate(stmts *preparedStatements, t *discordgo.ThreadUpdate) {
	if t == nil || t.Channel == nil || t.GuildID == "" {
		return
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	if err := upsertChannelRow(stmts.upsertChannel, stmts.upsertIDNameMapping, t.GuildID, t.Channel, now); err != nil {
		log.Printf("thread upsert failed (thread=%s): %v", t.ID, err)
	}

	if t.BeforeUpdate == nil {
		return
	}
	beforeName := strings.TrimSpace(t.BeforeUpdate.Name)
	afterName := strings.TrimSpace(t.Name)
	if beforeName == "" || afterName == "" || beforeName == afterName {
		return
	}

	payload := map[string]any{
		"before_name": beforeName,
		"after_name":  afterName,
		"type":        int(t.Type),
		"parent_id":   t.ParentID,
	}
	if err := recordLifecycleEvent(
		stmts.insertEvent,
		eventThreadRenamed,
		t.GuildID,
		t.ID,
		"",
		"",
		now,
		payload,
	); err != nil {
		log.Printf("event insert failed (type=%s thread=%s): %v", eventThreadRenamed, t.ID, err)
	}
	logTrackedEvent(eventThreadRenamed, t.GuildID, t.ID, "", "", payload)
}

func handleThreadDelete(stmts *preparedStatements, t *discordgo.ThreadDelete) {
	if t == nil || t.Channel == nil || t.GuildID == "" {
		return
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := stmts.markChannelDeleted.Exec(now, now, t.ID); err != nil {
		log.Printf("mark thread deleted failed (thread=%s): %v", t.ID, err)
	}

	if err := recordLifecycleEvent(
		stmts.insertEvent,
		eventThreadDeleted,
		t.GuildID,
		t.ID,
		"",
		"",
		now,
		channelEventPayload(t.Channel),
	); err != nil {
		log.Printf("event insert failed (type=%s thread=%s): %v", eventThreadDeleted, t.ID, err)
	}

	logTrackedEvent(
		eventThreadDeleted,
		t.GuildID,
		t.ID,
		"",
		"",
		channelEventPayload(t.Channel),
	)
}

func channelEventPayload(c *discordgo.Channel) map[string]any {
	if c == nil {
		return map[string]any{}
	}
	return map[string]any{
		"name":      c.Name,
		"type":      int(c.Type),
		"parent_id": c.ParentID,
		"is_thread": c.IsThread(),
	}
}

func userLifecyclePayload(u *discordgo.User) map[string]any {
	if u == nil {
		return map[string]any{}
	}
	payload := map[string]any{
		"user_id": u.ID,
	}
	if username := strings.TrimSpace(u.Username); username != "" {
		payload["username"] = username
	}
	if globalName := strings.TrimSpace(u.GlobalName); globalName != "" {
		payload["global_name"] = globalName
	}
	if discriminator := strings.TrimSpace(u.Discriminator); discriminator != "" {
		payload["discriminator"] = discriminator
	}
	return payload
}

func inferMemberRemovalCause(
	s *discordgo.Session,
	guildID, userID string,
	now time.Time,
) (memberRemovalCause, *discordgo.AuditLogEntry) {
	if entry := fetchRecentAuditLogEntry(s, guildID, userID, discordgo.AuditLogActionMemberBanAdd, now); entry != nil {
		return memberRemovalCauseBanned, entry
	}
	if entry := fetchRecentAuditLogEntry(s, guildID, userID, discordgo.AuditLogActionMemberKick, now); entry != nil {
		return memberRemovalCauseKicked, entry
	}
	return memberRemovalCauseLeft, nil
}

func fetchRecentAuditLogEntry(
	s *discordgo.Session,
	guildID, userID string,
	actionType discordgo.AuditLogAction,
	now time.Time,
) *discordgo.AuditLogEntry {
	guildID = strings.TrimSpace(guildID)
	userID = strings.TrimSpace(userID)
	if guildID == "" || userID == "" {
		return nil
	}
	st, err := guildAuditLogFetcher(s, guildID, userID, actionType, 5)
	if err != nil || st == nil {
		return nil
	}

	for _, entry := range st.AuditLogEntries {
		if entry == nil || strings.TrimSpace(entry.TargetID) != userID {
			continue
		}
		if !isRecentAuditLogEntry(entry.ID, now, auditLogFreshnessWindow) {
			continue
		}
		if entry.ActionType != nil && *entry.ActionType != actionType {
			continue
		}
		return entry
	}
	return nil
}

func isRecentAuditLogEntry(entryID string, now time.Time, maxAge time.Duration) bool {
	entryTime, ok := snowflakeTime(entryID)
	if !ok {
		return false
	}
	age := now.Sub(entryTime)
	if age < 0 {
		age = -age
	}
	return age <= maxAge
}

func snowflakeTime(id string) (time.Time, bool) {
	id = strings.TrimSpace(id)
	if id == "" {
		return time.Time{}, false
	}
	v, err := strconv.ParseUint(id, 10, 64)
	if err != nil {
		return time.Time{}, false
	}
	ms := int64(v>>22) + discordSnowflakeEpochMS
	return time.UnixMilli(ms).UTC(), true
}

func currentMappedName(db *sql.DB, entityType, entityID, guildID string) string {
	if db == nil || strings.TrimSpace(entityType) == "" || strings.TrimSpace(entityID) == "" || strings.TrimSpace(guildID) == "" {
		return ""
	}
	var humanName string
	err := db.QueryRow(
		selectCurrentMappedNameQuery,
		entityType, entityID, guildID,
	).Scan(&humanName)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(humanName)
}

func logTrackedEvent(
	eventType lifecycleEventType,
	guildID, channelID, messageID, actorID string,
	payload map[string]any,
) {
	_ = eventType
	_ = guildID
	_ = channelID
	_ = messageID
	_ = actorID
	_ = payload
	// Intentionally no-op: structured lifecycle rows are persisted in SQLite and
	// the human-readable message_sent block is logged separately.
}

func logMessageSentEvent(
	s *discordgo.Session,
	db *sql.DB,
	guildID, channelID, messageID, senderName, content, createdAt string,
) {
	if !trackedEventLoggingEnabled.Load() {
		return
	}

	senderName = strings.TrimSpace(senderName)
	if senderName == "" {
		senderName = "unknown"
	}

	threadName, channelName := resolveMessageSentLocation(s, db, guildID, channelID)
	if channelName == "" {
		channelName = "unknown"
	}
	if !strings.HasPrefix(channelName, "#") {
		channelName = "#" + channelName
	}
	if threadName != "" && !strings.HasPrefix(threadName, "#") {
		threadName = "#" + threadName
	}

	content = replaceMentionsWithDisplayNames(s, db, guildID, content)
	eventTime := formatMessageSentTime(createdAt, messageID)
	fmt.Print(renderMessageSentEventLog(
		senderName,
		threadName,
		channelName,
		content,
		eventTime,
	))
}

func renderMessageSentEventLog(senderName, threadName, channelName, content, eventTime string) string {
	locationLines := fmt.Sprintf("Channel: %s\n", channelName)
	if strings.TrimSpace(threadName) != "" {
		locationLines = fmt.Sprintf("Thread: %s\nChannel: %s\n", threadName, channelName)
	}
	return fmt.Sprintf(
		"Event: message_sent\nUser: %s\n%sMessage: %s\nTime: %s\n\n",
		senderName,
		locationLines,
		content,
		eventTime,
	)
}

func messageSentLogContent(content, attachmentLogContent string) string {
	if strings.TrimSpace(content) != "" {
		return content
	}
	if strings.TrimSpace(attachmentLogContent) != "" {
		return attachmentLogContent
	}
	return content
}

func replaceMentionsWithDisplayNames(s *discordgo.Session, db *sql.DB, guildID, content string) string {
	if content == "" {
		return content
	}

	content = messageLogUserMentionPattern.ReplaceAllStringFunc(content, func(token string) string {
		matches := messageLogUserMentionPattern.FindStringSubmatch(token)
		if len(matches) != 2 {
			return token
		}
		userID := strings.TrimSpace(matches[1])
		if userID == "" {
			return token
		}
		if strings.HasPrefix(userID, "&") {
			return token
		}
		if display := resolveMentionUserDisplayName(s, db, guildID, userID); display != "" {
			return "@" + display
		}
		return token
	})

	content = messageLogRoleMentionPattern.ReplaceAllStringFunc(content, func(token string) string {
		matches := messageLogRoleMentionPattern.FindStringSubmatch(token)
		if len(matches) != 2 {
			return token
		}
		roleID := strings.TrimSpace(matches[1])
		if roleID == "" {
			return token
		}
		if roleName := currentMappedName(db, nameMappingEntityRole, roleID, guildID); roleName != "" {
			return "@" + roleName
		}
		return token
	})

	return content
}

func resolveMentionUserDisplayName(s *discordgo.Session, db *sql.DB, guildID, userID string) string {
	userID = strings.TrimSpace(userID)
	guildID = strings.TrimSpace(guildID)
	if userID == "" {
		return ""
	}

	if s != nil && s.State != nil && guildID != "" {
		if member, err := s.State.Member(guildID, userID); err == nil && member != nil {
			if nick := strings.TrimSpace(member.Nick); nick != "" {
				return nick
			}
			if member.User != nil {
				if global := strings.TrimSpace(member.User.GlobalName); global != "" {
					return global
				}
				if username := strings.TrimSpace(member.User.Username); username != "" {
					return username
				}
			}
		}
	}

	if mapped := currentMappedName(db, nameMappingEntityUser, userID, guildID); mapped != "" {
		return mapped
	}
	return userID
}

func resolveMessageSentLocation(s *discordgo.Session, db *sql.DB, guildID, channelID string) (string, string) {
	channelID = strings.TrimSpace(channelID)
	guildID = strings.TrimSpace(guildID)
	if channelID == "" {
		return "", ""
	}

	if s != nil && s.State != nil {
		if c, err := s.State.Channel(channelID); err == nil && c != nil {
			if c.IsThread() {
				threadName := strings.TrimSpace(c.Name)
				if threadName == "" {
					threadName = channelID
				}
				parentName := resolveMessageSentChannelNameByID(s, db, guildID, c.ParentID)
				if parentName == "" {
					parentName = "unknown"
				}
				return threadName, parentName
			}
			if name := strings.TrimSpace(c.Name); name != "" {
				return "", name
			}
			return "", channelID
		}
	}

	if db != nil {
		var (
			dbName     string
			dbIsThread int
			parentID   string
		)
		if err := db.QueryRow(
			selectChannelNameAndThreadByGuildAndChannelQuery,
			guildID,
			channelID,
		).Scan(&dbName, &dbIsThread, &parentID); err == nil {
			dbName = strings.TrimSpace(dbName)
			if dbName == "" {
				dbName = channelID
			}
			if dbIsThread != 0 {
				parentName := resolveMessageSentChannelNameByID(s, db, guildID, parentID)
				if parentName == "" {
					parentName = "unknown"
				}
				return dbName, parentName
			}
			return "", dbName
		}
	}

	if name := currentMappedName(db, nameMappingEntityChannel, channelID, guildID); name != "" {
		return "", name
	}
	return "", channelID
}

func resolveMessageSentChannelNameByID(s *discordgo.Session, db *sql.DB, guildID, channelID string) string {
	channelID = strings.TrimSpace(channelID)
	guildID = strings.TrimSpace(guildID)
	if channelID == "" {
		return ""
	}

	if s != nil && s.State != nil {
		if c, err := s.State.Channel(channelID); err == nil && c != nil {
			if name := strings.TrimSpace(c.Name); name != "" {
				return name
			}
		}
	}

	if db != nil {
		var (
			dbName     string
			dbIsThread int
			parentID   string
		)
		if err := db.QueryRow(
			selectChannelNameAndThreadByGuildAndChannelQuery,
			guildID,
			channelID,
		).Scan(&dbName, &dbIsThread, &parentID); err == nil {
			_ = dbIsThread
			_ = parentID
			if dbName = strings.TrimSpace(dbName); dbName != "" {
				return dbName
			}
		}
	}

	if name := currentMappedName(db, nameMappingEntityChannel, channelID, guildID); name != "" {
		return name
	}

	return channelID
}

func formatMessageSentTime(createdAt, messageID string) string {
	createdAt = strings.TrimSpace(createdAt)
	if createdAt != "" {
		if ts, err := time.Parse(time.RFC3339Nano, createdAt); err == nil {
			return ts.Local().Format("2006-01-02, 03:04:05 PM")
		}
	}
	if ts, ok := snowflakeTime(messageID); ok {
		return ts.Local().Format("2006-01-02, 03:04:05 PM")
	}
	return time.Now().Local().Format("2006-01-02, 03:04:05 PM")
}

func setTrackedEventLoggingEnabled(enabled bool) {
	trackedEventLoggingEnabled.Store(enabled)
}

func notifyStartupFatal(startupFatal chan<- error, err error) {
	if startupFatal == nil || err == nil {
		return
	}
	select {
	case startupFatal <- err:
	default:
	}
}

func waitForShutdown(startupFatal <-chan error) error {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(stop)

	select {
	case <-stop:
		return nil
	case err := <-startupFatal:
		if err == nil {
			return fmt.Errorf("startup aborted")
		}
		return err
	}
}
