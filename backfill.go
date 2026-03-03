// Package main implements guild/channel backfill and active-thread discovery routines.
package main

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bwmarrin/discordgo"
)

var errBackfillBudgetReached = errors.New("backfill budget reached")
var archivedThreadsPermissionNoticeOnce sync.Once

var guildChannelsFetcher = func(s *discordgo.Session, guildID string) ([]*discordgo.Channel, error) {
	if s == nil {
		return nil, fmt.Errorf("discord session is nil")
	}
	return s.GuildChannels(guildID)
}

var channelMessagesFetcher = func(
	s *discordgo.Session,
	channelID string,
	limit int,
	beforeID, afterID, aroundID string,
) ([]*discordgo.Message, error) {
	if s == nil {
		return nil, fmt.Errorf("discord session is nil")
	}
	return s.ChannelMessages(channelID, limit, beforeID, afterID, aroundID)
}

var guildActiveThreadsFetcher = func(s *discordgo.Session, guildID string) ([]*discordgo.Channel, error) {
	if s == nil {
		return nil, fmt.Errorf("discord session is nil")
	}
	resp, err := s.GuildThreadsActive(guildID)
	if err != nil {
		return nil, err
	}
	if resp == nil || resp.Threads == nil {
		return []*discordgo.Channel{}, nil
	}
	return resp.Threads, nil
}

var publicArchivedThreadsFetcher = func(
	s *discordgo.Session,
	channelID string,
	before *time.Time,
	limit int,
) (*discordgo.ThreadsList, error) {
	if s == nil {
		return nil, fmt.Errorf("discord session is nil")
	}
	return s.ThreadsArchived(channelID, before, limit)
}

var privateArchivedThreadsFetcher = func(
	s *discordgo.Session,
	channelID string,
	before *time.Time,
	limit int,
) (*discordgo.ThreadsList, error) {
	if s == nil {
		return nil, fmt.Errorf("discord session is nil")
	}
	return s.ThreadsPrivateArchived(channelID, before, limit)
}

var joinedPrivateArchivedThreadsFetcher = func(
	s *discordgo.Session,
	channelID string,
	before *time.Time,
	limit int,
) (*discordgo.ThreadsList, error) {
	if s == nil {
		return nil, fmt.Errorf("discord session is nil")
	}
	return s.ThreadsPrivateJoinedArchived(channelID, before, limit)
}

type archivedThreadPageFetcher struct {
	name  string
	fetch func(*discordgo.Session, string, *time.Time, int) (*discordgo.ThreadsList, error)
}

func backfillGuild(s *discordgo.Session, db *sql.DB, stmts *preparedStatements, guildID string, run *backfillRun) error {
	if run != nil {
		run.noteRequest()
	}
	chans, err := guildChannelsFetcher(s, guildID)
	if err != nil {
		return fmt.Errorf("GuildChannels: %w", err)
	}

	// Backfill message-bearing channels + threads. Use a map to dedupe.
	targets := make(map[string]*discordgo.Channel, len(chans))
	threadParents := make(map[string]*discordgo.Channel, len(chans))

	now := time.Now().UTC().Format(time.RFC3339Nano)
	_ = upsertGuildFromState(s, stmts.upsertIDNameMapping, guildID, now)

	for _, c := range chans {
		if c == nil {
			continue
		}
		if isDirectBackfillChannel(c) {
			targets[c.ID] = c
		}
		if canHaveArchivedThreads(c) {
			threadParents[c.ID] = c
		}

		// Upsert metadata for all channels we see anyway (cheap + helpful).
		// This also means the LLM can map ids->names even for non-text channels.
		_ = upsertChannelRow(stmts.upsertChannel, stmts.upsertIDNameMapping, guildID, c, now)
	}

	threads, err := fetchActiveThreads(s, guildID, run)
	if err != nil {
		log.Printf("fetchActiveThreads guild=%s failed: %v", guildID, err)
	} else {
		for _, t := range threads {
			if t == nil || t.ID == "" {
				continue
			}
			targets[t.ID] = t
			_ = upsertChannelRow(stmts.upsertChannel, stmts.upsertIDNameMapping, guildID, t, now)
		}
		log.Printf("guild=%s active_threads=%d", guildID, len(threads))
	}

	parentIDs := make([]string, 0, len(threadParents))
	for parentID := range threadParents {
		parentIDs = append(parentIDs, parentID)
	}
	sort.Strings(parentIDs)

	// Skip parents that were recently checked for archived threads.
	var archivedDiscoveryTTL time.Duration
	if run != nil {
		archivedDiscoveryTTL = run.cfg.archivedDiscoveryTTL
	}
	if archivedDiscoveryTTL > 0 {
		discoveryState := loadArchivedDiscoveryState(db, guildID)
		var filtered []string
		skipped := 0
		for _, parentID := range parentIDs {
			if checkedAt, ok := discoveryState[parentID]; ok {
				if time.Since(checkedAt) < archivedDiscoveryTTL {
					skipped++
					continue
				}
			}
			filtered = append(filtered, parentID)
		}
		if skipped > 0 {
			log.Printf(
				"archived-thread discovery: skipped %d parent(s) checked within %s",
				skipped,
				archivedDiscoveryTTL,
			)
		}
		parentIDs = filtered
	}

	if len(parentIDs) > 0 {
		log.Printf("discovering archived threads guild=%s parent_channels=%d", guildID, len(parentIDs))
	}

	for idx, parentID := range parentIDs {
		parent := threadParents[parentID]
		log.Printf(
			"discovering archived threads [%d/%d] %s",
			idx+1,
			len(parentIDs),
			backfillTargetLabel(parent),
		)
		threads, err := fetchArchivedThreadsForParent(s, parent, run)
		if err != nil {
			if errors.Is(err, errBackfillBudgetReached) {
				return errBackfillBudgetReached
			}
			log.Printf("fetchArchivedThreads parent=%s failed: %v", parentID, err)
			continue
		}
		for _, t := range threads {
			if t == nil || t.ID == "" {
				continue
			}
			targets[t.ID] = t
			_ = upsertChannelRow(stmts.upsertChannel, stmts.upsertIDNameMapping, guildID, t, now)
		}
		log.Printf("archived-thread discovery result parent=%s count=%d", parentID, len(threads))
		saveArchivedDiscoveryState(stmts.upsertArchivedDiscoveryState, parentID, guildID, len(threads))
	}

	var list []*discordgo.Channel
	for _, c := range targets {
		list = append(list, c)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].ID < list[j].ID })

	log.Printf("backfill guild=%s channels_and_threads=%d", guildID, len(list))
	if run != nil && run.metrics != nil {
		run.metrics.addChannelsTotal(int64(len(list)))
	}

	missingAccessSkipped := 0

	for idx, c := range list {
		if run != nil && run.budgetReached() {
			return errBackfillBudgetReached
		}
		log.Printf("now syncing [%d/%d] %s", idx+1, len(list), backfillTargetLabel(c))

		lastID, err := getLastMessageID(db, c.ID)
		if err != nil {
			log.Printf("state read failed (channel=%s): %v", c.ID, err)
			lastID = ""
		}
		if lastID == "" {
			resumeID, resumeErr := getLatestStoredMessageID(db, c.ID)
			if resumeErr != nil {
				log.Printf("message history read failed (channel=%s): %v", c.ID, resumeErr)
			} else if resumeID != "" {
				lastID = resumeID
				if err := setHighWaterMark(stmts.upsertState, guildID, c.ID, resumeID); err != nil {
					log.Printf("state seed failed (channel=%s): %v", c.ID, err)
				}
				log.Printf(
					"state missing for channel=%s; resuming incremental sync from latest stored message_id=%s",
					c.ID,
					resumeID,
				)
			}
		}

		fallbackThreadParentID := ""
		if c.IsThread() {
			fallbackThreadParentID = strings.TrimSpace(c.ParentID)
		}

		inserted, newMax, budgetReached, err := backfillChannel(
			s,
			db,
			stmts,
			guildID,
			c.ID,
			lastID,
			fallbackThreadParentID,
			run,
		)
		if run != nil && run.metrics != nil {
			run.metrics.addMessagesInserted(int64(inserted))
			run.metrics.addChannelsDone(1)
		}
		if err != nil {
			if isDiscordMissingAccessError(err) {
				missingAccessSkipped++
				continue
			}
			log.Printf("backfill channel=%s failed: %v", c.ID, err)
			continue
		}
		if newMax != "" && snowflakeGreater(newMax, lastID) {
			if err := setHighWaterMark(stmts.upsertState, guildID, c.ID, newMax); err != nil {
				log.Printf("state write failed (channel=%s): %v", c.ID, err)
			}
		}

		if run != nil && run.metrics != nil {
			snap := run.metrics.snapshot()
			log.Printf(
				"backfill progress guild=%s channel=%s channels_remaining=%d requests=%d rate_limits=%d retry_after_total=%s",
				guildID,
				c.ID,
				snap.ChannelsRemaining,
				snap.RequestCount,
				snap.RateLimitCount,
				snap.RetryAfterTotal,
			)
		}

		if budgetReached {
			return errBackfillBudgetReached
		}
	}
	if missingAccessSkipped > 0 {
		log.Printf(
			"backfill skipped %d channel(s)/thread(s) due to missing access; grant View Channels + Read Message History in those channels to include them",
			missingAccessSkipped,
		)
	}

	return nil
}

func backfillTargetLabel(c *discordgo.Channel) string {
	if c == nil {
		return "target=(nil)"
	}

	name := strings.TrimSpace(c.Name)
	if name == "" {
		name = c.ID
	}

	if c.IsThread() {
		parentID := strings.TrimSpace(c.ParentID)
		if parentID != "" {
			return fmt.Sprintf("thread=%s (id=%s parent_id=%s)", name, c.ID, parentID)
		}
		return fmt.Sprintf("thread=%s (id=%s)", name, c.ID)
	}

	if !strings.HasPrefix(name, "#") {
		name = "#" + name
	}
	return fmt.Sprintf("channel=%s (id=%s)", name, c.ID)
}

func backfillChannel(
	s *discordgo.Session,
	db *sql.DB,
	stmts *preparedStatements,
	guildID, channelID, lastSeenID, fallbackThreadParentID string,
	run *backfillRun,
) (int, string, bool, error) {
	lastSeenID = strings.TrimSpace(lastSeenID)
	if lastSeenID == "" {
		return backfillChannelFullHistory(s, db, stmts, guildID, channelID, fallbackThreadParentID, run)
	}
	return backfillChannelIncremental(s, db, stmts, guildID, channelID, lastSeenID, fallbackThreadParentID, run)
}

func backfillChannelIncremental(
	s *discordgo.Session,
	db *sql.DB,
	stmts *preparedStatements,
	guildID, channelID, lastSeenID, fallbackThreadParentID string,
	run *backfillRun,
) (int, string, bool, error) {
	const pageSize = 100

	var inserted int
	maxID := lastSeenID
	cursorAfter := lastSeenID

	for {
		if run != nil && !run.consumeBackfillPage() {
			return inserted, maxID, true, nil
		}

		// ChannelMessages(channelID, limit, beforeID, afterID, aroundID)
		msgs, err := channelMessagesFetcher(s, channelID, pageSize, "", cursorAfter, "")
		if err != nil {
			return inserted, maxID, false, fmt.Errorf("ChannelMessages(after=%s): %w", cursorAfter, err)
		}
		if len(msgs) == 0 {
			break
		}

		for i := len(msgs) - 1; i >= 0; i-- {
			m := msgs[i]
			if m == nil {
				continue
			}

			if snowflakeGreater(m.ID, maxID) {
				maxID = m.ID
			}

			if m.Author == nil {
				continue
			}
			if m.Author.Bot {
				continue
			}
			didInsert, err := persistBackfillMessage(db, stmts, s, guildID, channelID, fallbackThreadParentID, m)
			if err != nil {
				return inserted, maxID, false, err
			}
			if didInsert {
				inserted++
			}
		}

		if maxID == "" || maxID == cursorAfter {
			break
		}
		cursorAfter = maxID
	}

	if inserted > 0 {
		log.Printf("backfilled channel=%s inserted=%d", channelID, inserted)
	}
	return inserted, maxID, false, nil
}

func backfillChannelFullHistory(
	s *discordgo.Session,
	db *sql.DB,
	stmts *preparedStatements,
	guildID, channelID, fallbackThreadParentID string,
	run *backfillRun,
) (int, string, bool, error) {
	const pageSize = 100

	var (
		inserted int
		maxID    string
		beforeID string
	)

	for {
		if run != nil && !run.consumeBackfillPage() {
			return inserted, maxID, true, nil
		}

		// ChannelMessages(channelID, limit, beforeID, afterID, aroundID)
		msgs, err := channelMessagesFetcher(s, channelID, pageSize, beforeID, "", "")
		if err != nil {
			return inserted, maxID, false, fmt.Errorf("ChannelMessages(before=%s): %w", beforeID, err)
		}
		if len(msgs) == 0 {
			break
		}

		oldestID := ""
		for i := len(msgs) - 1; i >= 0; i-- {
			m := msgs[i]
			if m == nil {
				continue
			}

			if snowflakeGreater(m.ID, maxID) {
				maxID = m.ID
			}
			if oldestID == "" || snowflakeGreater(oldestID, m.ID) {
				oldestID = m.ID
			}

			if m.Author == nil || m.Author.Bot {
				continue
			}

			didInsert, err := persistBackfillMessage(db, stmts, s, guildID, channelID, fallbackThreadParentID, m)
			if err != nil {
				return inserted, maxID, false, err
			}
			if didInsert {
				inserted++
			}
		}

		if oldestID == "" || oldestID == beforeID {
			break
		}
		beforeID = oldestID
	}

	if inserted > 0 {
		log.Printf("backfilled channel=%s inserted=%d", channelID, inserted)
	}
	return inserted, maxID, false, nil
}

func isDirectBackfillChannel(c *discordgo.Channel) bool {
	if c == nil {
		return false
	}
	if c.IsThread() {
		return true
	}
	switch c.Type {
	case discordgo.ChannelTypeGuildText, discordgo.ChannelTypeGuildNews:
		return true
	default:
		return false
	}
}

func canHaveArchivedThreads(c *discordgo.Channel) bool {
	if c == nil {
		return false
	}
	switch c.Type {
	case discordgo.ChannelTypeGuildText, discordgo.ChannelTypeGuildNews, discordgo.ChannelTypeGuildForum, discordgo.ChannelTypeGuildMedia:
		return true
	default:
		return false
	}
}

func archivedThreadFetchersForParent(c *discordgo.Channel) []archivedThreadPageFetcher {
	if c == nil {
		return nil
	}
	switch c.Type {
	case discordgo.ChannelTypeGuildForum, discordgo.ChannelTypeGuildMedia:
		return []archivedThreadPageFetcher{
			{name: "public", fetch: publicArchivedThreadsFetcher},
		}
	case discordgo.ChannelTypeGuildText, discordgo.ChannelTypeGuildNews:
		return []archivedThreadPageFetcher{
			{name: "public", fetch: publicArchivedThreadsFetcher},
			{name: "private", fetch: privateArchivedThreadsFetcher},
			{name: "joined_private", fetch: joinedPrivateArchivedThreadsFetcher},
		}
	default:
		return nil
	}
}

func fetchArchivedThreadsForParent(
	s *discordgo.Session,
	parent *discordgo.Channel,
	run *backfillRun,
) ([]*discordgo.Channel, error) {
	fetchers := archivedThreadFetchersForParent(parent)
	if len(fetchers) == 0 {
		return []*discordgo.Channel{}, nil
	}

	seen := make(map[string]*discordgo.Channel)
	for _, f := range fetchers {
		threads, err := fetchArchivedThreadPages(s, parent.ID, run, f)
		if err != nil {
			if errors.Is(err, errBackfillBudgetReached) {
				return nil, errBackfillBudgetReached
			}
			if suppressesArchivedThreadPermissionError(err) {
				continue
			}
			log.Printf(
				"fetchArchivedThreads parent=%s scope=%s failed: %v",
				parent.ID,
				f.name,
				err,
			)
			continue
		}
		for _, t := range threads {
			if t == nil || t.ID == "" {
				continue
			}
			seen[t.ID] = t
		}
	}

	out := make([]*discordgo.Channel, 0, len(seen))
	for _, t := range seen {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func fetchArchivedThreadPages(
	s *discordgo.Session,
	channelID string,
	run *backfillRun,
	fetcher archivedThreadPageFetcher,
) ([]*discordgo.Channel, error) {
	const pageSize = 100

	var (
		out    []*discordgo.Channel
		before *time.Time
	)
	for {
		if run != nil && !run.consumeBackfillPage() {
			return out, errBackfillBudgetReached
		}
		resp, err := fetcher.fetch(s, channelID, before, pageSize)
		if err != nil {
			return out, err
		}
		if resp == nil || len(resp.Threads) == 0 {
			break
		}
		out = append(out, resp.Threads...)
		if !resp.HasMore {
			break
		}

		oldest, ok := oldestArchiveTimestamp(resp.Threads)
		if !ok {
			break
		}
		next := oldest.Add(-time.Nanosecond)
		before = &next
	}
	return out, nil
}

func oldestArchiveTimestamp(threads []*discordgo.Channel) (time.Time, bool) {
	var (
		out time.Time
		ok  bool
	)
	for _, t := range threads {
		if t == nil {
			continue
		}
		var ts time.Time
		if t.ThreadMetadata != nil && !t.ThreadMetadata.ArchiveTimestamp.IsZero() {
			ts = t.ThreadMetadata.ArchiveTimestamp.UTC()
		} else if parsed, parsedOK := snowflakeTime(t.ID); parsedOK {
			ts = parsed
		}
		if ts.IsZero() {
			continue
		}
		if !ok || ts.Before(out) {
			out = ts
			ok = true
		}
	}
	return out, ok
}

func suppressesArchivedThreadPermissionError(err error) bool {
	if !isDiscordMissingAccessError(err) {
		return false
	}

	archivedThreadsPermissionNoticeOnce.Do(func() {
		log.Printf(
			"archived-thread backup is skipped where missing access; grant View Channels + Read Message History (and Manage Threads for private archives) to include archived threads",
		)
	})
	return true
}

func isDiscordMissingAccessError(err error) bool {
	var restErr *discordgo.RESTError
	if !errors.As(err, &restErr) {
		return false
	}
	if restErr.Message != nil && restErr.Message.Code == discordgo.ErrCodeMissingAccess {
		return true
	}
	return restErr.Response != nil && restErr.Response.StatusCode == http.StatusForbidden
}

func persistBackfillMessage(
	db *sql.DB,
	stmts *preparedStatements,
	s *discordgo.Session,
	guildID, channelID, fallbackThreadParentID string,
	m *discordgo.Message,
) (bool, error) {
	if stmts == nil || m == nil || m.Author == nil || m.Author.Bot {
		return false, nil
	}
	if m.Content == "" && len(m.Attachments) == 0 {
		return false, nil
	}

	createdAt := normalizeTimestamp(m.Timestamp)
	rel := deriveMessageRelationship(s, m, fallbackThreadParentID)
	if err := upsertUserRow(stmts.upsertUser, stmts.upsertIDNameMapping, guildID, m.Author, createdAt); err != nil {
		log.Printf("backfill user upsert failed (author=%s): %v", m.Author.ID, err)
	}
	if err := upsertGuildMemberRow(stmts.upsertGuildMember, guildID, m.Author.ID, createdAt); err != nil {
		log.Printf("backfill guild member upsert failed (author=%s): %v", m.Author.ID, err)
	}
	if m.Content != "" {
		if _, err := stmts.insertMsg.Exec(
			m.ID,
			guildID,
			channelID,
			m.Author.ID,
			createdAt,
			m.Content,
			rel.referencedMessageID,
			rel.referencedChannelID,
			rel.referencedGuildID,
			rel.threadID,
			rel.threadParentID,
		); err != nil {
			return false, fmt.Errorf("insert msg=%s: %w", m.ID, err)
		}
	}
	if len(m.Attachments) > 0 {
		_, _ = upsertMessageAttachments(
			stmts.upsertAttachment,
			m.Attachments,
			guildID,
			channelID,
			m.ID,
			m.Author.ID,
			createdAt,
			time.Now().UTC().Format(time.RFC3339Nano),
			rel,
		)
	}
	recordUserPings(stmts, guildID, channelID, m.ID, m.Author.ID, createdAt, m.Mentions)
	recordRolePings(stmts, s, guildID, channelID, m.ID, m.Author.ID, createdAt, m.MentionRoles)
	shouldRecordMessageSent := true
	if db != nil {
		var count int
		if err := db.QueryRow(
			countLifecycleByMessageAndTypeQuery,
			m.ID,
			string(eventMessageSent),
		).Scan(&count); err != nil {
			return false, fmt.Errorf("query lifecycle count for msg=%s: %w", m.ID, err)
		}
		shouldRecordMessageSent = count == 0
	}
	if shouldRecordMessageSent {
		if err := recordLifecycleEvent(
			stmts.insertEvent,
			eventMessageSent,
			guildID,
			channelID,
			m.ID,
			m.Author.ID,
			createdAt,
			map[string]any{
				"content":           m.Content,
				"attachments_count": len(m.Attachments),
				"embeds_count":      len(m.Embeds),
				"backfilled":        true,
			},
		); err != nil {
			return false, fmt.Errorf("insert lifecycle event for msg=%s: %w", m.ID, err)
		}
	}

	return true, nil
}

func fetchActiveThreads(s *discordgo.Session, guildID string, run *backfillRun) ([]*discordgo.Channel, error) {
	if run != nil {
		run.noteRequest()
	}
	return guildActiveThreadsFetcher(s, guildID)
}

// loadArchivedDiscoveryState returns a map from parent_channel_id to the time
// it was last checked for archived threads.
func loadArchivedDiscoveryState(db *sql.DB, guildID string) map[string]time.Time {
	state := make(map[string]time.Time)
	if db == nil {
		return state
	}
	rows, err := db.Query(selectArchivedDiscoveryStateByGuildQuery, guildID)
	if err != nil {
		log.Printf("loadArchivedDiscoveryState guild=%s failed: %v", guildID, err)
		return state
	}
	defer rows.Close()
	for rows.Next() {
		var parentID, lastCheckedAt string
		var threadsFound int
		if err := rows.Scan(&parentID, &lastCheckedAt, &threadsFound); err != nil {
			log.Printf("loadArchivedDiscoveryState scan failed: %v", err)
			continue
		}
		t, err := time.Parse(time.RFC3339Nano, lastCheckedAt)
		if err != nil {
			continue
		}
		state[parentID] = t
	}
	return state
}

func saveArchivedDiscoveryState(stmt *sql.Stmt, parentChannelID, guildID string, threadsFound int) {
	if stmt == nil {
		return
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := stmt.Exec(parentChannelID, guildID, now, threadsFound); err != nil {
		log.Printf("saveArchivedDiscoveryState parent=%s failed: %v", parentChannelID, err)
	}
}
