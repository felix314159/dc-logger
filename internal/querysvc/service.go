// Package querysvc provides read-only message retrieval APIs for assistant tooling.
package querysvc

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	_ "modernc.org/sqlite"
)

const maxLimit = 100

const (
	entityTypeGuild   = "guild"
	entityTypeChannel = "channel"
	entityTypeUser    = "user"
	entityTypeRole    = "role"
)

type Service struct {
	db              *sql.DB
	hasAttachments  bool
	hasUsers        bool
	hasIDNameMapper bool
	hasPingEvents   bool
	hasRoles        bool
	hasRolePings    bool
	hasMemberRoles  bool
	hasGuildMembers bool
	hasLifecycle    bool
	hasSearchFTS    bool
}

type Record struct {
	Source              string  `json:"source,omitempty"`
	MessageID           string  `json:"message_id,omitempty"`
	AttachmentID        string  `json:"attachment_id,omitempty"`
	GuildID             string  `json:"guild_id,omitempty"`
	GuildName           string  `json:"guild_name,omitempty"`
	ChannelID           string  `json:"channel_id,omitempty"`
	ChannelName         string  `json:"channel_name,omitempty"`
	AuthorID            string  `json:"author_id,omitempty"`
	AuthorName          string  `json:"author_name,omitempty"`
	TargetID            string  `json:"target_id,omitempty"`
	TargetName          string  `json:"target_name,omitempty"`
	RoleID              string  `json:"role_id,omitempty"`
	RoleName            string  `json:"role_name,omitempty"`
	Day                 string  `json:"day,omitempty"`
	CreatedAt           string  `json:"created_at,omitempty"`
	Content             string  `json:"content,omitempty"`
	Filename            string  `json:"filename,omitempty"`
	ContentType         string  `json:"content_type,omitempty"`
	URL                 string  `json:"url,omitempty"`
	ReferencedMessageID string  `json:"referenced_message_id,omitempty"`
	ReferencedChannelID string  `json:"referenced_channel_id,omitempty"`
	ReferencedGuildID   string  `json:"referenced_guild_id,omitempty"`
	ThreadID            string  `json:"thread_id,omitempty"`
	ThreadParentID      string  `json:"thread_parent_id,omitempty"`
	EventID             string  `json:"event_id,omitempty"`
	EventType           string  `json:"event_type,omitempty"`
	PayloadJSON         string  `json:"payload_json,omitempty"`
	Term                string  `json:"term,omitempty"`
	HitCount            int     `json:"hit_count,omitempty"`
	MessageCount        int     `json:"message_count,omitempty"`
	UniqueAuthors       int     `json:"unique_authors,omitempty"`
	UniqueChannels      int     `json:"unique_channels,omitempty"`
	Percentage          float64 `json:"percentage,omitempty"`
	FirstSeenAt         string  `json:"first_seen_at,omitempty"`
	LastSeenAt          string  `json:"last_seen_at,omitempty"`
}

func Open(dbPath string) (*Service, error) {
	if dbPath == "" {
		return nil, fmt.Errorf("db path is required")
	}
	if _, err := os.Stat(dbPath); err != nil {
		return nil, fmt.Errorf("db path %q not accessible: %w", dbPath, err)
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, err
	}

	// Enforce read-only behavior for this helper process.
	for _, p := range readOnlyPragmas {
		if _, err := db.Exec(p); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("pragma failed (%s): %w", p, err)
		}
	}

	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	hasAttachments, err := tableExists(db, "attachments")
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	hasUsers, err := tableExists(db, "users")
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	hasIDNameMapper, err := tableExists(db, "id_name_mappings")
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	hasPingEvents, err := tableExists(db, "ping_events")
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	hasRoles, err := tableExists(db, "roles")
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	hasRolePings, err := tableExists(db, "role_ping_events")
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	hasMemberRoles, err := tableExists(db, "member_roles")
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	hasGuildMembers, err := tableExists(db, "guild_members")
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	hasLifecycle, err := tableExists(db, "lifecycle_events")
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	hasSearchFTS, err := tableExists(db, "message_search_fts")
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	return &Service{
		db:              db,
		hasAttachments:  hasAttachments,
		hasUsers:        hasUsers,
		hasIDNameMapper: hasIDNameMapper,
		hasPingEvents:   hasPingEvents,
		hasRoles:        hasRoles,
		hasRolePings:    hasRolePings,
		hasMemberRoles:  hasMemberRoles,
		hasGuildMembers: hasGuildMembers,
		hasLifecycle:    hasLifecycle,
		hasSearchFTS:    hasSearchFTS,
	}, nil
}

func (s *Service) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Service) RecentMessages(ctx context.Context, guildID string, limit int) ([]Record, error) {
	return s.RecentMessagesFiltered(ctx, guildID, "", "", limit)
}

func (s *Service) RecentMessagesFiltered(
	ctx context.Context,
	guildID, since, until string,
	limit int,
) ([]Record, error) {
	return s.RecentMessagesWindow(ctx, guildID, since, until, "", "", limit)
}

func (s *Service) RecentMessagesWindow(
	ctx context.Context,
	guildID, since, until, beforeTime, beforeID string,
	limit int,
) ([]Record, error) {
	return s.queryCombined(
		ctx,
		"m.guild_id = ?",
		"a.guild_id = ?",
		[]any{guildID},
		[]any{guildID},
		since,
		until,
		beforeTime,
		beforeID,
		limit,
	)
}

func (s *Service) RecentMessagesInChannel(ctx context.Context, guildID, channelID string, limit int) ([]Record, error) {
	return s.RecentMessagesInChannelFiltered(ctx, guildID, channelID, "", "", limit)
}

func (s *Service) RecentMessagesInChannelFiltered(
	ctx context.Context,
	guildID, channelID, since, until string,
	limit int,
) ([]Record, error) {
	return s.RecentMessagesInChannelWindow(ctx, guildID, channelID, since, until, "", "", limit)
}

func (s *Service) RecentMessagesInChannelWindow(
	ctx context.Context,
	guildID, channelID, since, until, beforeTime, beforeID string,
	limit int,
) ([]Record, error) {
	return s.queryCombined(
		ctx,
		"m.guild_id = ? AND m.channel_id = ?",
		"a.guild_id = ? AND a.channel_id = ?",
		[]any{guildID, channelID},
		[]any{guildID, channelID},
		since,
		until,
		beforeTime,
		beforeID,
		limit,
	)
}

func (s *Service) RecentMessagesByUser(ctx context.Context, guildID, authorID string, limit int) ([]Record, error) {
	return s.RecentMessagesByUserFiltered(ctx, guildID, authorID, "", "", limit)
}

func (s *Service) RecentMessagesByUserFiltered(
	ctx context.Context,
	guildID, authorID, since, until string,
	limit int,
) ([]Record, error) {
	return s.RecentMessagesByUserWindow(ctx, guildID, authorID, since, until, "", "", limit)
}

func (s *Service) RecentMessagesByUserWindow(
	ctx context.Context,
	guildID, authorID, since, until, beforeTime, beforeID string,
	limit int,
) ([]Record, error) {
	return s.queryCombined(
		ctx,
		"m.guild_id = ? AND m.author_id = ?",
		"a.guild_id = ? AND a.author_id = ?",
		[]any{guildID, authorID},
		[]any{guildID, authorID},
		since,
		until,
		beforeTime,
		beforeID,
		limit,
	)
}

func (s *Service) LastMessageByUser(ctx context.Context, guildID, authorID string) (*Record, error) {
	rows, err := s.RecentMessagesByUser(ctx, guildID, authorID, 1)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	return &rows[0], nil
}

func (s *Service) SearchMessages(ctx context.Context, guildID, query string, limit int) ([]Record, error) {
	return s.SearchMessagesFiltered(ctx, guildID, query, "", "", limit)
}

func (s *Service) TopicActivitySummary(
	ctx context.Context,
	guildID, query, since, until string,
	limit int,
) ([]Record, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("service is not initialized")
	}
	trimmed := strings.TrimSpace(query)
	if trimmed == "" {
		return nil, fmt.Errorf("search query is required")
	}
	var err error
	since, until, err = normalizeTimeRange(since, until)
	if err != nil {
		return nil, err
	}
	limit = clampLimit(limit)

	var rows []Record
	if s.hasSearchFTS {
		rows, err = s.topicActivitySummaryFTS(ctx, guildID, trimmed, since, until, limit)
	} else {
		rows, err = s.topicActivitySummaryLike(ctx, guildID, trimmed, since, until, limit)
	}
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 || !s.hasIDNameMapper {
		return rows, nil
	}

	authorIDs := make([]string, 0, len(rows))
	seen := make(map[string]struct{}, len(rows))
	for _, r := range rows {
		id := strings.TrimSpace(r.AuthorID)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		authorIDs = append(authorIDs, id)
	}
	if len(authorIDs) == 0 {
		return rows, nil
	}
	nameMap, err := s.lookupNamesByID(ctx, guildID, entityTypeUser, authorIDs)
	if err != nil {
		return rows, nil
	}
	for i := range rows {
		if rows[i].AuthorName != "" {
			continue
		}
		if mapped := strings.TrimSpace(nameMap[rows[i].AuthorID]); mapped != "" {
			rows[i].AuthorName = mapped
		}
	}
	return rows, nil
}

func (s *Service) SearchMessagesFiltered(
	ctx context.Context,
	guildID, query, since, until string,
	limit int,
) ([]Record, error) {
	return s.SearchMessagesWindow(ctx, guildID, query, since, until, "", "", limit)
}

func (s *Service) SearchMessagesWindow(
	ctx context.Context,
	guildID, query, since, until, beforeTime, beforeID string,
	limit int,
) ([]Record, error) {
	trimmed := strings.TrimSpace(query)
	if trimmed == "" {
		return nil, fmt.Errorf("search query is required")
	}
	since, until, err := normalizeTimeRange(since, until)
	if err != nil {
		return nil, err
	}
	beforeTime, beforeID, err = normalizeBeforeCursor(beforeTime, beforeID)
	if err != nil {
		return nil, err
	}
	if s.hasSearchFTS {
		return s.searchMessagesFTS(ctx, guildID, trimmed, since, until, beforeTime, beforeID, limit)
	}

	pattern := "%" + escapeLikePattern(trimmed) + "%"
	return s.queryCombined(
		ctx,
		"m.guild_id = ? AND m.content LIKE ? ESCAPE '\\'",
		"a.guild_id = ? AND ((a.content_text != '' AND a.content_text LIKE ? ESCAPE '\\') OR (a.content_text = '' AND a.filename LIKE ? ESCAPE '\\'))",
		[]any{guildID, pattern},
		[]any{guildID, pattern, pattern},
		since,
		until,
		beforeTime,
		beforeID,
		limit,
	)
}

func (s *Service) EnrichNames(ctx context.Context, guildID string, rows []Record) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("service is not initialized")
	}
	guildID = strings.TrimSpace(guildID)
	if !s.hasIDNameMapper || guildID == "" || len(rows) == 0 {
		return nil
	}

	authorSet := make(map[string]struct{})
	channelSet := make(map[string]struct{})
	for _, row := range rows {
		if id := strings.TrimSpace(row.AuthorID); id != "" {
			authorSet[id] = struct{}{}
		}
		if id := strings.TrimSpace(row.ChannelID); id != "" {
			channelSet[id] = struct{}{}
		}
	}

	authorMap, err := s.lookupNamesByID(ctx, guildID, entityTypeUser, mapKeys(authorSet))
	if err != nil {
		return err
	}
	channelMap, err := s.lookupNamesByID(ctx, guildID, entityTypeChannel, mapKeys(channelSet))
	if err != nil {
		return err
	}
	guildMap, err := s.lookupNamesByID(ctx, guildID, entityTypeGuild, []string{guildID})
	if err != nil {
		return err
	}
	guildName := strings.TrimSpace(guildMap[guildID])

	for i := range rows {
		if rows[i].AuthorName == "" {
			rows[i].AuthorName = strings.TrimSpace(authorMap[rows[i].AuthorID])
		}
		if rows[i].ChannelName == "" {
			rows[i].ChannelName = strings.TrimSpace(channelMap[rows[i].ChannelID])
		}
		if rows[i].GuildName == "" {
			rows[i].GuildName = guildName
		}
	}
	return nil
}

func (s *Service) ServerActivitySummary(
	ctx context.Context,
	guildID, since, until string,
) ([]Record, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("service is not initialized")
	}
	var err error
	since, until, err = normalizeTimeRange(since, until)
	if err != nil {
		return nil, err
	}

	sinceFilter := ""
	untilFilter := ""
	args := []any{guildID}
	if since != "" {
		sinceFilter = messageTimeSinceFilterQuery
		args = append(args, since)
	}
	if until != "" {
		untilFilter = messageTimeUntilFilterQuery
		args = append(args, until)
	}

	query := fmt.Sprintf(serverActivitySummaryQueryTemplate, sinceFilter, untilFilter)

	var (
		messageCount   int
		uniqueAuthors  int
		uniqueChannels int
		firstSeenAt    string
		lastSeenAt     string
	)
	if err := s.db.QueryRowContext(ctx, query, args...).Scan(
		&messageCount,
		&uniqueAuthors,
		&uniqueChannels,
		&firstSeenAt,
		&lastSeenAt,
	); err != nil {
		return nil, err
	}

	out := []Record{
		{
			Source:         "server_activity_summary",
			GuildID:        guildID,
			MessageCount:   messageCount,
			UniqueAuthors:  uniqueAuthors,
			UniqueChannels: uniqueChannels,
			FirstSeenAt:    firstSeenAt,
			LastSeenAt:     lastSeenAt,
		},
	}
	if s.hasIDNameMapper {
		_ = s.EnrichNames(ctx, guildID, out)
	}
	return out, nil
}

func (s *Service) ServerActivitySummaryByDay(
	ctx context.Context,
	guildID, since, until string,
) ([]Record, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("service is not initialized")
	}
	var err error
	since, until, err = normalizeTimeRange(since, until)
	if err != nil {
		return nil, err
	}

	sinceFilter := ""
	untilFilter := ""
	args := []any{guildID}
	if since != "" {
		sinceFilter = messageTimeSinceFilterQuery
		args = append(args, since)
	}
	if until != "" {
		untilFilter = messageTimeUntilFilterQuery
		args = append(args, until)
	}

	query := fmt.Sprintf(serverActivitySummaryByDayQueryTemplate, sinceFilter, untilFilter)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]Record, 0)
	for rows.Next() {
		var (
			day            string
			messageCount   int
			uniqueAuthors  int
			uniqueChannels int
			firstSeenAt    string
			lastSeenAt     string
		)
		if err := rows.Scan(
			&day,
			&messageCount,
			&uniqueAuthors,
			&uniqueChannels,
			&firstSeenAt,
			&lastSeenAt,
		); err != nil {
			return nil, err
		}
		out = append(out, Record{
			Source:         "server_activity_summary_by_day",
			GuildID:        guildID,
			Day:            day,
			MessageCount:   messageCount,
			UniqueAuthors:  uniqueAuthors,
			UniqueChannels: uniqueChannels,
			FirstSeenAt:    firstSeenAt,
			LastSeenAt:     lastSeenAt,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if s.hasIDNameMapper {
		_ = s.EnrichNames(ctx, guildID, out)
	}
	return out, nil
}

func (s *Service) ChannelActivitySummary(
	ctx context.Context,
	guildID, since, until string,
	limit int,
) ([]Record, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("service is not initialized")
	}
	var err error
	since, until, err = normalizeTimeRange(since, until)
	if err != nil {
		return nil, err
	}
	limit = clampLimit(limit)

	sinceFilter := ""
	untilFilter := ""
	args := []any{guildID}
	if since != "" {
		sinceFilter = messageTimeSinceFilterQuery
		args = append(args, since)
	}
	if until != "" {
		untilFilter = messageTimeUntilFilterQuery
		args = append(args, until)
	}

	channelNameExpr := "''"
	channelNameJoin := ""
	if s.hasIDNameMapper {
		channelNameExpr = "COALESCE(ch_map.human_name, '')"
		channelNameJoin = `
LEFT JOIN id_name_mappings ch_map
	ON ch_map.entity_type = 'channel'
	AND ch_map.guild_id = c.guild_id
	AND ch_map.entity_id = c.channel_id`
	}

	query := fmt.Sprintf(
		channelActivitySummaryQueryTemplate,
		sinceFilter,
		untilFilter,
		channelNameExpr,
		channelNameJoin,
	)
	args = append(args, limit)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]Record, 0, limit)
	for rows.Next() {
		var (
			channelID   string
			channelName string
			messageCnt  int
			percentage  float64
			firstSeenAt string
			lastSeenAt  string
		)
		if err := rows.Scan(
			&channelID,
			&channelName,
			&messageCnt,
			&percentage,
			&firstSeenAt,
			&lastSeenAt,
		); err != nil {
			return nil, err
		}
		out = append(out, Record{
			Source:       "channel_activity_summary",
			GuildID:      guildID,
			ChannelID:    channelID,
			ChannelName:  channelName,
			MessageCount: messageCnt,
			Percentage:   percentage,
			FirstSeenAt:  firstSeenAt,
			LastSeenAt:   lastSeenAt,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if s.hasIDNameMapper {
		_ = s.EnrichNames(ctx, guildID, out)
	}
	return out, nil
}

func (s *Service) AuthorActivitySummary(
	ctx context.Context,
	guildID, since, until string,
	limit int,
) ([]Record, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("service is not initialized")
	}
	var err error
	since, until, err = normalizeTimeRange(since, until)
	if err != nil {
		return nil, err
	}
	limit = clampLimit(limit)

	sinceFilter := ""
	untilFilter := ""
	args := []any{guildID}
	if since != "" {
		sinceFilter = messageTimeSinceFilterQuery
		args = append(args, since)
	}
	if until != "" {
		untilFilter = messageTimeUntilFilterQuery
		args = append(args, until)
	}

	authorNameExpr := "''"
	authorNameJoin := ""
	if s.hasIDNameMapper {
		authorNameExpr = "COALESCE(u_map.human_name, '')"
		authorNameJoin = `
LEFT JOIN id_name_mappings u_map
	ON u_map.entity_type = 'user'
	AND u_map.guild_id = c.guild_id
	AND u_map.entity_id = c.author_id`
	}

	query := fmt.Sprintf(
		authorActivitySummaryQueryTemplate,
		sinceFilter,
		untilFilter,
		authorNameExpr,
		authorNameJoin,
	)
	args = append(args, limit)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]Record, 0, limit)
	for rows.Next() {
		var (
			authorID    string
			authorName  string
			messageCnt  int
			percentage  float64
			firstSeenAt string
			lastSeenAt  string
		)
		if err := rows.Scan(
			&authorID,
			&authorName,
			&messageCnt,
			&percentage,
			&firstSeenAt,
			&lastSeenAt,
		); err != nil {
			return nil, err
		}
		out = append(out, Record{
			Source:       "author_activity_summary",
			GuildID:      guildID,
			AuthorID:     authorID,
			AuthorName:   authorName,
			MessageCount: messageCnt,
			Percentage:   percentage,
			FirstSeenAt:  firstSeenAt,
			LastSeenAt:   lastSeenAt,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if s.hasIDNameMapper {
		_ = s.EnrichNames(ctx, guildID, out)
	}
	return out, nil
}

func (s *Service) KeywordFrequency(
	ctx context.Context,
	guildID, since, until string,
	minLength int,
	stopwordList []string,
	limit int,
) ([]Record, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("service is not initialized")
	}
	var err error
	since, until, err = normalizeTimeRange(since, until)
	if err != nil {
		return nil, err
	}
	limit = clampLimit(limit)
	if minLength <= 0 {
		minLength = 4
	}

	texts, err := s.collectKeywordTexts(ctx, guildID, since, until)
	if err != nil {
		return nil, err
	}
	if len(texts) == 0 {
		return []Record{}, nil
	}

	stopwords := make(map[string]struct{}, len(stopwordList))
	for _, word := range stopwordList {
		word = strings.ToLower(strings.TrimSpace(word))
		if word == "" {
			continue
		}
		stopwords[word] = struct{}{}
	}

	counts := make(map[string]int)
	for _, text := range texts {
		terms := keywordTokenPattern.FindAllString(strings.ToLower(text), -1)
		for _, term := range terms {
			if len([]rune(term)) < minLength {
				continue
			}
			if _, blocked := stopwords[term]; blocked {
				continue
			}
			counts[term]++
		}
	}
	if len(counts) == 0 {
		return []Record{}, nil
	}

	type keywordCount struct {
		term  string
		count int
	}
	pairs := make([]keywordCount, 0, len(counts))
	for term, count := range counts {
		pairs = append(pairs, keywordCount{term: term, count: count})
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].count == pairs[j].count {
			return pairs[i].term < pairs[j].term
		}
		return pairs[i].count > pairs[j].count
	})
	if len(pairs) > limit {
		pairs = pairs[:limit]
	}

	out := make([]Record, 0, len(pairs))
	for _, pair := range pairs {
		out = append(out, Record{
			Source:   "keyword_frequency",
			GuildID:  guildID,
			Term:     pair.term,
			HitCount: pair.count,
		})
	}
	if s.hasIDNameMapper {
		_ = s.EnrichNames(ctx, guildID, out)
	}
	return out, nil
}

func (s *Service) collectKeywordTexts(
	ctx context.Context,
	guildID, since, until string,
) ([]string, error) {
	sinceFilter := ""
	untilFilter := ""

	messageArgs := []any{guildID}
	if since != "" {
		sinceFilter = messageTimeSinceFilterQuery
		messageArgs = append(messageArgs, since)
	}
	if until != "" {
		untilFilter = messageTimeUntilFilterQuery
		messageArgs = append(messageArgs, until)
	}
	messageQuery := fmt.Sprintf(keywordFrequencyMessagesQueryTemplate, sinceFilter, untilFilter)

	texts := make([]string, 0)
	rows, err := s.db.QueryContext(ctx, messageQuery, messageArgs...)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var content string
		if err := rows.Scan(&content); err != nil {
			_ = rows.Close()
			return nil, err
		}
		content = strings.TrimSpace(content)
		if content != "" {
			texts = append(texts, content)
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}

	if !s.hasAttachments {
		return texts, nil
	}

	attachmentArgs := []any{guildID}
	if since != "" {
		attachmentArgs = append(attachmentArgs, since)
	}
	if until != "" {
		attachmentArgs = append(attachmentArgs, until)
	}
	attachmentQuery := fmt.Sprintf(keywordFrequencyAttachmentsQueryTemplate, sinceFilter, untilFilter)
	attachmentRows, err := s.db.QueryContext(ctx, attachmentQuery, attachmentArgs...)
	if err != nil {
		return nil, err
	}
	for attachmentRows.Next() {
		var content string
		if err := attachmentRows.Scan(&content); err != nil {
			_ = attachmentRows.Close()
			return nil, err
		}
		content = strings.TrimSpace(content)
		if content != "" {
			texts = append(texts, content)
		}
	}
	if err := attachmentRows.Err(); err != nil {
		_ = attachmentRows.Close()
		return nil, err
	}
	if err := attachmentRows.Close(); err != nil {
		return nil, err
	}
	return texts, nil
}

func (s *Service) RecentLifecycleEvents(
	ctx context.Context,
	guildID, eventType, actorID, channelID, since, until string,
	limit int,
) ([]Record, error) {
	return s.RecentLifecycleEventsWindow(ctx, guildID, eventType, actorID, channelID, since, until, "", "", limit)
}

func (s *Service) RecentLifecycleEventsWindow(
	ctx context.Context,
	guildID, eventType, actorID, channelID, since, until, beforeTime, beforeID string,
	limit int,
) ([]Record, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("service is not initialized")
	}
	if !s.hasLifecycle {
		return []Record{}, nil
	}

	since, until, err := normalizeTimeRange(since, until)
	if err != nil {
		return nil, err
	}
	beforeTime, beforeID, err = normalizeBeforeCursor(beforeTime, beforeID)
	if err != nil {
		return nil, err
	}
	limit = clampLimit(limit)

	authorNameExpr := lifecycleEventAuthorNameDefault
	userJoin := ""
	if s.hasIDNameMapper {
		authorNameExpr = lifecycleEventAuthorNameMapped
		userJoin = recentLifecycleEventsUserJoinQuery
	}

	query := fmt.Sprintf(
		recentLifecycleEventsQueryTemplate,
		lifecycleEventSourceValue,
		authorNameExpr,
		userJoin,
	)
	args := []any{guildID}

	if eventType = strings.TrimSpace(eventType); eventType != "" {
		query += lifecycleTypeFilterQuery
		args = append(args, eventType)
	}
	if actorID = strings.TrimSpace(actorID); actorID != "" {
		query += lifecycleActorFilterQuery
		args = append(args, actorID)
	}
	if channelID = strings.TrimSpace(channelID); channelID != "" {
		query += lifecycleChannelFilterQuery
		args = append(args, channelID)
	}
	if since != "" {
		query += lifecycleTimeSinceFilterQuery
		args = append(args, since)
	}
	if until != "" {
		query += lifecycleTimeUntilFilterQuery
		args = append(args, until)
	}
	if beforeTime != "" {
		if beforeID != "" {
			lifecycleID, convErr := parsePositiveInt64(beforeID)
			if convErr != nil {
				return nil, fmt.Errorf("invalid before_id for lifecycle query %q: %w", beforeID, convErr)
			}
			query += lifecycleBeforeTimeIDFilterQuery
			args = append(args, beforeTime, beforeTime, lifecycleID)
		} else {
			query += lifecycleBeforeTimeFilterQuery
			args = append(args, beforeTime)
		}
	}
	query += lifecycleOrderByLimitQuery
	args = append(args, limit)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]Record, 0, limit)
	for rows.Next() {
		var (
			r           Record
			occurredAt  string
			eventID     int64
			eventTypeV  string
			payloadJSON string
		)
		if err := rows.Scan(
			&r.Source,
			&r.MessageID,
			&r.GuildID,
			&r.ChannelID,
			&r.AuthorID,
			&r.AuthorName,
			&occurredAt,
			&eventID,
			&eventTypeV,
			&payloadJSON,
		); err != nil {
			return nil, err
		}
		r.CreatedAt = occurredAt
		r.EventID = strconv.FormatInt(eventID, 10)
		r.EventType = eventTypeV
		r.PayloadJSON = payloadJSON
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Service) RecentPingsByUser(ctx context.Context, guildID, actorID string, limit int) ([]Record, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("service is not initialized")
	}
	if !s.hasPingEvents {
		return []Record{}, nil
	}
	limit = clampLimit(limit)
	rows, err := s.scanRecords(ctx, recentPingsByUserQuery, guildID, actorID, limit)
	if err != nil {
		return nil, err
	}
	_ = s.humanizePingRows(ctx, guildID, rows)
	return rows, nil
}

func (s *Service) RecentPingsTargetingUser(ctx context.Context, guildID, targetID string, limit int) ([]Record, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("service is not initialized")
	}
	if !s.hasPingEvents && !(s.hasRolePings && s.hasMemberRoles) {
		return []Record{}, nil
	}
	limit = clampLimit(limit)
	branches := make([]string, 0, 2)
	args := make([]any, 0, 8)

	if s.hasPingEvents {
		branches = append(branches, recentPingsTargetingUserDirectBranchQuery)
		args = append(args, guildID, targetID)
	}

	if s.hasRolePings && s.hasMemberRoles {
		branches = append(branches, recentPingsTargetingUserRoleBranchQuery)
		args = append(args, guildID, targetID)
	}

	if len(branches) == 0 {
		return []Record{}, nil
	}

	fetchLimit := limit
	if len(branches) > 1 {
		fetchLimit = limit * 3
	}
	query := fmt.Sprintf(
		recentPingsTargetingUserCombinedQueryTemplate,
		strings.Join(branches, "\nUNION ALL\n"),
	)
	args = append(args, fetchLimit)
	rows, err := s.scanRecords(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	if len(branches) > 1 {
		rows = dedupePingRows(rows)
		if len(rows) > limit {
			rows = rows[:limit]
		}
	}
	if len(rows) > 0 {
		if nameMap, err := s.lookupNamesByID(ctx, guildID, entityTypeUser, []string{targetID}); err == nil {
			targetName := strings.TrimSpace(nameMap[targetID])
			if targetName != "" {
				for i := range rows {
					if rows[i].TargetID == targetID && strings.TrimSpace(rows[i].TargetName) == "" {
						rows[i].TargetName = targetName
					}
				}
			}
		}
	}
	_ = s.humanizePingRows(ctx, guildID, rows)
	return rows, nil
}

func (s *Service) UnansweredPingsTargetingUser(
	ctx context.Context,
	guildID, targetID, since, until, beforeTime, beforeID string,
	limit int,
) ([]Record, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("service is not initialized")
	}
	if !s.hasPingEvents && !(s.hasRolePings && s.hasMemberRoles) {
		return []Record{}, nil
	}
	var err error
	since, until, err = normalizeTimeRange(since, until)
	if err != nil {
		return nil, err
	}
	beforeTime, beforeID, err = normalizeBeforeCursor(beforeTime, beforeID)
	if err != nil {
		return nil, err
	}
	limit = clampLimit(limit)

	branches := make([]string, 0, 2)
	args := make([]any, 0, 16)
	appendBranchFilters := func(alias, idColumn string) (string, []any) {
		queryArgs := make([]any, 0, 6)
		var b strings.Builder
		if since != "" {
			b.WriteString(" AND ")
			b.WriteString(alias)
			b.WriteString(".occurred_at >= ?")
			queryArgs = append(queryArgs, since)
		}
		if until != "" {
			b.WriteString(" AND ")
			b.WriteString(alias)
			b.WriteString(".occurred_at <= ?")
			queryArgs = append(queryArgs, until)
		}
		if beforeTime != "" {
			if beforeID != "" {
				b.WriteString(" AND (")
				b.WriteString(alias)
				b.WriteString(".occurred_at < ? OR (")
				b.WriteString(alias)
				b.WriteString(".occurred_at = ? AND ")
				b.WriteString(alias)
				b.WriteString(".")
				b.WriteString(idColumn)
				b.WriteString(" < ?))")
				queryArgs = append(queryArgs, beforeTime, beforeTime, beforeID)
			} else {
				b.WriteString(" AND ")
				b.WriteString(alias)
				b.WriteString(".occurred_at < ?")
				queryArgs = append(queryArgs, beforeTime)
			}
		}
		return b.String(), queryArgs
	}

	if s.hasPingEvents {
		filters, filterArgs := appendBranchFilters("pe", "message_id")
		branches = append(branches, fmt.Sprintf(unansweredPingsTargetingUserDirectBranchQueryTemplate, filters))
		args = append(args, guildID, targetID)
		args = append(args, filterArgs...)
	}

	if s.hasRolePings && s.hasMemberRoles {
		filters, filterArgs := appendBranchFilters("rpe", "message_id")
		branches = append(branches, fmt.Sprintf(unansweredPingsTargetingUserRoleBranchQueryTemplate, filters))
		args = append(args, guildID, targetID)
		args = append(args, filterArgs...)
	}

	if len(branches) == 0 {
		return []Record{}, nil
	}

	fetchLimit := limit
	if len(branches) > 1 {
		fetchLimit = limit * 3
	}
	query := fmt.Sprintf(
		recentPingsTargetingUserCombinedQueryTemplate,
		strings.Join(branches, "\nUNION ALL\n"),
	)
	args = append(args, fetchLimit)
	rows, err := s.scanRecords(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	if len(branches) > 1 {
		rows = dedupePingRows(rows)
		if len(rows) > limit {
			rows = rows[:limit]
		}
	}
	if len(rows) > 0 {
		if nameMap, nameErr := s.lookupNamesByID(ctx, guildID, entityTypeUser, []string{targetID}); nameErr == nil {
			targetName := strings.TrimSpace(nameMap[targetID])
			if targetName != "" {
				for i := range rows {
					if rows[i].TargetID == targetID && strings.TrimSpace(rows[i].TargetName) == "" {
						rows[i].TargetName = targetName
					}
				}
			}
		}
	}
	_ = s.humanizePingRows(ctx, guildID, rows)
	return rows, nil
}

func (s *Service) LastPingedUserByUser(ctx context.Context, guildID, actorID string) (*Record, error) {
	rows, err := s.RecentPingsByUser(ctx, guildID, actorID, 1)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	return &rows[0], nil
}

func (s *Service) RolesOfUser(ctx context.Context, guildID, userID string, limit int) ([]Record, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("service is not initialized")
	}
	if !s.hasMemberRoles {
		return []Record{}, nil
	}
	limit = clampLimit(limit)
	userNameExpr := "''"
	userJoin := ""
	roleNameExpr := "''"
	roleMapJoin := ""
	if s.hasIDNameMapper {
		userNameExpr = "COALESCE(u_map.human_name, '')"
		userJoin = `
LEFT JOIN id_name_mappings u_map
	ON u_map.entity_type = 'user'
	AND u_map.guild_id = mr.guild_id
	AND u_map.entity_id = mr.user_id`
		roleNameExpr = "COALESCE(r_map.human_name, '')"
		roleMapJoin = `
LEFT JOIN id_name_mappings r_map
	ON r_map.entity_type = 'role'
	AND r_map.guild_id = mr.guild_id
	AND r_map.entity_id = mr.role_id`
	}
	roleJoin := ""
	if s.hasRoles {
		if roleNameExpr == "''" {
			roleNameExpr = "COALESCE(r.name, '')"
		} else {
			roleNameExpr = "COALESCE(r_map.human_name, r.name, '')"
		}
		roleJoin = `
LEFT JOIN roles r
	ON r.guild_id = mr.guild_id
	AND r.role_id = mr.role_id`
	}
	query := fmt.Sprintf(
		rolesOfUserQueryTemplate,
		userNameExpr, roleNameExpr, userJoin, roleMapJoin, roleJoin,
	)
	return s.scanRecords(ctx, query, guildID, userID, limit)
}

func (s *Service) UsersWithRole(ctx context.Context, guildID, roleID string, limit int) ([]Record, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("service is not initialized")
	}
	if !s.hasMemberRoles {
		return []Record{}, nil
	}
	limit = clampLimit(limit)
	userNameExpr := "''"
	userJoin := ""
	roleNameExpr := "''"
	roleMapJoin := ""
	if s.hasIDNameMapper {
		userNameExpr = "COALESCE(u_map.human_name, '')"
		userJoin = `
LEFT JOIN id_name_mappings u_map
	ON u_map.entity_type = 'user'
	AND u_map.guild_id = mr.guild_id
	AND u_map.entity_id = mr.user_id`
		roleNameExpr = "COALESCE(r_map.human_name, '')"
		roleMapJoin = `
LEFT JOIN id_name_mappings r_map
	ON r_map.entity_type = 'role'
	AND r_map.guild_id = mr.guild_id
	AND r_map.entity_id = mr.role_id`
	}
	roleJoin := ""
	if s.hasRoles {
		if roleNameExpr == "''" {
			roleNameExpr = "COALESCE(r.name, '')"
		} else {
			roleNameExpr = "COALESCE(r_map.human_name, r.name, '')"
		}
		roleJoin = `
LEFT JOIN roles r
	ON r.guild_id = mr.guild_id
	AND r.role_id = mr.role_id`
	}
	query := fmt.Sprintf(
		usersWithRoleQueryTemplate,
		userNameExpr, roleNameExpr, userJoin, roleMapJoin, roleJoin,
	)
	return s.scanRecords(ctx, query, guildID, roleID, limit)
}

func (s *Service) ListGuildMembers(ctx context.Context, guildID string, limit int) ([]Record, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("service is not initialized")
	}
	if !s.hasGuildMembers {
		return []Record{}, nil
	}
	limit = clampLimit(limit)

	userNameExpr := "''"
	userJoin := ""
	if s.hasIDNameMapper {
		userNameExpr = "COALESCE(u_map.human_name, '')"
		userJoin = `
LEFT JOIN id_name_mappings u_map
	ON u_map.entity_type = 'user'
	AND u_map.guild_id = gm.guild_id
	AND u_map.entity_id = gm.user_id`
	}
	if s.hasUsers {
		if userNameExpr == "''" {
			userNameExpr = "COALESCE(CASE WHEN u.global_name != '' THEN u.global_name ELSE u.username END, '')"
		} else {
			userNameExpr = "COALESCE(u_map.human_name, CASE WHEN u.global_name != '' THEN u.global_name ELSE u.username END, '')"
		}
		userJoin += `
LEFT JOIN users u
	ON u.author_id = gm.user_id`
	}

	query := fmt.Sprintf(listGuildMembersQueryTemplate, userNameExpr, userJoin)

	return s.scanRecords(ctx, query, guildID, limit)
}

func (s *Service) ResolveGuildIDByName(ctx context.Context, guildName string) (string, error) {
	return s.resolveEntityIDByName(ctx, entityTypeGuild, "", guildName)
}

func (s *Service) ResolveChannelIDByName(ctx context.Context, guildID, channelName string) (string, error) {
	return s.resolveEntityIDByName(ctx, entityTypeChannel, guildID, channelName)
}

func (s *Service) ResolveAuthorIDByName(ctx context.Context, guildID, authorName string) (string, error) {
	return s.resolveEntityIDByName(ctx, entityTypeUser, guildID, authorName)
}

func (s *Service) ResolveRoleIDByName(ctx context.Context, guildID, roleName string) (string, error) {
	return s.resolveEntityIDByName(ctx, entityTypeRole, guildID, roleName)
}

func (s *Service) MostCommonGuildID(ctx context.Context) (string, error) {
	if s == nil || s.db == nil {
		return "", fmt.Errorf("service is not initialized")
	}

	query := mostCommonGuildIDFromMessagesQuery
	if s.hasAttachments {
		query = mostCommonGuildIDWithAttachmentsQuery
	}

	var guildID string
	err := s.db.QueryRowContext(ctx, query).Scan(&guildID)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return guildID, nil
}

func (s *Service) resolveEntityIDByName(
	ctx context.Context,
	entityType, guildID, humanName string,
) (string, error) {
	if s == nil || s.db == nil {
		return "", fmt.Errorf("service is not initialized")
	}
	entityType = strings.TrimSpace(entityType)
	guildID = strings.TrimSpace(guildID)
	humanName = strings.TrimSpace(humanName)
	if entityType == "" {
		return "", fmt.Errorf("entity type is required")
	}
	if humanName == "" {
		return "", fmt.Errorf("human-readable name is required")
	}

	normalizedName := normalizeHumanName(humanName)
	if normalizedName == "" {
		return "", fmt.Errorf("human-readable name is required")
	}

	if s.hasIDNameMapper {
		var (
			query string
			args  []any
		)

		if entityType == entityTypeGuild {
			query = resolveEntityIDByNameGuildQuery
			args = []any{entityType, normalizedName}
		} else {
			if guildID == "" {
				return "", fmt.Errorf("guild id is required")
			}
			query = resolveEntityIDByNameScopedQuery
			args = []any{entityType, guildID, normalizedName}
		}

		var entityID string
		err := s.db.QueryRowContext(ctx, query, args...).Scan(&entityID)
		if err == nil {
			return entityID, nil
		}
		if err != sql.ErrNoRows {
			return "", err
		}
	}

	// Backward-compatible fallback when id_name_mappings is not available.
	switch entityType {
	case entityTypeChannel:
		if guildID == "" {
			return "", fmt.Errorf("guild id is required")
		}
		var channelID string
		err := s.db.QueryRowContext(
			ctx,
			resolveChannelIDByNameFallbackQuery,
			guildID, normalizedName,
		).Scan(&channelID)
		if err == sql.ErrNoRows {
			return "", nil
		}
		return channelID, err
	case entityTypeUser:
		if guildID == "" {
			return "", fmt.Errorf("guild id is required")
		}
		var authorID string
		err := s.db.QueryRowContext(
			ctx,
			resolveAuthorIDByNameFallbackQuery,
			normalizedName, guildID,
		).Scan(&authorID)
		if err == sql.ErrNoRows {
			return "", nil
		}
		return authorID, err
	case entityTypeRole:
		if guildID == "" {
			return "", fmt.Errorf("guild id is required")
		}
		if !s.hasRoles {
			return "", nil
		}
		var roleID string
		err := s.db.QueryRowContext(
			ctx,
			resolveRoleIDByNameFallbackQuery,
			guildID, normalizedName,
		).Scan(&roleID)
		if err == sql.ErrNoRows {
			return "", nil
		}
		return roleID, err
	case entityTypeGuild:
		// No legacy fallback exists for guild names in old schema.
		return "", nil
	default:
		return "", fmt.Errorf("unsupported entity type %q", entityType)
	}
}

func (s *Service) queryCombined(
	ctx context.Context,
	messageWhere, attachmentWhere string,
	messageArgs, attachmentArgs []any,
	since, until, beforeTime, beforeID string,
	limit int,
) ([]Record, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("service is not initialized")
	}
	if strings.TrimSpace(messageWhere) == "" || strings.TrimSpace(attachmentWhere) == "" {
		return nil, fmt.Errorf("where clause is required")
	}
	if len(messageArgs) == 0 || len(attachmentArgs) == 0 {
		return nil, fmt.Errorf("at least one filter argument is required")
	}
	var err error
	since, until, err = normalizeTimeRange(since, until)
	if err != nil {
		return nil, err
	}
	beforeTime, beforeID, err = normalizeBeforeCursor(beforeTime, beforeID)
	if err != nil {
		return nil, err
	}

	limit = clampLimit(limit)
	if since != "" {
		messageWhere += messageTimeSinceFilterQuery
		messageArgs = append(messageArgs, since)
		attachmentWhere += attachmentTimeSinceFilterQuery
		attachmentArgs = append(attachmentArgs, since)
	}
	if until != "" {
		messageWhere += messageTimeUntilFilterQuery
		messageArgs = append(messageArgs, until)
		attachmentWhere += attachmentTimeUntilFilterQuery
		attachmentArgs = append(attachmentArgs, until)
	}
	if beforeTime != "" {
		if beforeID != "" {
			messageWhere += messageBeforeTimeIDFilterQuery
			messageArgs = append(messageArgs, beforeTime, beforeTime, beforeID)
			attachmentWhere += attachmentBeforeTimeIDFilterQuery
			attachmentArgs = append(attachmentArgs, beforeTime, beforeTime, beforeID)
		} else {
			messageWhere += messageBeforeTimeFilterQuery
			messageArgs = append(messageArgs, beforeTime)
			attachmentWhere += attachmentBeforeTimeFilterQuery
			attachmentArgs = append(attachmentArgs, beforeTime)
		}
	}

	if !s.hasAttachments {
		query := fmt.Sprintf(combinedMessagesOnlyQueryTemplate, messageWhere)

		args := make([]any, 0, len(messageArgs)+1)
		args = append(args, messageArgs...)
		args = append(args, limit)
		return s.scanRecords(ctx, query, args...)
	}

	query := fmt.Sprintf(combinedMessagesAndAttachmentsQueryTemplate, messageWhere, attachmentWhere)

	args := make([]any, 0, len(messageArgs)+len(attachmentArgs)+1)
	args = append(args, messageArgs...)
	args = append(args, attachmentArgs...)
	args = append(args, limit)

	return s.scanRecords(ctx, query, args...)
}

func (s *Service) searchMessagesFTS(
	ctx context.Context,
	guildID, query, since, until, beforeTime, beforeID string,
	limit int,
) ([]Record, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("service is not initialized")
	}
	limit = clampLimit(limit)
	ftsMatchQuery, ok := buildSafeFTSMatchQuery(query)
	if !ok {
		pattern := "%" + escapeLikePattern(query) + "%"
		return s.queryCombined(
			ctx,
			"m.guild_id = ? AND m.content LIKE ? ESCAPE '\\'",
			"a.guild_id = ? AND ((a.content_text != '' AND a.content_text LIKE ? ESCAPE '\\') OR (a.content_text = '' AND a.filename LIKE ? ESCAPE '\\'))",
			[]any{guildID, pattern},
			[]any{guildID, pattern, pattern},
			since,
			until,
			beforeTime,
			beforeID,
			limit,
		)
	}

	sinceFilter := ""
	untilFilter := ""
	beforeFilter := ""
	args := make([]any, 0, 8)
	args = append(args, guildID, ftsMatchQuery)
	if since != "" {
		sinceFilter = searchFTSTimeSinceFilterQuery
		args = append(args, since)
	}
	if until != "" {
		untilFilter = searchFTSTimeUntilFilterQuery
		args = append(args, until)
	}
	if beforeTime != "" {
		if beforeID != "" {
			beforeFilter = searchFTSBeforeTimeIDFilterQuery
			args = append(args, beforeTime, beforeTime, beforeID)
		} else {
			beforeFilter = searchFTSBeforeTimeFilterQuery
			args = append(args, beforeTime)
		}
	}
	args = append(args, limit)

	ftsQuery := fmt.Sprintf(searchMessagesFTSQuery, sinceFilter, untilFilter, beforeFilter)
	rows, err := s.scanRecords(ctx, ftsQuery, args...)
	if err != nil {
		// Fallback for environments where FTS MATCH parsing still rejects input.
		if isFTSMatchError(err) {
			pattern := "%" + escapeLikePattern(query) + "%"
			return s.queryCombined(
				ctx,
				"m.guild_id = ? AND m.content LIKE ? ESCAPE '\\'",
				"a.guild_id = ? AND ((a.content_text != '' AND a.content_text LIKE ? ESCAPE '\\') OR (a.content_text = '' AND a.filename LIKE ? ESCAPE '\\'))",
				[]any{guildID, pattern},
				[]any{guildID, pattern, pattern},
				since,
				until,
				beforeTime,
				beforeID,
				limit,
			)
		}
		return nil, err
	}
	return rows, nil
}

func (s *Service) topicActivitySummaryFTS(
	ctx context.Context,
	guildID, query, since, until string,
	limit int,
) ([]Record, error) {
	ftsMatchQuery, ok := buildSafeFTSMatchQuery(query)
	if !ok {
		return s.topicActivitySummaryLike(ctx, guildID, query, since, until, limit)
	}

	sinceFilter := ""
	untilFilter := ""
	args := make([]any, 0, 5)
	args = append(args, guildID, ftsMatchQuery)
	if since != "" {
		sinceFilter = searchFTSTimeSinceFilterQuery
		args = append(args, since)
	}
	if until != "" {
		untilFilter = searchFTSTimeUntilFilterQuery
		args = append(args, until)
	}
	args = append(args, limit)
	queryText := fmt.Sprintf(topicActivitySummaryFTSQueryTemplate, sinceFilter, untilFilter)
	rows, err := s.scanSummaryRecords(ctx, queryText, args...)
	if err != nil {
		if isFTSMatchError(err) {
			return s.topicActivitySummaryLike(ctx, guildID, query, since, until, limit)
		}
		return nil, err
	}
	return rows, nil
}

func (s *Service) topicActivitySummaryLike(
	ctx context.Context,
	guildID, query, since, until string,
	limit int,
) ([]Record, error) {
	messageSinceFilter := ""
	messageUntilFilter := ""
	attachmentSinceFilter := ""
	attachmentUntilFilter := ""
	pattern := "%" + escapeLikePattern(query) + "%"
	args := make([]any, 0, 10)
	args = append(args, guildID, pattern)
	if since != "" {
		messageSinceFilter = messageTimeSinceFilterQuery
		args = append(args, since)
	}
	if until != "" {
		messageUntilFilter = messageTimeUntilFilterQuery
		args = append(args, until)
	}
	args = append(args, guildID, pattern, pattern)
	if since != "" {
		attachmentSinceFilter = attachmentTimeSinceFilterQuery
		args = append(args, since)
	}
	if until != "" {
		attachmentUntilFilter = attachmentTimeUntilFilterQuery
		args = append(args, until)
	}
	args = append(args, limit)
	queryText := fmt.Sprintf(
		topicActivitySummaryLikeQueryTemplate,
		messageSinceFilter,
		messageUntilFilter,
		attachmentSinceFilter,
		attachmentUntilFilter,
	)
	return s.scanSummaryRecords(ctx, queryText, args...)
}

func (s *Service) scanSummaryRecords(ctx context.Context, query string, args ...any) ([]Record, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]Record, 0)
	for rows.Next() {
		var r Record
		if err := rows.Scan(
			&r.Source,
			&r.MessageID,
			&r.AttachmentID,
			&r.GuildID,
			&r.ChannelID,
			&r.AuthorID,
			&r.AuthorName,
			&r.TargetID,
			&r.TargetName,
			&r.RoleID,
			&r.RoleName,
			&r.CreatedAt,
			&r.Content,
			&r.Filename,
			&r.ContentType,
			&r.URL,
			&r.ReferencedMessageID,
			&r.ReferencedChannelID,
			&r.ReferencedGuildID,
			&r.ThreadID,
			&r.ThreadParentID,
			&r.EventID,
			&r.EventType,
			&r.PayloadJSON,
		); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Service) scanRecords(ctx context.Context, query string, args ...any) ([]Record, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Record
	for rows.Next() {
		var r Record
		if err := rows.Scan(
			&r.Source,
			&r.MessageID,
			&r.AttachmentID,
			&r.GuildID,
			&r.ChannelID,
			&r.AuthorID,
			&r.AuthorName,
			&r.TargetID,
			&r.TargetName,
			&r.RoleID,
			&r.RoleName,
			&r.CreatedAt,
			&r.Content,
			&r.Filename,
			&r.ContentType,
			&r.URL,
			&r.ReferencedMessageID,
			&r.ReferencedChannelID,
			&r.ReferencedGuildID,
			&r.ThreadID,
			&r.ThreadParentID,
		); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func tableExists(db *sql.DB, tableName string) (bool, error) {
	var count int
	err := db.QueryRow(
		tableExistsQuery,
		tableName,
	).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func clampLimit(n int) int {
	switch {
	case n <= 0:
		return 10
	case n > maxLimit:
		return maxLimit
	default:
		return n
	}
}

func escapeLikePattern(input string) string {
	replacer := strings.NewReplacer(
		`\`, `\\`,
		`%`, `\%`,
		`_`, `\_`,
	)
	return replacer.Replace(input)
}

func buildSafeFTSMatchQuery(input string) (string, bool) {
	fields := strings.Fields(strings.TrimSpace(input))
	if len(fields) == 0 {
		return "", false
	}

	terms := make([]string, 0, len(fields))
	for _, field := range fields {
		if !hasFTSSearchableRune(field) {
			continue
		}
		escaped := strings.ReplaceAll(field, `"`, `""`)
		terms = append(terms, `"`+escaped+`"`)
	}
	if len(terms) == 0 {
		return "", false
	}
	return strings.Join(terms, " AND "), true
}

func hasFTSSearchableRune(value string) bool {
	for _, r := range value {
		if unicode.IsLetter(r) || unicode.IsNumber(r) {
			return true
		}
	}
	return false
}

func isFTSMatchError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "fts5") || strings.Contains(msg, "match")
}

func normalizeTimeRange(since, until string) (string, string, error) {
	var err error
	since = strings.TrimSpace(since)
	until = strings.TrimSpace(until)

	if since != "" {
		since, err = parseRFC3339ToUTC(since)
		if err != nil {
			return "", "", fmt.Errorf("invalid since timestamp %q: %w", since, err)
		}
	}
	if until != "" {
		until, err = parseRFC3339ToUTC(until)
		if err != nil {
			return "", "", fmt.Errorf("invalid until timestamp %q: %w", until, err)
		}
	}
	if since != "" && until != "" && since > until {
		return "", "", fmt.Errorf("since must be <= until")
	}
	return since, until, nil
}

func normalizeBeforeCursor(beforeTime, beforeID string) (string, string, error) {
	var err error
	beforeTime = strings.TrimSpace(beforeTime)
	beforeID = strings.TrimSpace(beforeID)
	if beforeTime != "" {
		beforeTime, err = parseRFC3339ToUTC(beforeTime)
		if err != nil {
			return "", "", fmt.Errorf("invalid before_time timestamp %q: %w", beforeTime, err)
		}
	}
	if beforeID != "" && beforeTime == "" {
		return "", "", fmt.Errorf("before_time is required when before_id is set")
	}
	return beforeTime, beforeID, nil
}

func parseRFC3339ToUTC(value string) (string, error) {
	ts, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return "", err
	}
	return ts.UTC().Format(time.RFC3339Nano), nil
}

func parsePositiveInt64(value string) (int64, error) {
	v, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil {
		return 0, err
	}
	if v < 0 {
		return 0, fmt.Errorf("must be non-negative")
	}
	return v, nil
}

func normalizeHumanName(v string) string {
	return strings.ToLower(strings.TrimSpace(v))
}

var (
	keywordTokenPattern = regexp.MustCompile(`[[:alnum:]_]+`)
	userMentionPattern  = regexp.MustCompile(`<@!?([^>\s]+)>`)
	roleMentionPattern  = regexp.MustCompile(`<@&([^>\s]+)>`)
)

func dedupePingRows(rows []Record) []Record {
	if len(rows) == 0 {
		return rows
	}
	seen := make(map[string]struct{}, len(rows))
	out := make([]Record, 0, len(rows))
	for _, r := range rows {
		key := r.MessageID + "\x00" + r.AuthorID + "\x00" + r.TargetID
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, r)
	}
	return out
}

func (s *Service) humanizePingRows(ctx context.Context, guildID string, rows []Record) error {
	if len(rows) == 0 {
		return nil
	}

	userIDs := make(map[string]struct{})
	roleIDs := make(map[string]struct{})
	for _, r := range rows {
		for _, m := range userMentionPattern.FindAllStringSubmatch(r.Content, -1) {
			if len(m) >= 2 {
				userIDs[m[1]] = struct{}{}
			}
		}
		for _, m := range roleMentionPattern.FindAllStringSubmatch(r.Content, -1) {
			if len(m) >= 2 {
				roleIDs[m[1]] = struct{}{}
			}
		}
	}

	userNames, err := s.lookupNamesByID(ctx, guildID, entityTypeUser, mapKeys(userIDs))
	if err != nil {
		return err
	}
	roleNames, err := s.lookupNamesByID(ctx, guildID, entityTypeRole, mapKeys(roleIDs))
	if err != nil {
		return err
	}

	for i := range rows {
		content := rows[i].Content
		content = humanizeMentionContent(content, userNames, roleNames)
		content = humanizeTargetMention(content, rows[i].TargetID, rows[i].TargetName)
		rows[i].Content = content
	}
	return nil
}

func humanizeMentionContent(content string, userNames, roleNames map[string]string) string {
	for userID, humanName := range userNames {
		if strings.TrimSpace(userID) == "" {
			continue
		}
		if strings.TrimSpace(humanName) == "" {
			humanName = userID
		}
		mention := "@" + humanName
		content = strings.ReplaceAll(content, "<@"+userID+">", mention)
		content = strings.ReplaceAll(content, "<@!"+userID+">", mention)
	}
	for roleID, humanName := range roleNames {
		if strings.TrimSpace(roleID) == "" {
			continue
		}
		if strings.TrimSpace(humanName) == "" {
			humanName = roleID
		}
		mention := "@" + humanName
		content = strings.ReplaceAll(content, "<@&"+roleID+">", mention)
	}
	return content
}

func humanizeTargetMention(content, targetID, targetName string) string {
	if content == "" || targetID == "" {
		return content
	}

	display := strings.TrimSpace(targetName)
	if display == "" {
		display = targetID
	}
	mention := "@" + display

	content = strings.ReplaceAll(content, "<@"+targetID+">", mention)
	content = strings.ReplaceAll(content, "<@!"+targetID+">", mention)
	return content
}

func (s *Service) lookupNamesByID(
	ctx context.Context,
	guildID, entityType string,
	ids []string,
) (map[string]string, error) {
	out := make(map[string]string, len(ids))
	if !s.hasIDNameMapper || len(ids) == 0 || strings.TrimSpace(guildID) == "" || strings.TrimSpace(entityType) == "" {
		return out, nil
	}

	filtered := make([]string, 0, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id != "" {
			filtered = append(filtered, id)
		}
	}
	if len(filtered) == 0 {
		return out, nil
	}

	placeholders := strings.Repeat("?,", len(filtered))
	placeholders = strings.TrimSuffix(placeholders, ",")
	query := fmt.Sprintf(lookupNamesByIDQueryTemplate, placeholders)

	args := make([]any, 0, len(filtered)+2)
	args = append(args, entityType, guildID)
	for _, id := range filtered {
		args = append(args, id)
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var entityID, humanName string
		if err := rows.Scan(&entityID, &humanName); err != nil {
			return nil, err
		}
		entityID = strings.TrimSpace(entityID)
		if entityID == "" {
			continue
		}
		out[entityID] = strings.TrimSpace(humanName)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Backward-compatible fallback for user names when id_name_mappings is incomplete.
	if entityType == entityTypeUser {
		missing := make([]string, 0, len(filtered))
		for _, id := range filtered {
			if _, exists := out[id]; !exists {
				missing = append(missing, id)
			}
		}
		if len(missing) > 0 && s.hasPingEvents {
			placeholders := strings.Repeat("?,", len(missing))
			placeholders = strings.TrimSuffix(placeholders, ",")
			pingQuery := fmt.Sprintf(lookupNamesByPingEventsQueryTemplate, placeholders)
			pingArgs := make([]any, 0, len(missing)+1)
			pingArgs = append(pingArgs, guildID)
			for _, id := range missing {
				pingArgs = append(pingArgs, id)
			}
			pingRows, err := s.db.QueryContext(ctx, pingQuery, pingArgs...)
			if err != nil {
				return nil, err
			}
			for pingRows.Next() {
				var targetID, targetName string
				if err := pingRows.Scan(&targetID, &targetName); err != nil {
					_ = pingRows.Close()
					return nil, err
				}
				targetID = strings.TrimSpace(targetID)
				targetName = strings.TrimSpace(targetName)
				if targetID == "" || targetName == "" {
					continue
				}
				if _, exists := out[targetID]; !exists {
					out[targetID] = targetName
				}
			}
			if err := pingRows.Close(); err != nil {
				return nil, err
			}
		}
	}

	return out, nil
}

func mapKeys(in map[string]struct{}) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, 0, len(in))
	for k := range in {
		out = append(out, k)
	}
	return out
}
