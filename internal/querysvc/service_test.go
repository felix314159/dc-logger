// Package querysvc tests read-only assistant retrieval queries over the backup database.
package querysvc

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

func openTestService(t *testing.T) *Service {
	t.Helper()
	return openTestServiceWithSetup(t, nil)
}

func openTestServiceWithSetup(t *testing.T, setup func(*sql.DB) error) *Service {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "query.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("sql.Open failed: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(testServiceSchemaQuery); err != nil {
		t.Fatalf("schema exec failed: %v", err)
	}

	if _, err := db.Exec(testServiceSeedMessagesQuery); err != nil {
		t.Fatalf("seed messages failed: %v", err)
	}

	if _, err := db.Exec(testServiceSeedAttachmentsQuery); err != nil {
		t.Fatalf("seed attachments failed: %v", err)
	}

	if _, err := db.Exec(testServiceSeedChannelsQuery); err != nil {
		t.Fatalf("seed channels failed: %v", err)
	}

	if _, err := db.Exec(testServiceSeedUsersQuery); err != nil {
		t.Fatalf("seed users failed: %v", err)
	}

	if _, err := db.Exec(testServiceSeedNameMappingsQuery); err != nil {
		t.Fatalf("seed id_name_mappings failed: %v", err)
	}

	if _, err := db.Exec(testServiceSeedPingEventsQuery); err != nil {
		t.Fatalf("seed ping_events failed: %v", err)
	}

	if _, err := db.Exec(testServiceSeedGuildMembersQuery); err != nil {
		t.Fatalf("seed guild_members failed: %v", err)
	}

	if _, err := db.Exec(testServiceSeedMemberRolesQuery); err != nil {
		t.Fatalf("seed member_roles failed: %v", err)
	}

	if _, err := db.Exec(testServiceSeedRolePingEventsQuery); err != nil {
		t.Fatalf("seed role_ping_events failed: %v", err)
	}
	if _, err := db.Exec(testServiceSeedLifecycleEventsQuery); err != nil {
		t.Fatalf("seed lifecycle_events failed: %v", err)
	}
	if setup != nil {
		if err := setup(db); err != nil {
			t.Fatalf("custom setup failed: %v", err)
		}
	}

	svc, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	t.Cleanup(func() { _ = svc.Close() })
	return svc
}

func openTestServiceWithFTS(t *testing.T) *Service {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "query-fts.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("sql.Open failed: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(testServiceSchemaQuery); err != nil {
		t.Fatalf("schema exec failed: %v", err)
	}
	if _, err := db.Exec(testServiceCreateFTSTableQuery); err != nil {
		t.Fatalf("create fts failed: %v", err)
	}
	if _, err := db.Exec(testServiceSeedFTSRowsQuery); err != nil {
		t.Fatalf("seed fts rows failed: %v", err)
	}

	svc, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	t.Cleanup(func() { _ = svc.Close() })
	return svc
}

func TestRecentMessages_MergesMessageAndAttachmentRows(t *testing.T) {
	svc := openTestService(t)
	ctx := context.Background()

	rows, err := svc.RecentMessages(ctx, "g1", 10)
	if err != nil {
		t.Fatalf("RecentMessages failed: %v", err)
	}
	if len(rows) != 4 {
		t.Fatalf("row count mismatch: got %d want %d", len(rows), 4)
	}
	if rows[0].Source != "attachment" || rows[0].AttachmentID != "a2" {
		t.Fatalf("unexpected first row: %+v", rows[0])
	}
	if rows[0].Content != "[attachment] img.png" {
		t.Fatalf("fallback attachment content mismatch: %q", rows[0].Content)
	}
	if rows[1].Source != "message" || rows[1].MessageID != "m2" {
		t.Fatalf("unexpected second row: %+v", rows[1])
	}
	if rows[2].ReferencedMessageID != "m1" || rows[2].ThreadID != "th1" {
		t.Fatalf("expected relationship fields on attachment row: %+v", rows[2])
	}
}

func TestRecentMessagesInChannel_FiltersChannel(t *testing.T) {
	svc := openTestService(t)
	ctx := context.Background()

	rows, err := svc.RecentMessagesInChannel(ctx, "g1", "c1", 10)
	if err != nil {
		t.Fatalf("RecentMessagesInChannel failed: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("row count mismatch: got %d want %d", len(rows), 2)
	}
	for _, r := range rows {
		if r.ChannelID != "c1" {
			t.Fatalf("unexpected channel id in row: %+v", r)
		}
	}
}

func TestLastMessageByUser_ReturnsMostRecentActiveRow(t *testing.T) {
	svc := openTestService(t)
	ctx := context.Background()

	row, err := svc.LastMessageByUser(ctx, "g1", "u1")
	if err != nil {
		t.Fatalf("LastMessageByUser failed: %v", err)
	}
	if row == nil {
		t.Fatal("expected non-nil row")
	}
	if row.Source != "attachment" || row.AttachmentID != "a1" {
		t.Fatalf("unexpected row: %+v", row)
	}
}

func TestSearchMessages_FiltersAcrossMessagesAndAttachments(t *testing.T) {
	svc := openTestService(t)
	ctx := context.Background()

	rows, err := svc.SearchMessages(ctx, "g1", "long", 10)
	if err != nil {
		t.Fatalf("SearchMessages(long) failed: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("row count mismatch for long: got %d want %d", len(rows), 1)
	}
	if rows[0].Source != "attachment" || rows[0].AttachmentID != "a1" {
		t.Fatalf("unexpected long search row: %+v", rows[0])
	}

	rows, err = svc.SearchMessages(ctx, "g1", "img", 10)
	if err != nil {
		t.Fatalf("SearchMessages(img) failed: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("row count mismatch for img: got %d want %d", len(rows), 1)
	}
	if rows[0].AttachmentID != "a2" {
		t.Fatalf("unexpected img search row: %+v", rows[0])
	}
}

func TestSearchMessages_RejectsBlankQuery(t *testing.T) {
	svc := openTestService(t)
	_, err := svc.SearchMessages(context.Background(), "g1", "   ", 10)
	if err == nil {
		t.Fatal("expected error for blank query")
	}
}

func TestRecentMessagesFiltered_RespectsTimeRange(t *testing.T) {
	svc := openTestService(t)
	rows, err := svc.RecentMessagesFiltered(
		context.Background(),
		"g1",
		"2026-01-01T00:00:02Z",
		"2026-01-01T00:00:04Z",
		10,
	)
	if err != nil {
		t.Fatalf("RecentMessagesFiltered failed: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("row count mismatch: got %d want %d", len(rows), 2)
	}
	if rows[0].MessageID != "m2" || rows[1].AttachmentID != "a1" {
		t.Fatalf("unexpected rows: %+v", rows)
	}
}

func TestRecentMessagesWindow_RespectsBeforeCursor(t *testing.T) {
	svc := openTestService(t)
	rows, err := svc.RecentMessagesWindow(
		context.Background(),
		"g1",
		"",
		"",
		"2026-01-01T00:00:03Z",
		"m2",
		10,
	)
	if err != nil {
		t.Fatalf("RecentMessagesWindow failed: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("row count mismatch: got %d want %d", len(rows), 2)
	}
	if rows[0].AttachmentID != "a1" || rows[1].MessageID != "m1" {
		t.Fatalf("unexpected rows: %+v", rows)
	}
}

func TestSearchMessagesFiltered_UsesFTSTable(t *testing.T) {
	svc := openTestServiceWithFTS(t)
	rows, err := svc.SearchMessagesFiltered(
		context.Background(),
		"g1",
		"incident",
		"2026-01-01T00:00:01Z",
		"2026-01-01T00:00:02Z",
		10,
	)
	if err != nil {
		t.Fatalf("SearchMessagesFiltered failed: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("row count mismatch: got %d want %d", len(rows), 2)
	}
	if rows[0].Source != "attachment" || rows[0].AttachmentID != "a1" {
		t.Fatalf("unexpected first row: %+v", rows[0])
	}
	if rows[1].Source != "message" || rows[1].MessageID != "m1" {
		t.Fatalf("unexpected second row: %+v", rows[1])
	}
}

func TestSearchMessagesFiltered_HandlesHyphenatedQuery_FTSPath(t *testing.T) {
	svc := openTestServiceWithFTS(t)
	rows, err := svc.SearchMessagesFiltered(
		context.Background(),
		"g1",
		"lorem-ipsum",
		"",
		"",
		10,
	)
	if err != nil {
		t.Fatalf("SearchMessagesFiltered(lorem-ipsum) failed: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("row count mismatch: got %d want %d", len(rows), 1)
	}
	if rows[0].Source != "message" || rows[0].MessageID != "m3" {
		t.Fatalf("unexpected row for hyphenated query: %+v", rows[0])
	}
}

func TestTopicActivitySummary_AggregatesHitsByAuthor_LikePath(t *testing.T) {
	svc := openTestService(t)
	rows, err := svc.TopicActivitySummary(
		context.Background(),
		"g1",
		"l",
		"",
		"",
		10,
	)
	if err != nil {
		t.Fatalf("TopicActivitySummary failed: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("row count mismatch: got %d want %d", len(rows), 1)
	}
	if rows[0].AuthorID != "u1" || rows[0].AuthorName != "ipsum" {
		t.Fatalf("unexpected author in topic summary: %+v", rows[0])
	}
	if !strings.Contains(rows[0].PayloadJSON, `"hit_count":2`) {
		t.Fatalf("unexpected payload_json in topic summary: %q", rows[0].PayloadJSON)
	}
}

func TestTopicActivitySummary_AggregatesHitsByAuthor_FTSPath(t *testing.T) {
	svc := openTestServiceWithFTS(t)
	rows, err := svc.TopicActivitySummary(
		context.Background(),
		"g1",
		"incident",
		"",
		"",
		10,
	)
	if err != nil {
		t.Fatalf("TopicActivitySummary failed: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("row count mismatch: got %d want %d", len(rows), 1)
	}
	if rows[0].AuthorID != "u1" {
		t.Fatalf("unexpected author in FTS topic summary: %+v", rows[0])
	}
	if !strings.Contains(rows[0].PayloadJSON, `"hit_count":2`) {
		t.Fatalf("unexpected payload_json in FTS topic summary: %q", rows[0].PayloadJSON)
	}
}

func TestTopicActivitySummary_HandlesHyphenatedQuery_FTSPath(t *testing.T) {
	svc := openTestServiceWithFTS(t)
	rows, err := svc.TopicActivitySummary(
		context.Background(),
		"g1",
		"lorem-ipsum",
		"",
		"",
		10,
	)
	if err != nil {
		t.Fatalf("TopicActivitySummary(lorem-ipsum) failed: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("row count mismatch: got %d want %d", len(rows), 1)
	}
	if rows[0].AuthorID != "u2" {
		t.Fatalf("unexpected author for hyphenated query: %+v", rows[0])
	}
	if !strings.Contains(rows[0].PayloadJSON, `"hit_count":1`) {
		t.Fatalf("unexpected payload_json for hyphenated query: %q", rows[0].PayloadJSON)
	}
}

func TestTopicActivitySummary_WeirdSymbolOnlyQuery_DoesNotError(t *testing.T) {
	svc := openTestServiceWithFTS(t)
	rows, err := svc.TopicActivitySummary(
		context.Background(),
		"g1",
		"--- @@ !!!",
		"",
		"",
		10,
	)
	if err != nil {
		t.Fatalf("TopicActivitySummary(symbol-only) failed: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("expected zero rows for symbol-only query, got %d", len(rows))
	}
}

func TestResolveIDsByHumanName(t *testing.T) {
	svc := openTestService(t)
	ctx := context.Background()

	guildID, err := svc.ResolveGuildIDByName(ctx, "lorem")
	if err != nil {
		t.Fatalf("ResolveGuildIDByName failed: %v", err)
	}
	if guildID != "g1" {
		t.Fatalf("guild id mismatch: got %q want %q", guildID, "g1")
	}

	channelID, err := svc.ResolveChannelIDByName(ctx, "g1", "GENERAL")
	if err != nil {
		t.Fatalf("ResolveChannelIDByName failed: %v", err)
	}
	if channelID != "c1" {
		t.Fatalf("channel id mismatch: got %q want %q", channelID, "c1")
	}

	authorID, err := svc.ResolveAuthorIDByName(ctx, "g1", "ipsum")
	if err != nil {
		t.Fatalf("ResolveAuthorIDByName failed: %v", err)
	}
	if authorID != "u1" {
		t.Fatalf("author id mismatch: got %q want %q", authorID, "u1")
	}
}

func TestRecentLifecycleEvents_FiltersByTypeUserChannelAndTime(t *testing.T) {
	svc := openTestService(t)
	rows, err := svc.RecentLifecycleEvents(
		context.Background(),
		"g1",
		"message_deleted",
		"u2",
		"c2",
		"2026-01-01T00:00:09Z",
		"2026-01-01T00:00:11Z",
		10,
	)
	if err != nil {
		t.Fatalf("RecentLifecycleEvents failed: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("row count mismatch: got %d want %d", len(rows), 1)
	}
	if rows[0].Source != "lifecycle_event" {
		t.Fatalf("unexpected source: %q", rows[0].Source)
	}
	if rows[0].EventType != "message_deleted" || rows[0].AuthorID != "u2" || rows[0].ChannelID != "c2" {
		t.Fatalf("unexpected event row: %+v", rows[0])
	}
	if rows[0].PayloadJSON == "" {
		t.Fatalf("expected payload_json to be populated: %+v", rows[0])
	}
	if rows[0].EventID == "" {
		t.Fatalf("expected event_id to be populated: %+v", rows[0])
	}
}

func TestUnansweredPingsTargetingUser_ReturnsOnlyOpenPings(t *testing.T) {
	svc := openTestService(t)
	rows, err := svc.UnansweredPingsTargetingUser(
		context.Background(),
		"g1",
		"u2",
		"2026-01-01T00:00:00Z",
		"2026-01-01T00:01:00Z",
		"",
		"",
		10,
	)
	if err != nil {
		t.Fatalf("UnansweredPingsTargetingUser failed: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("row count mismatch: got %d want %d", len(rows), 1)
	}
	if rows[0].MessageID != "m1" || rows[0].TargetID != "u2" {
		t.Fatalf("unexpected unanswered row: %+v", rows[0])
	}
}

func TestLastPingedUserByUser_ReturnsMostRecentTarget(t *testing.T) {
	svc := openTestService(t)
	row, err := svc.LastPingedUserByUser(context.Background(), "g1", "u1")
	if err != nil {
		t.Fatalf("LastPingedUserByUser failed: %v", err)
	}
	if row == nil {
		t.Fatal("expected non-nil row")
	}
	if row.Source != "ping" || row.TargetID != "u3" {
		t.Fatalf("unexpected ping row: %+v", row)
	}
	if row.Content != "bye @amet @Consectetur" {
		t.Fatalf("unexpected ping content: got %q want %q", row.Content, "bye @amet @Consectetur")
	}
}

func TestRecentPingsTargetingUser_ReturnsRecentRows(t *testing.T) {
	svc := openTestService(t)
	rows, err := svc.RecentPingsTargetingUser(context.Background(), "g1", "u2", 10)
	if err != nil {
		t.Fatalf("RecentPingsTargetingUser failed: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("row count mismatch: got %d want %d", len(rows), 1)
	}
	if rows[0].Source != "ping" || rows[0].TargetID != "u2" || rows[0].AuthorID != "u1" {
		t.Fatalf("unexpected ping row: %+v", rows[0])
	}
	if rows[0].Content != "hello @dolor" {
		t.Fatalf("unexpected ping content: got %q want %q", rows[0].Content, "hello @dolor")
	}
}

func TestRecentPingsTargetingUser_IncludesRoleMentionPings(t *testing.T) {
	svc := openTestService(t)
	rows, err := svc.RecentPingsTargetingUser(context.Background(), "g1", "u1", 10)
	if err != nil {
		t.Fatalf("RecentPingsTargetingUser failed: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("row count mismatch: got %d want %d", len(rows), 1)
	}
	if rows[0].MessageID != "m2" || rows[0].AuthorID != "u2" || rows[0].TargetID != "u1" {
		t.Fatalf("unexpected role-derived ping row: %+v", rows[0])
	}
	if rows[0].Content != "bye @amet @Consectetur" {
		t.Fatalf("unexpected ping content: got %q want %q", rows[0].Content, "bye @amet @Consectetur")
	}
}

func TestRolesOfUser_ReturnsAssignedRoles(t *testing.T) {
	svc := openTestService(t)
	rows, err := svc.RolesOfUser(context.Background(), "g1", "u1", 10)
	if err != nil {
		t.Fatalf("RolesOfUser failed: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("row count mismatch: got %d want %d", len(rows), 1)
	}
	if rows[0].AuthorID != "u1" || rows[0].AuthorName != "ipsum" {
		t.Fatalf("unexpected author info: %+v", rows[0])
	}
	if rows[0].RoleID != "r1" || rows[0].RoleName != "Consectetur" {
		t.Fatalf("unexpected role info: %+v", rows[0])
	}
}

func TestUsersWithRole_ReturnsAssignedUsers(t *testing.T) {
	svc := openTestService(t)
	rows, err := svc.UsersWithRole(context.Background(), "g1", "r1", 10)
	if err != nil {
		t.Fatalf("UsersWithRole failed: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("row count mismatch: got %d want %d", len(rows), 1)
	}
	if rows[0].AuthorID != "u1" || rows[0].AuthorName != "ipsum" {
		t.Fatalf("unexpected author info: %+v", rows[0])
	}
	if rows[0].RoleID != "r1" || rows[0].RoleName != "Consectetur" {
		t.Fatalf("unexpected role info: %+v", rows[0])
	}
}

func TestListGuildMembers_ReturnsCurrentUsernames(t *testing.T) {
	svc := openTestService(t)
	rows, err := svc.ListGuildMembers(context.Background(), "g1", 10)
	if err != nil {
		t.Fatalf("ListGuildMembers failed: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("row count mismatch: got %d want %d", len(rows), 2)
	}
	if rows[0].AuthorID != "u2" || rows[0].AuthorName != "dolor" {
		t.Fatalf("unexpected first member row: %+v", rows[0])
	}
	if rows[1].AuthorID != "u1" || rows[1].AuthorName != "ipsum" {
		t.Fatalf("unexpected second member row: %+v", rows[1])
	}
}

func TestEnrichNames_PopulatesMessageLikeRows(t *testing.T) {
	svc := openTestService(t)
	rows := []Record{
		{
			Source:    "message",
			GuildID:   "g1",
			ChannelID: "c1",
			AuthorID:  "u1",
		},
	}
	if err := svc.EnrichNames(context.Background(), "g1", rows); err != nil {
		t.Fatalf("EnrichNames failed: %v", err)
	}
	if rows[0].GuildName != "Lorem" || rows[0].ChannelName != "general" || rows[0].AuthorName != "ipsum" {
		t.Fatalf("unexpected enriched row: %+v", rows[0])
	}
}

func TestServerActivitySummary_ReturnsWindowTotals(t *testing.T) {
	svc := openTestService(t)
	rows, err := svc.ServerActivitySummary(
		context.Background(),
		"g1",
		"2026-01-01T00:00:01Z",
		"2026-01-01T00:00:04Z",
	)
	if err != nil {
		t.Fatalf("ServerActivitySummary failed: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("row count mismatch: got %d want 1", len(rows))
	}
	if rows[0].MessageCount != 2 || rows[0].UniqueAuthors != 2 || rows[0].UniqueChannels != 2 {
		t.Fatalf("unexpected summary row: %+v", rows[0])
	}
}

func TestServerActivitySummaryByDay_ReturnsDailyBreakdown(t *testing.T) {
	svc := openTestServiceWithSetup(t, func(db *sql.DB) error {
		_, err := db.Exec(
			`INSERT INTO messages(message_id, guild_id, channel_id, author_id, created_at, content) VALUES ('m5', 'g1', 'c1', 'u1', '2026-01-02T03:04:05Z', 'day2 message')`,
		)
		return err
	})

	rows, err := svc.ServerActivitySummaryByDay(context.Background(), "g1", "", "")
	if err != nil {
		t.Fatalf("ServerActivitySummaryByDay failed: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("row count mismatch: got %d want 2", len(rows))
	}
	if rows[0].Day != "2026-01-01" || rows[1].Day != "2026-01-02" {
		t.Fatalf("unexpected day values/order: %+v", rows)
	}
	if rows[0].MessageCount != 2 || rows[0].UniqueAuthors != 2 || rows[0].UniqueChannels != 2 {
		t.Fatalf("unexpected day1 breakdown: %+v", rows[0])
	}
	if rows[1].MessageCount != 1 || rows[1].UniqueAuthors != 1 || rows[1].UniqueChannels != 1 {
		t.Fatalf("unexpected day2 breakdown: %+v", rows[1])
	}
	if rows[0].FirstSeenAt != "2026-01-01T00:00:01Z" || rows[0].LastSeenAt != "2026-01-01T00:00:03Z" {
		t.Fatalf("unexpected day1 first/last values: %+v", rows[0])
	}
	if rows[1].FirstSeenAt != "2026-01-02T03:04:05Z" || rows[1].LastSeenAt != "2026-01-02T03:04:05Z" {
		t.Fatalf("unexpected day2 first/last values: %+v", rows[1])
	}
}

func TestChannelActivitySummary_ReturnsPerChannelCounts(t *testing.T) {
	svc := openTestService(t)
	rows, err := svc.ChannelActivitySummary(context.Background(), "g1", "", "", 10)
	if err != nil {
		t.Fatalf("ChannelActivitySummary failed: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("row count mismatch: got %d want 2", len(rows))
	}
	if rows[0].MessageCount != 1 || rows[1].MessageCount != 1 {
		t.Fatalf("unexpected message counts: %+v", rows)
	}
}

func TestAuthorActivitySummary_ReturnsPerAuthorCounts(t *testing.T) {
	svc := openTestService(t)
	rows, err := svc.AuthorActivitySummary(context.Background(), "g1", "", "", 10)
	if err != nil {
		t.Fatalf("AuthorActivitySummary failed: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("row count mismatch: got %d want 2", len(rows))
	}
	if rows[0].MessageCount != 1 || rows[1].MessageCount != 1 {
		t.Fatalf("unexpected message counts: %+v", rows)
	}
}

func TestKeywordFrequency_ReturnsTopTerms(t *testing.T) {
	svc := openTestService(t)
	rows, err := svc.KeywordFrequency(context.Background(), "g1", "", "", 5, nil, 10)
	if err != nil {
		t.Fatalf("KeywordFrequency failed: %v", err)
	}
	if len(rows) == 0 {
		t.Fatalf("expected keyword rows, got none")
	}
	if rows[0].Term != "hello" || rows[0].HitCount != 1 {
		t.Fatalf("unexpected top keyword row: %+v", rows[0])
	}
}

func TestClampLimit(t *testing.T) {
	if got := clampLimit(0); got != 10 {
		t.Fatalf("clampLimit(0) = %d, want 10", got)
	}
	if got := clampLimit(999); got != 100 {
		t.Fatalf("clampLimit(999) = %d, want 100", got)
	}
	if got := clampLimit(7); got != 7 {
		t.Fatalf("clampLimit(7) = %d, want 7", got)
	}
}

func TestEscapeLikePattern(t *testing.T) {
	got := escapeLikePattern(`100%_ready\set`)
	if got != `100\%\_ready\\set` {
		t.Fatalf("escapeLikePattern mismatch: got %q", got)
	}
}

func TestBuildSafeFTSMatchQuery(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
		ok    bool
	}{
		{name: "single token", input: "lorem-ipsum", want: `"lorem-ipsum"`, ok: true},
		{name: "multi token", input: "dolor-sit amet", want: `"dolor-sit" AND "amet"`, ok: true},
		{name: "quotes escaped", input: `lorem "ipsum"`, want: `"lorem" AND """ipsum"""`, ok: true},
		{name: "symbol only", input: "--- @@ !!!", want: "", ok: false},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got, ok := buildSafeFTSMatchQuery(tc.input)
			if ok != tc.ok {
				t.Fatalf("ok mismatch: got %v want %v", ok, tc.ok)
			}
			if got != tc.want {
				t.Fatalf("query mismatch: got %q want %q", got, tc.want)
			}
		})
	}
}

func TestMostCommonGuildID_PrefersMostFrequentAcrossSources(t *testing.T) {
	svc := openTestService(t)
	ctx := context.Background()

	guildID, err := svc.MostCommonGuildID(ctx)
	if err != nil {
		t.Fatalf("MostCommonGuildID failed: %v", err)
	}
	if guildID != "g1" {
		t.Fatalf("most common guild mismatch: got %q want %q", guildID, "g1")
	}
}

func TestOpen_WithoutAttachmentsTableFallsBackToMessagesOnly(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "query-no-attachments.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("sql.Open failed: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(testServiceSchemaWithoutAttachmentsAndSeedQuery); err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	svc, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer func() { _ = svc.Close() }()

	rows, err := svc.RecentMessages(context.Background(), "g2", 10)
	if err != nil {
		t.Fatalf("RecentMessages failed: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("row count mismatch: got %d want %d", len(rows), 1)
	}
	if rows[0].MessageID != "m-only" {
		t.Fatalf("unexpected row: %+v", rows[0])
	}
}
