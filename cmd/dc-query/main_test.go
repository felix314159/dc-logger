package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"example.org/dc-logger/internal/config"

	_ "modernc.org/sqlite"
)

func seedQueryDB(t *testing.T) string {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "query-cli.db")
	seedQueryDBAt(t, dbPath)
	return dbPath
}

func seedQueryDBAt(t *testing.T, dbPath string) {
	t.Helper()

	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		t.Fatalf("mkdir for db path failed: %v", err)
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("sql.Open failed: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(cliTestSchemaQuery); err != nil {
		t.Fatalf("schema exec failed: %v", err)
	}

	if _, err := db.Exec(cliTestSeedMessagesQuery); err != nil {
		t.Fatalf("seed messages failed: %v", err)
	}
	if _, err := db.Exec(cliTestSeedAttachmentsQuery); err != nil {
		t.Fatalf("seed attachments failed: %v", err)
	}
	if _, err := db.Exec(cliTestSeedIDNameMappingsQuery); err != nil {
		t.Fatalf("seed id_name_mappings failed: %v", err)
	}
	if _, err := db.Exec(cliTestSeedPingEventsQuery); err != nil {
		t.Fatalf("seed ping_events failed: %v", err)
	}
	if _, err := db.Exec(cliTestSeedGuildMembersQuery); err != nil {
		t.Fatalf("seed guild_members failed: %v", err)
	}
	if _, err := db.Exec(cliTestSeedMemberRolesQuery); err != nil {
		t.Fatalf("seed member_roles failed: %v", err)
	}
	if _, err := db.Exec(cliTestSeedRolePingEventsQuery); err != nil {
		t.Fatalf("seed role_ping_events failed: %v", err)
	}
	if _, err := db.Exec(cliTestSeedLifecycleEventsQuery); err != nil {
		t.Fatalf("seed lifecycle_events failed: %v", err)
	}
}

func TestRunSearchMessagesCommand(t *testing.T) {
	dbPath := seedQueryDB(t)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	exit := run(
		[]string{"search-messages", "--db", dbPath, "--guild-id", "g1", "--query", "incident"},
		strings.NewReader(""),
		&stdout,
		&stderr,
	)
	if exit != 0 {
		t.Fatalf("unexpected exit code: got %d, stderr=%s", exit, stderr.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("unexpected stderr: %s", stderr.String())
	}

	var out response
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		t.Fatalf("decode response failed: %v\nraw=%s", err, stdout.String())
	}
	if out.Command != "search-messages" {
		t.Fatalf("unexpected command: %q", out.Command)
	}
	if out.Count != 3 {
		t.Fatalf("unexpected count: got %d want 3", out.Count)
	}
}

func TestRunSearchMessages_DefaultDBPathWhenFlagAndEnvUnset(t *testing.T) {
	t.Setenv(config.EnvDiscordLogDB, "")
	t.Chdir(t.TempDir())

	dbPath := filepath.Join(".", "database", "database.db")
	seedQueryDBAt(t, dbPath)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	exit := run(
		[]string{"search-messages", "--guild-id", "g1", "--query", "incident"},
		strings.NewReader(""),
		&stdout,
		&stderr,
	)
	if exit != 0 {
		t.Fatalf("unexpected exit code: got %d, stderr=%s", exit, stderr.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("unexpected stderr: %s", stderr.String())
	}

	var out response
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		t.Fatalf("decode response failed: %v\nraw=%s", err, stdout.String())
	}
	if out.Command != "search-messages" {
		t.Fatalf("unexpected command: %q", out.Command)
	}
	if out.Count != 3 {
		t.Fatalf("unexpected count: got %d want 3", out.Count)
	}
}

func TestRunRecentMessagesCommand_WithTimeWindow(t *testing.T) {
	dbPath := seedQueryDB(t)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	exit := run(
		[]string{
			"recent-messages",
			"--db", dbPath,
			"--guild-id", "g1",
			"--since", "2026-01-01T00:00:03Z",
			"--until", "2026-01-01T00:00:04Z",
		},
		strings.NewReader(""),
		&stdout,
		&stderr,
	)
	if exit != 0 {
		t.Fatalf("unexpected exit code: got %d, stderr=%s", exit, stderr.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("unexpected stderr: %s", stderr.String())
	}

	var out response
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		t.Fatalf("decode response failed: %v\nraw=%s", err, stdout.String())
	}
	if out.Command != "recent-messages" {
		t.Fatalf("unexpected command: %q", out.Command)
	}
	if out.Count != 2 {
		t.Fatalf("unexpected count: got %d want 2", out.Count)
	}
	if out.Items[0].AttachmentID != "a2" || out.Items[1].AttachmentID != "a1" {
		t.Fatalf("unexpected rows: %+v", out.Items)
	}
	if out.Items[1].ReferencedMessageID != "m1" || out.Items[1].ThreadID != "th1" {
		t.Fatalf("expected relationship fields in output: %+v", out.Items[1])
	}
}

func TestRunRecentMessagesCommand_WithBeforeCursor(t *testing.T) {
	dbPath := seedQueryDB(t)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	exit := run(
		[]string{
			"recent-messages",
			"--db", dbPath,
			"--guild-id", "g1",
			"--before-time", "2026-01-01T00:00:04Z",
			"--before-id", "a2",
		},
		strings.NewReader(""),
		&stdout,
		&stderr,
	)
	if exit != 0 {
		t.Fatalf("unexpected exit code: got %d, stderr=%s", exit, stderr.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("unexpected stderr: %s", stderr.String())
	}

	var out response
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		t.Fatalf("decode response failed: %v\nraw=%s", err, stdout.String())
	}
	if out.Command != "recent-messages" {
		t.Fatalf("unexpected command: %q", out.Command)
	}
	if out.Count != 3 {
		t.Fatalf("unexpected count: got %d want 3", out.Count)
	}
	if out.Items[0].AttachmentID != "a1" || out.Items[1].MessageID != "m2" || out.Items[2].MessageID != "m1" {
		t.Fatalf("unexpected rows: %+v", out.Items)
	}
}

func TestRunRecentEventsCommand(t *testing.T) {
	dbPath := seedQueryDB(t)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	exit := run(
		[]string{
			"recent-events",
			"--db", dbPath,
			"--guild-name", "Lorem",
			"--event-type", "message_deleted",
			"--author-name", "dolor",
			"--channel-id", "c2",
			"--since", "2026-01-01T00:00:10Z",
			"--until", "2026-01-01T00:00:16Z",
		},
		strings.NewReader(""),
		&stdout,
		&stderr,
	)
	if exit != 0 {
		t.Fatalf("unexpected exit code: got %d, stderr=%s", exit, stderr.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("unexpected stderr: %s", stderr.String())
	}

	var out response
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		t.Fatalf("decode response failed: %v\nraw=%s", err, stdout.String())
	}
	if out.Command != "recent-events" {
		t.Fatalf("unexpected command: %q", out.Command)
	}
	if out.Count != 1 {
		t.Fatalf("unexpected count: got %d want 1", out.Count)
	}
	if out.Items[0].Source != "lifecycle_event" {
		t.Fatalf("unexpected source: %q", out.Items[0].Source)
	}
	if out.Items[0].EventType != "message_deleted" || out.Items[0].AuthorID != "u2" || out.Items[0].ChannelID != "c2" {
		t.Fatalf("unexpected event row: %+v", out.Items[0])
	}
	if out.Items[0].PayloadJSON == "" {
		t.Fatalf("expected payload_json in event row: %+v", out.Items[0])
	}
}

func TestRunJSONRequestMode(t *testing.T) {
	dbPath := seedQueryDB(t)
	reqBody, err := json.Marshal(request{
		Command:  "last_message_by_user",
		DBPath:   dbPath,
		GuildID:  "g1",
		AuthorID: "u1",
	})
	if err != nil {
		t.Fatalf("marshal request failed: %v", err)
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	exit := run(
		[]string{"json-request"},
		bytes.NewReader(reqBody),
		&stdout,
		&stderr,
	)
	if exit != 0 {
		t.Fatalf("unexpected exit code: got %d, stderr=%s", exit, stderr.String())
	}

	var out response
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		t.Fatalf("decode response failed: %v\nraw=%s", err, stdout.String())
	}
	if out.Command != "last-message-by-user" {
		t.Fatalf("unexpected command: %q", out.Command)
	}
	if out.Count != 1 {
		t.Fatalf("unexpected count: got %d want 1", out.Count)
	}
}

func TestRunJSONRequestMode_ServerActivityByDay(t *testing.T) {
	dbPath := seedQueryDB(t)

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("sql.Open failed: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(
		`INSERT INTO messages(message_id, guild_id, channel_id, author_id, created_at, content) VALUES ('m5', 'g1', 'c1', 'u1', '2026-01-02T03:04:05Z', 'day2 message')`,
	); err != nil {
		t.Fatalf("insert day2 message failed: %v", err)
	}

	reqBody, err := json.Marshal(request{
		Command:   "server_activity_summary",
		DBPath:    dbPath,
		GuildName: "Lorem",
		ByDay:     true,
	})
	if err != nil {
		t.Fatalf("marshal request failed: %v", err)
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	exit := run(
		[]string{"json-request"},
		bytes.NewReader(reqBody),
		&stdout,
		&stderr,
	)
	if exit != 0 {
		t.Fatalf("unexpected exit code: got %d, stderr=%s", exit, stderr.String())
	}

	var out response
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		t.Fatalf("decode response failed: %v\nraw=%s", err, stdout.String())
	}
	if out.Command != "server-activity-summary" {
		t.Fatalf("unexpected command: %q", out.Command)
	}
	if out.Count != 2 {
		t.Fatalf("unexpected count: got %d want 2", out.Count)
	}
	if out.Items[0].Day != "2026-01-01" || out.Items[1].Day != "2026-01-02" {
		t.Fatalf("unexpected day values/order: %+v", out.Items)
	}
}

func TestRunResolvesAuthorName(t *testing.T) {
	dbPath := seedQueryDB(t)

	reqBody, err := json.Marshal(request{
		Command:    "last_message_by_user",
		DBPath:     dbPath,
		GuildName:  "Lorem",
		AuthorName: "ipsum",
	})
	if err != nil {
		t.Fatalf("marshal request failed: %v", err)
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	exit := run(
		[]string{"json-request"},
		bytes.NewReader(reqBody),
		&stdout,
		&stderr,
	)
	if exit != 0 {
		t.Fatalf("unexpected exit code: got %d, stderr=%s", exit, stderr.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("unexpected stderr: %s", stderr.String())
	}

	var out response
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		t.Fatalf("decode response failed: %v\nraw=%s", err, stdout.String())
	}
	if out.Count != 1 {
		t.Fatalf("unexpected count: got %d want 1", out.Count)
	}
	if out.Items[0].AuthorID != "u1" {
		t.Fatalf("unexpected author id: got %q want %q", out.Items[0].AuthorID, "u1")
	}
}

func TestRunListServerMembers(t *testing.T) {
	dbPath := seedQueryDB(t)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	exit := run(
		[]string{"list-server-members", "--db", dbPath, "--guild-name", "Lorem", "--limit", "10"},
		strings.NewReader(""),
		&stdout,
		&stderr,
	)
	if exit != 0 {
		t.Fatalf("unexpected exit code: got %d, stderr=%s", exit, stderr.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("unexpected stderr: %s", stderr.String())
	}

	var out response
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		t.Fatalf("decode response failed: %v\nraw=%s", err, stdout.String())
	}
	if out.Command != "list-server-members" {
		t.Fatalf("unexpected command: %q", out.Command)
	}
	if out.Count != 2 {
		t.Fatalf("unexpected count: got %d want 2", out.Count)
	}
	if out.Items[0].AuthorName != "dolor" || out.Items[1].AuthorName != "ipsum" {
		t.Fatalf("unexpected member names: %+v", out.Items)
	}
}

func TestRunRolesOfUser(t *testing.T) {
	dbPath := seedQueryDB(t)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	exit := run(
		[]string{"roles-of-user", "--db", dbPath, "--guild-name", "Lorem", "--author-name", "ipsum", "--limit", "10"},
		strings.NewReader(""),
		&stdout,
		&stderr,
	)
	if exit != 0 {
		t.Fatalf("unexpected exit code: got %d, stderr=%s", exit, stderr.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("unexpected stderr: %s", stderr.String())
	}

	var out response
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		t.Fatalf("decode response failed: %v\nraw=%s", err, stdout.String())
	}
	if out.Command != "roles-of-user" {
		t.Fatalf("unexpected command: %q", out.Command)
	}
	if out.Count != 1 {
		t.Fatalf("unexpected count: got %d want 1", out.Count)
	}
	if out.Items[0].RoleID != "r1" || out.Items[0].RoleName != "Consectetur" {
		t.Fatalf("unexpected role info: %+v", out.Items[0])
	}
}

func TestRunUsersWithRole(t *testing.T) {
	dbPath := seedQueryDB(t)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	exit := run(
		[]string{"users-with-role", "--db", dbPath, "--guild-name", "Lorem", "--role-name", "Consectetur", "--limit", "10"},
		strings.NewReader(""),
		&stdout,
		&stderr,
	)
	if exit != 0 {
		t.Fatalf("unexpected exit code: got %d, stderr=%s", exit, stderr.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("unexpected stderr: %s", stderr.String())
	}

	var out response
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		t.Fatalf("decode response failed: %v\nraw=%s", err, stdout.String())
	}
	if out.Command != "users-with-role" {
		t.Fatalf("unexpected command: %q", out.Command)
	}
	if out.Count != 1 {
		t.Fatalf("unexpected count: got %d want 1", out.Count)
	}
	if out.Items[0].AuthorID != "u1" || out.Items[0].AuthorName != "ipsum" {
		t.Fatalf("unexpected user info: %+v", out.Items[0])
	}
}

func TestRunLastPingedUserByUser(t *testing.T) {
	dbPath := seedQueryDB(t)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	exit := run(
		[]string{"last-pinged-user-by-user", "--db", dbPath, "--guild-name", "Lorem", "--author-name", "ipsum"},
		strings.NewReader(""),
		&stdout,
		&stderr,
	)
	if exit != 0 {
		t.Fatalf("unexpected exit code: got %d, stderr=%s", exit, stderr.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("unexpected stderr: %s", stderr.String())
	}

	var out response
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		t.Fatalf("decode response failed: %v\nraw=%s", err, stdout.String())
	}
	if out.Command != "last-pinged-user-by-user" {
		t.Fatalf("unexpected command: %q", out.Command)
	}
	if out.Count != 1 {
		t.Fatalf("unexpected count: got %d want 1", out.Count)
	}
	if out.Items[0].TargetID != "u2" || out.Items[0].TargetName != "dolor" {
		t.Fatalf("unexpected target info: %+v", out.Items[0])
	}
	if out.Items[0].Content != "hello @dolor incident" {
		t.Fatalf("unexpected ping content: got %q want %q", out.Items[0].Content, "hello @dolor incident")
	}
}

func TestRunRecentPingsTargetingUser(t *testing.T) {
	dbPath := seedQueryDB(t)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	exit := run(
		[]string{"recent-pings-targeting-user", "--db", dbPath, "--guild-name", "Lorem", "--target-name", "dolor", "--limit", "5"},
		strings.NewReader(""),
		&stdout,
		&stderr,
	)
	if exit != 0 {
		t.Fatalf("unexpected exit code: got %d, stderr=%s", exit, stderr.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("unexpected stderr: %s", stderr.String())
	}

	var out response
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		t.Fatalf("decode response failed: %v\nraw=%s", err, stdout.String())
	}
	if out.Command != "recent-pings-targeting-user" {
		t.Fatalf("unexpected command: %q", out.Command)
	}
	if out.Count != 1 {
		t.Fatalf("unexpected count: got %d want 1", out.Count)
	}
	if out.Items[0].TargetID != "u2" || out.Items[0].AuthorID != "u1" {
		t.Fatalf("unexpected target/actor info: %+v", out.Items[0])
	}
	if out.Items[0].Content != "hello @dolor incident" {
		t.Fatalf("unexpected ping content: got %q want %q", out.Items[0].Content, "hello @dolor incident")
	}
}

func TestRunRecentPingsTargetingUser_IncludesRoleMentions(t *testing.T) {
	dbPath := seedQueryDB(t)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	exit := run(
		[]string{"recent-pings-targeting-user", "--db", dbPath, "--guild-name", "Lorem", "--target-name", "ipsum", "--limit", "5"},
		strings.NewReader(""),
		&stdout,
		&stderr,
	)
	if exit != 0 {
		t.Fatalf("unexpected exit code: got %d, stderr=%s", exit, stderr.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("unexpected stderr: %s", stderr.String())
	}

	var out response
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		t.Fatalf("decode response failed: %v\nraw=%s", err, stdout.String())
	}
	if out.Command != "recent-pings-targeting-user" {
		t.Fatalf("unexpected command: %q", out.Command)
	}
	if out.Count != 1 {
		t.Fatalf("unexpected count: got %d want 1", out.Count)
	}
	if out.Items[0].MessageID != "m2" || out.Items[0].AuthorID != "u2" {
		t.Fatalf("unexpected role-derived row: %+v", out.Items[0])
	}
	if out.Items[0].Content != "normal @Consectetur" {
		t.Fatalf("unexpected ping content: got %q want %q", out.Items[0].Content, "normal @Consectetur")
	}
}

func TestRunUnansweredPingsTargetingUser(t *testing.T) {
	dbPath := seedQueryDB(t)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	exit := run(
		[]string{"unanswered-pings-targeting-user", "--db", dbPath, "--guild-name", "Lorem", "--target-name", "dolor", "--since", "2026-01-01T00:00:00Z", "--limit", "5"},
		strings.NewReader(""),
		&stdout,
		&stderr,
	)
	if exit != 0 {
		t.Fatalf("unexpected exit code: got %d, stderr=%s", exit, stderr.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("unexpected stderr: %s", stderr.String())
	}

	var out response
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		t.Fatalf("decode response failed: %v\nraw=%s", err, stdout.String())
	}
	if out.Command != "unanswered-pings-targeting-user" {
		t.Fatalf("unexpected command: %q", out.Command)
	}
	if out.Count != 1 {
		t.Fatalf("unexpected count: got %d want 1", out.Count)
	}
	if out.Items[0].MessageID != "m1" || out.Items[0].TargetID != "u2" {
		t.Fatalf("unexpected unanswered row: %+v", out.Items[0])
	}
}

func TestRunTopicActivitySummary(t *testing.T) {
	dbPath := seedQueryDB(t)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	exit := run(
		[]string{"topic-activity-summary", "--db", dbPath, "--guild-name", "Lorem", "--query", "incident", "--limit", "10"},
		strings.NewReader(""),
		&stdout,
		&stderr,
	)
	if exit != 0 {
		t.Fatalf("unexpected exit code: got %d, stderr=%s", exit, stderr.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("unexpected stderr: %s", stderr.String())
	}

	var out response
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		t.Fatalf("decode response failed: %v\nraw=%s", err, stdout.String())
	}
	if out.Command != "topic-activity-summary" {
		t.Fatalf("unexpected command: %q", out.Command)
	}
	if out.Count != 2 {
		t.Fatalf("unexpected count: got %d want 2", out.Count)
	}
	if out.Items[0].AuthorID != "u2" || out.Items[1].AuthorID != "u1" {
		t.Fatalf("unexpected summary ordering/authors: %+v", out.Items)
	}
	if !strings.Contains(out.Items[1].PayloadJSON, `"hit_count":2`) {
		t.Fatalf("expected payload hit count in summary row: %+v", out.Items[1])
	}
}

func TestRunSearchMessages_NoMatchesReturnsEmptyArray(t *testing.T) {
	dbPath := seedQueryDB(t)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	exit := run(
		[]string{"search-messages", "--db", dbPath, "--guild-id", "g1", "--query", "no-such-token"},
		strings.NewReader(""),
		&stdout,
		&stderr,
	)
	if exit != 0 {
		t.Fatalf("unexpected exit code: got %d, stderr=%s", exit, stderr.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("unexpected stderr: %s", stderr.String())
	}

	var out response
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		t.Fatalf("decode response failed: %v\nraw=%s", err, stdout.String())
	}
	if out.Count != 0 {
		t.Fatalf("unexpected count: got %d want 0", out.Count)
	}
	if out.Items == nil {
		t.Fatalf("expected items to be an empty array, got nil")
	}
	if len(out.Items) != 0 {
		t.Fatalf("expected no items, got %d", len(out.Items))
	}
}

func TestRunRecentMessages_ResolveNames(t *testing.T) {
	dbPath := seedQueryDB(t)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	exit := run(
		[]string{
			"recent-messages",
			"--db", dbPath,
			"--guild-id", "g1",
			"--resolve-names",
			"--limit", "1",
		},
		strings.NewReader(""),
		&stdout,
		&stderr,
	)
	if exit != 0 {
		t.Fatalf("unexpected exit code: got %d, stderr=%s", exit, stderr.String())
	}

	var out response
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		t.Fatalf("decode response failed: %v\nraw=%s", err, stdout.String())
	}
	if out.Count != 1 {
		t.Fatalf("unexpected count: got %d want 1", out.Count)
	}
	if out.Items[0].AuthorName != "dolor" {
		t.Fatalf("expected resolved author_name, got %q", out.Items[0].AuthorName)
	}
	if out.Items[0].GuildName != "Lorem" {
		t.Fatalf("expected resolved guild_name, got %q", out.Items[0].GuildName)
	}
}

func TestRunServerActivitySummary(t *testing.T) {
	dbPath := seedQueryDB(t)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	exit := run(
		[]string{
			"server-activity-summary",
			"--db", dbPath,
			"--guild-name", "Lorem",
			"--since", "2026-01-01T00:00:01Z",
			"--until", "2026-01-01T00:00:02Z",
		},
		strings.NewReader(""),
		&stdout,
		&stderr,
	)
	if exit != 0 {
		t.Fatalf("unexpected exit code: got %d, stderr=%s", exit, stderr.String())
	}

	var out response
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		t.Fatalf("decode response failed: %v\nraw=%s", err, stdout.String())
	}
	if out.Command != "server-activity-summary" {
		t.Fatalf("unexpected command: %q", out.Command)
	}
	if out.Count != 1 {
		t.Fatalf("unexpected count: got %d want 1", out.Count)
	}
	if out.Items[0].MessageCount != 2 || out.Items[0].UniqueAuthors != 2 || out.Items[0].UniqueChannels != 2 {
		t.Fatalf("unexpected summary row: %+v", out.Items[0])
	}
}

func TestRunServerActivitySummary_ByDay(t *testing.T) {
	dbPath := seedQueryDB(t)

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("sql.Open failed: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(
		`INSERT INTO messages(message_id, guild_id, channel_id, author_id, created_at, content) VALUES ('m5', 'g1', 'c1', 'u1', '2026-01-02T03:04:05Z', 'day2 message')`,
	); err != nil {
		t.Fatalf("insert day2 message failed: %v", err)
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	exit := run(
		[]string{
			"server-activity-summary",
			"--db", dbPath,
			"--guild-name", "Lorem",
			"--by-day",
		},
		strings.NewReader(""),
		&stdout,
		&stderr,
	)
	if exit != 0 {
		t.Fatalf("unexpected exit code: got %d, stderr=%s", exit, stderr.String())
	}

	var out response
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		t.Fatalf("decode response failed: %v\nraw=%s", err, stdout.String())
	}
	if out.Command != "server-activity-summary" {
		t.Fatalf("unexpected command: %q", out.Command)
	}
	if out.Count != 2 {
		t.Fatalf("unexpected count: got %d want 2", out.Count)
	}
	if out.Items[0].Day != "2026-01-01" || out.Items[1].Day != "2026-01-02" {
		t.Fatalf("unexpected day values/order: %+v", out.Items)
	}
}

func TestRunChannelActivitySummary(t *testing.T) {
	dbPath := seedQueryDB(t)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	exit := run(
		[]string{"channel-activity-summary", "--db", dbPath, "--guild-name", "Lorem", "--limit", "10"},
		strings.NewReader(""),
		&stdout,
		&stderr,
	)
	if exit != 0 {
		t.Fatalf("unexpected exit code: got %d, stderr=%s", exit, stderr.String())
	}

	var out response
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		t.Fatalf("decode response failed: %v\nraw=%s", err, stdout.String())
	}
	if out.Command != "channel-activity-summary" {
		t.Fatalf("unexpected command: %q", out.Command)
	}
	if out.Count != 2 {
		t.Fatalf("unexpected count: got %d want 2", out.Count)
	}
	if out.Items[0].MessageCount != 1 || out.Items[1].MessageCount != 1 {
		t.Fatalf("unexpected channel counts: %+v", out.Items)
	}
}

func TestRunAuthorActivitySummary(t *testing.T) {
	dbPath := seedQueryDB(t)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	exit := run(
		[]string{"author-activity-summary", "--db", dbPath, "--guild-name", "Lorem", "--limit", "10"},
		strings.NewReader(""),
		&stdout,
		&stderr,
	)
	if exit != 0 {
		t.Fatalf("unexpected exit code: got %d, stderr=%s", exit, stderr.String())
	}

	var out response
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		t.Fatalf("decode response failed: %v\nraw=%s", err, stdout.String())
	}
	if out.Command != "author-activity-summary" {
		t.Fatalf("unexpected command: %q", out.Command)
	}
	if out.Count != 2 {
		t.Fatalf("unexpected count: got %d want 2", out.Count)
	}
	if out.Items[0].MessageCount != 1 || out.Items[1].MessageCount != 1 {
		t.Fatalf("unexpected author counts: %+v", out.Items)
	}
}

func TestRunKeywordFrequency(t *testing.T) {
	dbPath := seedQueryDB(t)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	exit := run(
		[]string{
			"keyword-frequency",
			"--db", dbPath,
			"--guild-name", "Lorem",
			"--min-length", "4",
			"--limit", "10",
		},
		strings.NewReader(""),
		&stdout,
		&stderr,
	)
	if exit != 0 {
		t.Fatalf("unexpected exit code: got %d, stderr=%s", exit, stderr.String())
	}

	var out response
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		t.Fatalf("decode response failed: %v\nraw=%s", err, stdout.String())
	}
	if out.Command != "keyword-frequency" {
		t.Fatalf("unexpected command: %q", out.Command)
	}
	if out.Count == 0 {
		t.Fatalf("expected at least one keyword, got none")
	}
	if out.Items[0].Term != "incident" || out.Items[0].HitCount != 3 {
		t.Fatalf("unexpected top keyword row: %+v", out.Items[0])
	}
}

func TestRunStructuredErrorCode(t *testing.T) {
	dbPath := seedQueryDB(t)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	exit := run(
		[]string{"search-messages", "--db", dbPath, "--guild-id", "g1"},
		strings.NewReader(""),
		&stdout,
		&stderr,
	)
	if exit != exitInvalid {
		t.Fatalf("unexpected exit code: got %d want %d", exit, exitInvalid)
	}
	if stdout.Len() != 0 {
		t.Fatalf("unexpected stdout: %s", stdout.String())
	}

	var errOut errorEnvelope
	if err := json.Unmarshal(stderr.Bytes(), &errOut); err != nil {
		t.Fatalf("decode stderr failed: %v\nraw=%s", err, stderr.String())
	}
	if errOut.Error.Code != codeInvalidArgument {
		t.Fatalf("unexpected error code: %q", errOut.Error.Code)
	}
	if !strings.Contains(errOut.Error.Message, "query") {
		t.Fatalf("unexpected error message: %q", errOut.Error.Message)
	}
}
