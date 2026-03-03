package main

import (
	"bytes"
	"database/sql"
	"log"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	_ "modernc.org/sqlite"
)

func TestLogTrackedEvent_IsNoOp(t *testing.T) {
	oldWriter := log.Writer()
	oldFlags := log.Flags()
	var logBuf bytes.Buffer
	log.SetOutput(&logBuf)
	log.SetFlags(0)

	t.Cleanup(func() {
		setTrackedEventLoggingEnabled(false)
		log.SetOutput(oldWriter)
		log.SetFlags(oldFlags)
	})

	setTrackedEventLoggingEnabled(false)
	logTrackedEvent(eventMessageSent, "g1", "c1", "m1", "u1", map[string]any{"content": "hello"})
	if got := strings.TrimSpace(logBuf.String()); got != "" {
		t.Fatalf("expected no tracked event log when realtime mode disabled, got %q", got)
	}

	setTrackedEventLoggingEnabled(true)
	logTrackedEvent(eventMessageSent, "g1", "c1", "m1", "u1", map[string]any{"content": "hello"})
	if got := strings.TrimSpace(logBuf.String()); got != "" {
		t.Fatalf("expected no tracked event log when realtime mode enabled, got %q", got)
	}
}

func TestLogMessageSentEvent_PrettyFormatting(t *testing.T) {
	got := renderMessageSentEventLog(
		"lorem",
		"",
		"#announcements",
		"hello world!",
		"2026-02-26, 04:01:57 PM",
	)

	want := "" +
		"Event: message_sent\n" +
		"User: lorem\n" +
		"Channel: #announcements\n" +
		"Message: hello world!\n" +
		"Time: 2026-02-26, 04:01:57 PM\n\n"
	if got != want {
		t.Fatalf("formatted message_sent log mismatch:\n--- got ---\n%q\n--- want ---\n%q", got, want)
	}
}

func TestRenderMessageSentEventLog_ThreadFormatting(t *testing.T) {
	got := renderMessageSentEventLog(
		"ipsum",
		"#lorem ipsum thread",
		"#forum-lorem",
		"summary text",
		"2026-03-02, 04:28:15 PM",
	)

	want := "" +
		"Event: message_sent\n" +
		"User: ipsum\n" +
		"Thread: #lorem ipsum thread\n" +
		"Channel: #forum-lorem\n" +
		"Message: summary text\n" +
		"Time: 2026-03-02, 04:28:15 PM\n\n"
	if got != want {
		t.Fatalf("formatted message_sent thread log mismatch:\n--- got ---\n%q\n--- want ---\n%q", got, want)
	}
}

func TestReplaceMentionsWithDisplayNames(t *testing.T) {
	got := replaceMentionsWithDisplayNames(
		nil,
		nil,
		"g1",
		"Sent an invite for <@333333333333333333> and pinged <@!444444444444444444> plus <@&role-14>",
	)

	want := "Sent an invite for @333333333333333333 and pinged @444444444444444444 plus <@&role-14>"
	if got != want {
		t.Fatalf("mention replacement mismatch:\n--- got ---\n%q\n--- want ---\n%q", got, want)
	}
}

func TestReplaceMentionsWithDisplayNames_UsesNameMappings(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "mentions.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("sql.Open failed: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(`
CREATE TABLE id_name_mappings (
	entity_type      TEXT NOT NULL,
	entity_id        TEXT NOT NULL,
	guild_id         TEXT NOT NULL,
	human_name       TEXT NOT NULL,
	normalized_name  TEXT NOT NULL,
	updated_at       TEXT NOT NULL,
	PRIMARY KEY(entity_type, entity_id, guild_id)
);`); err != nil {
		t.Fatalf("create id_name_mappings failed: %v", err)
	}
	if _, err := db.Exec(`
INSERT INTO id_name_mappings(entity_type, entity_id, guild_id, human_name, normalized_name, updated_at)
VALUES
	('user', '333333333333333333', 'g1', 'LoremIpsum', 'loremipsum', '2026-03-02T15:00:00Z'),
	('role', 'role-14', 'g1', 'Consectetur', 'consectetur', '2026-03-02T15:00:00Z');`); err != nil {
		t.Fatalf("seed id_name_mappings failed: %v", err)
	}

	got := replaceMentionsWithDisplayNames(
		nil,
		db,
		"g1",
		"Sent an invite for <@333333333333333333> and <@&role-14>",
	)

	want := "Sent an invite for @LoremIpsum and @Consectetur"
	if got != want {
		t.Fatalf("mention replacement mismatch:\n--- got ---\n%q\n--- want ---\n%q", got, want)
	}
}

func TestMessageSentLogContent_PrefersMessageContent(t *testing.T) {
	got := messageSentLogContent("hello world", "attachment body")
	if got != "hello world" {
		t.Fatalf("unexpected selected content: got %q want %q", got, "hello world")
	}
}

func TestMessageSentLogContent_FallsBackToAttachmentText(t *testing.T) {
	got := messageSentLogContent("", "attachment body")
	if got != "attachment body" {
		t.Fatalf("unexpected selected content: got %q want %q", got, "attachment body")
	}
}

func TestResolveMessageSentLocation_UsesThreadFlagFromDB(t *testing.T) {
	db, stmts := openTestDB(t)
	now := "2026-03-02T16:28:15Z"

	if _, err := stmts.upsertChannel.Exec(
		"parent-1",
		"g1",
		"forum-lorem",
		int(discordgo.ChannelTypeGuildText),
		"",
		0,
		now,
		"",
	); err != nil {
		t.Fatalf("seed parent channel failed: %v", err)
	}

	if _, err := stmts.upsertChannel.Exec(
		"thread-1",
		"g1",
		"lorem ipsum thread",
		int(discordgo.ChannelTypeGuildPublicThread),
		"parent-1",
		1,
		now,
		"",
	); err != nil {
		t.Fatalf("seed thread channel failed: %v", err)
	}

	threadName, channelName := resolveMessageSentLocation(nil, db, "g1", "thread-1")
	if threadName != "lorem ipsum thread" || channelName != "forum-lorem" {
		t.Fatalf("unexpected location: thread=%q channel=%q", threadName, channelName)
	}
}

func TestFormatMessageSentTime_UsesSystemLocalTime(t *testing.T) {
	prevLocal := time.Local
	time.Local = time.FixedZone("TEST_LOCAL", -5*60*60)
	t.Cleanup(func() { time.Local = prevLocal })

	got := formatMessageSentTime("2026-03-03T15:04:05Z", "")
	want := "2026-03-03, 10:04:05 AM"
	if got != want {
		t.Fatalf("formatMessageSentTime local conversion mismatch: got %q want %q", got, want)
	}
}
