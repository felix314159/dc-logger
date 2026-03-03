// Package main tests message lifecycle handlers and related database side effects.
package main

import (
	"database/sql"
	"encoding/json"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
)

func openTestDB(t *testing.T) (*sql.DB, *preparedStatements) {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := openAndInitDB(dbPath)
	if err != nil {
		t.Fatalf("openAndInitDB failed: %v", err)
	}

	stmts, err := prepareStatements(db)
	if err != nil {
		_ = db.Close()
		t.Fatalf("prepareStatements failed: %v", err)
	}

	t.Cleanup(func() {
		closePreparedStatements(stmts)
		_ = db.Close()
	})

	return db, stmts
}

func mustCount(t *testing.T, db *sql.DB, query string, args ...any) int {
	t.Helper()

	var n int
	if err := db.QueryRow(query, args...).Scan(&n); err != nil {
		t.Fatalf("count query failed: %v", err)
	}
	return n
}

func testAuditEntryIDForNow() string {
	nowMS := time.Now().UTC().UnixMilli()
	raw := uint64((nowMS - discordSnowflakeEpochMS) << 22)
	return strconv.FormatUint(raw, 10)
}

func TestHandleMessageUpdate_PersistsLatestContentAndLifecycleHistory(t *testing.T) {
	db, stmts := openTestDB(t)

	created := time.Now().UTC()
	handleMessageCreate(nil, db, stmts, &discordgo.MessageCreate{
		Message: &discordgo.Message{
			ID:        "1001",
			GuildID:   "guild-1",
			ChannelID: "channel-1",
			Content:   "hello",
			Timestamp: created,
			Author: &discordgo.User{
				ID:            "user-1",
				Username:      "alice",
				Discriminator: "0001",
			},
		},
	})

	edited := created.Add(2 * time.Minute)
	handleMessageUpdate(nil, stmts, &discordgo.MessageUpdate{
		Message: &discordgo.Message{
			ID:              "1001",
			GuildID:         "guild-1",
			ChannelID:       "channel-1",
			Content:         "hello world",
			EditedTimestamp: &edited,
			Author: &discordgo.User{
				ID:            "user-1",
				Username:      "alice",
				Discriminator: "0001",
			},
		},
		BeforeUpdate: &discordgo.Message{
			Content: "hello",
			Author: &discordgo.User{
				ID: "user-1",
			},
		},
	})

	var content, editedAt string
	if err := db.QueryRow(
		selectMessageContentEditedByIDQuery,
		"1001",
	).Scan(&content, &editedAt); err != nil {
		t.Fatalf("query updated message failed: %v", err)
	}

	wantEditedAt := edited.UTC().Format(time.RFC3339Nano)
	if content != "hello world" {
		t.Fatalf("content mismatch: got %q want %q", content, "hello world")
	}
	if editedAt != wantEditedAt {
		t.Fatalf("edited_at mismatch: got %q want %q", editedAt, wantEditedAt)
	}

	var payloadJSON string
	if err := db.QueryRow(
		selectLifecyclePayloadByTypeAndMessageLatestQuery,
		string(eventMessageUpdated),
		"1001",
	).Scan(&payloadJSON); err != nil {
		t.Fatalf("query lifecycle event failed: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("unmarshal payload failed: %v", err)
	}
	if payload["content"] != "hello world" {
		t.Fatalf("payload content mismatch: got %#v", payload["content"])
	}
	if payload["before_content"] != "hello" {
		t.Fatalf("payload before_content mismatch: got %#v", payload["before_content"])
	}
}

func TestHandleMessageUpdate_DedupesIdenticalPayload(t *testing.T) {
	db, stmts := openTestDB(t)

	created := time.Now().UTC()
	handleMessageCreate(nil, db, stmts, &discordgo.MessageCreate{
		Message: &discordgo.Message{
			ID:        "1002",
			GuildID:   "guild-1",
			ChannelID: "channel-1",
			Content:   "hello",
			Timestamp: created,
			Author: &discordgo.User{
				ID:            "user-1",
				Username:      "alice",
				Discriminator: "0001",
			},
		},
	})

	update := &discordgo.MessageUpdate{
		Message: &discordgo.Message{
			ID:        "1002",
			GuildID:   "guild-1",
			ChannelID: "channel-1",
			Content:   "same content",
			Author: &discordgo.User{
				ID:            "user-1",
				Username:      "alice",
				Discriminator: "0001",
			},
		},
	}

	handleMessageUpdate(nil, stmts, update)
	handleMessageUpdate(nil, stmts, update)

	if got := mustCount(t, db, countLifecycleByMessageAndTypeQuery, "1002", string(eventMessageUpdated)); got != 1 {
		t.Fatalf("expected deduped message_updated lifecycle event count=1, got %d", got)
	}
}

func TestHandleMessageCreate_EmptyContentSkipsPersistenceButAdvancesState(t *testing.T) {
	db, stmts := openTestDB(t)

	handleMessageCreate(nil, db, stmts, &discordgo.MessageCreate{
		Message: &discordgo.Message{
			ID:        "2001",
			GuildID:   "guild-2",
			ChannelID: "channel-2",
			Content:   "",
			Timestamp: time.Now().UTC(),
			Author: &discordgo.User{
				ID:            "user-2",
				Username:      "bob",
				Discriminator: "0002",
			},
		},
	})

	if got := mustCount(t, db, countMessagesByMessageIDQuery, "2001"); got != 0 {
		t.Fatalf("unexpected persisted empty-content message count: %d", got)
	}
	if got := mustCount(t, db, countLifecycleEventsByMessageIDQuery, "2001"); got != 0 {
		t.Fatalf("unexpected lifecycle events for empty-content message: %d", got)
	}

	var lastID string
	if err := db.QueryRow(
		selectChannelStateLastMessageByChannelQuery,
		"channel-2",
	).Scan(&lastID); err != nil {
		t.Fatalf("query channel_state failed: %v", err)
	}
	if lastID != "2001" {
		t.Fatalf("last_message_id mismatch: got %q want %q", lastID, "2001")
	}
}

func TestHandleMessageCreate_AttachmentOnlyMessagePersists(t *testing.T) {
	db, stmts := openTestDB(t)
	prevFetcher := attachmentTextFetcher
	attachmentTextFetcher = func(url string, maxBytes int) (string, error) {
		return "very long text body from attachment", nil
	}
	t.Cleanup(func() { attachmentTextFetcher = prevFetcher })

	handleMessageCreate(nil, db, stmts, &discordgo.MessageCreate{
		Message: &discordgo.Message{
			ID:        "2501",
			GuildID:   "guild-25",
			ChannelID: "channel-25",
			Content:   "",
			Timestamp: time.Now().UTC(),
			Author: &discordgo.User{
				ID:            "user-25",
				Username:      "alice",
				Discriminator: "0025",
			},
			Attachments: []*discordgo.MessageAttachment{
				{
					ID:          "att-1",
					Filename:    "long-message.txt",
					ContentType: "text/plain",
					URL:         "https://example.test/long-message.txt",
					ProxyURL:    "https://example.test/proxy-long-message.txt",
					Size:        1024,
				},
			},
		},
	})

	if got := mustCount(t, db, countMessagesByMessageIDQuery, "2501"); got != 0 {
		t.Fatalf("attachment-only message should not be inserted into messages, got count=%d", got)
	}
	if got := mustCount(
		t,
		db,
		countLifecycleByMessageAndTypeQuery,
		"2501",
		string(eventMessageSent),
	); got != 1 {
		t.Fatalf("attachment-only message should emit lifecycle event, got count=%d", got)
	}
	if got := mustCount(t, db, countAttachmentsByMessageIDQuery, "2501"); got != 1 {
		t.Fatalf("attachment-only message should insert into attachments, got count=%d", got)
	}

	var textBody string
	if err := db.QueryRow(selectAttachmentContentTextByIDQuery, "att-1").Scan(&textBody); err != nil {
		t.Fatalf("query attachment text failed: %v", err)
	}
	if textBody != "very long text body from attachment" {
		t.Fatalf("content_text mismatch: got %q", textBody)
	}
}

func TestHandleMessageCreate_PersistsMessageRelationshipFields(t *testing.T) {
	db, stmts := openTestDB(t)

	handleMessageCreate(nil, db, stmts, &discordgo.MessageCreate{
		Message: &discordgo.Message{
			ID:        "2551",
			GuildID:   "guild-255",
			ChannelID: "thread-255",
			Content:   "reply",
			Timestamp: time.Now().UTC(),
			Author: &discordgo.User{
				ID:            "user-255",
				Username:      "rel-user",
				Discriminator: "0255",
			},
			MessageReference: &discordgo.MessageReference{
				MessageID: "origin-255",
				ChannelID: "parent-255",
				GuildID:   "guild-255",
			},
			Thread: &discordgo.Channel{
				ID:       "thread-255",
				ParentID: "parent-255",
				Type:     discordgo.ChannelTypeGuildPublicThread,
			},
		},
	})

	var (
		refMessageID string
		refChannelID string
		refGuildID   string
		threadID     string
		threadParent string
	)
	if err := db.QueryRow(
		selectMessageRelationshipByMessageIDQuery,
		"2551",
	).Scan(&refMessageID, &refChannelID, &refGuildID, &threadID, &threadParent); err != nil {
		t.Fatalf("query message relationship fields failed: %v", err)
	}

	if refMessageID != "origin-255" || refChannelID != "parent-255" || refGuildID != "guild-255" {
		t.Fatalf(
			"unexpected referenced ids: message=%q channel=%q guild=%q",
			refMessageID,
			refChannelID,
			refGuildID,
		)
	}
	if threadID != "thread-255" || threadParent != "parent-255" {
		t.Fatalf("unexpected thread context: thread_id=%q thread_parent_id=%q", threadID, threadParent)
	}
}

func TestHandleMessageCreate_AttachmentOnlyPersistsRelationshipFields(t *testing.T) {
	db, stmts := openTestDB(t)

	handleMessageCreate(nil, db, stmts, &discordgo.MessageCreate{
		Message: &discordgo.Message{
			ID:        "2552",
			GuildID:   "guild-255",
			ChannelID: "thread-255",
			Content:   "",
			Timestamp: time.Now().UTC(),
			Author: &discordgo.User{
				ID:            "user-255",
				Username:      "rel-user",
				Discriminator: "0255",
			},
			MessageReference: &discordgo.MessageReference{
				MessageID: "origin-255",
				ChannelID: "parent-255",
				GuildID:   "guild-255",
			},
			Thread: &discordgo.Channel{
				ID:       "thread-255",
				ParentID: "parent-255",
				Type:     discordgo.ChannelTypeGuildPublicThread,
			},
			Attachments: []*discordgo.MessageAttachment{
				{
					ID:          "att-rel-2552",
					Filename:    "image.png",
					ContentType: "image/png",
					URL:         "https://example.test/image.png",
					ProxyURL:    "https://example.test/proxy-image.png",
					Size:        42,
				},
			},
		},
	})

	var (
		refMessageID string
		refChannelID string
		refGuildID   string
		threadID     string
		threadParent string
	)
	if err := db.QueryRow(
		selectAttachmentRelationshipByIDQuery,
		"att-rel-2552",
	).Scan(&refMessageID, &refChannelID, &refGuildID, &threadID, &threadParent); err != nil {
		t.Fatalf("query attachment relationship fields failed: %v", err)
	}

	if refMessageID != "origin-255" || refChannelID != "parent-255" || refGuildID != "guild-255" {
		t.Fatalf(
			"unexpected referenced ids: message=%q channel=%q guild=%q",
			refMessageID,
			refChannelID,
			refGuildID,
		)
	}
	if threadID != "thread-255" || threadParent != "parent-255" {
		t.Fatalf("unexpected thread context: thread_id=%q thread_parent_id=%q", threadID, threadParent)
	}
}

func TestHandleMessageCreate_IgnoresBotMessages(t *testing.T) {
	db, stmts := openTestDB(t)

	handleMessageCreate(nil, db, stmts, &discordgo.MessageCreate{
		Message: &discordgo.Message{
			ID:        "3001",
			GuildID:   "guild-3",
			ChannelID: "channel-3",
			Content:   "from bot",
			Timestamp: time.Now().UTC(),
			Author: &discordgo.User{
				ID:            "bot-1",
				Username:      "bot",
				Discriminator: "0003",
				Bot:           true,
			},
		},
	})

	if got := mustCount(t, db, countMessagesByMessageIDQuery, "3001"); got != 0 {
		t.Fatalf("unexpected bot message persisted: %d", got)
	}
	if got := mustCount(t, db, countLifecycleEventsByMessageIDQuery, "3001"); got != 0 {
		t.Fatalf("unexpected bot lifecycle event persisted: %d", got)
	}
}

func TestHandleMessageUpdateAndDelete_IgnoreBots(t *testing.T) {
	db, stmts := openTestDB(t)

	if _, err := stmts.insertMsg.Exec(
		"4001",
		"guild-4",
		"channel-4",
		"user-4",
		time.Now().UTC().Format(time.RFC3339Nano),
		"keep",
		"",
		"",
		"",
		"",
		"",
	); err != nil {
		t.Fatalf("seed insert failed: %v", err)
	}

	edited := time.Now().UTC()
	handleMessageUpdate(nil, stmts, &discordgo.MessageUpdate{
		Message: &discordgo.Message{
			ID:              "4001",
			GuildID:         "guild-4",
			ChannelID:       "channel-4",
			Content:         "bot edit",
			EditedTimestamp: &edited,
			Author: &discordgo.User{
				ID:  "bot-2",
				Bot: true,
			},
		},
	})

	handleMessageDelete(stmts, &discordgo.MessageDelete{
		Message: &discordgo.Message{
			ID:        "4001",
			GuildID:   "guild-4",
			ChannelID: "channel-4",
		},
		BeforeDelete: &discordgo.Message{
			Author: &discordgo.User{
				ID:  "bot-2",
				Bot: true,
			},
		},
	})

	var content, editedAt, deletedAt string
	if err := db.QueryRow(
		selectMessageContentEditedDeletedByIDQuery,
		"4001",
	).Scan(&content, &editedAt, &deletedAt); err != nil {
		t.Fatalf("query message after bot events failed: %v", err)
	}

	if content != "keep" {
		t.Fatalf("content changed by bot event: got %q", content)
	}
	if editedAt != "" {
		t.Fatalf("edited_at should remain empty for bot event, got %q", editedAt)
	}
	if deletedAt != "" {
		t.Fatalf("deleted_at should remain empty for bot event, got %q", deletedAt)
	}

	if got := mustCount(
		t,
		db,
		countLifecycleByMessageAndTypesQuery,
		"4001",
		string(eventMessageUpdated),
		string(eventMessageDeleted),
	); got != 0 {
		t.Fatalf("unexpected lifecycle events from bot update/delete: %d", got)
	}
}

func TestHandleMessageDelete_MarksAttachmentsDeleted(t *testing.T) {
	db, stmts := openTestDB(t)

	prevFetcher := attachmentTextFetcher
	attachmentTextFetcher = func(url string, maxBytes int) (string, error) {
		return "attachment text", nil
	}
	t.Cleanup(func() { attachmentTextFetcher = prevFetcher })

	handleMessageCreate(nil, db, stmts, &discordgo.MessageCreate{
		Message: &discordgo.Message{
			ID:        "5001",
			GuildID:   "guild-5",
			ChannelID: "channel-5",
			Content:   "message with attachment",
			Timestamp: time.Now().UTC(),
			Author: &discordgo.User{
				ID:            "user-5",
				Username:      "dolor",
				Discriminator: "0005",
			},
			Attachments: []*discordgo.MessageAttachment{
				{
					ID:          "att-5001",
					Filename:    "note.txt",
					ContentType: "text/plain",
					URL:         "https://example.test/note.txt",
					ProxyURL:    "https://example.test/proxy-note.txt",
					Size:        256,
				},
			},
		},
	})

	handleMessageDelete(stmts, &discordgo.MessageDelete{
		Message: &discordgo.Message{
			ID:        "5001",
			GuildID:   "guild-5",
			ChannelID: "channel-5",
		},
		BeforeDelete: &discordgo.Message{
			Author: &discordgo.User{
				ID: "user-5",
			},
		},
	})

	var deletedAt string
	if err := db.QueryRow(selectAttachmentDeletedAtByIDQuery, "att-5001").Scan(&deletedAt); err != nil {
		t.Fatalf("query attachment deleted_at failed: %v", err)
	}
	if deletedAt == "" {
		t.Fatal("expected attachment deleted_at to be set")
	}
}

func TestHandleMessageCreate_UpsertsUserNameMapping(t *testing.T) {
	db, stmts := openTestDB(t)

	handleMessageCreate(nil, db, stmts, &discordgo.MessageCreate{
		Message: &discordgo.Message{
			ID:        "6001",
			GuildID:   "guild-6",
			ChannelID: "channel-6",
			Content:   "hello",
			Timestamp: time.Now().UTC(),
			Author: &discordgo.User{
				ID:            "user-6",
				Username:      "ipsum",
				GlobalName:    "alice",
				Discriminator: "0006",
			},
		},
	})

	var humanName, normalizedName string
	if err := db.QueryRow(
		selectNameHumanAndNormalizedByTypeEntityGuildQuery,
		nameMappingEntityUser, "user-6", "guild-6",
	).Scan(&humanName, &normalizedName); err != nil {
		t.Fatalf("query user name mapping failed: %v", err)
	}

	if humanName != "ipsum" {
		t.Fatalf("human_name mismatch: got %q want %q", humanName, "ipsum")
	}
	if normalizedName != "ipsum" {
		t.Fatalf("normalized_name mismatch: got %q want %q", normalizedName, "ipsum")
	}
}

func TestHandleChannelCreate_UpsertsChannelNameMapping(t *testing.T) {
	db, stmts := openTestDB(t)

	handleChannelCreate(stmts, &discordgo.ChannelCreate{
		Channel: &discordgo.Channel{
			ID:      "channel-7",
			GuildID: "guild-7",
			Name:    "general",
			Type:    discordgo.ChannelTypeGuildText,
		},
	})

	var mappedID string
	if err := db.QueryRow(
		selectNameEntityByTypeGuildNormalizedQuery,
		nameMappingEntityChannel, "guild-7", "general",
	).Scan(&mappedID); err != nil {
		t.Fatalf("query channel name mapping failed: %v", err)
	}
	if mappedID != "channel-7" {
		t.Fatalf("entity_id mismatch: got %q want %q", mappedID, "channel-7")
	}
}

func TestHandleGuildCreateAndUpdate_UpsertGuildNameMapping(t *testing.T) {
	db, stmts := openTestDB(t)

	handleGuildCreate(stmts, &discordgo.GuildCreate{
		Guild: &discordgo.Guild{
			ID:   "guild-8",
			Name: "Lorem",
		},
	})
	handleGuildUpdate(db, stmts, &discordgo.GuildUpdate{
		Guild: &discordgo.Guild{
			ID:   "guild-8",
			Name: "Lorem Inc",
		},
	})

	var humanName string
	if err := db.QueryRow(
		selectNameHumanByTypeEntityGuildQuery,
		nameMappingEntityGuild, "guild-8", "guild-8",
	).Scan(&humanName); err != nil {
		t.Fatalf("query guild name mapping failed: %v", err)
	}
	if humanName != "Lorem Inc" {
		t.Fatalf("human_name mismatch: got %q want %q", humanName, "Lorem Inc")
	}

	var payloadJSON string
	if err := db.QueryRow(
		selectLifecyclePayloadByTypeAndGuildLatestQuery,
		string(eventGuildRenamed), "guild-8",
	).Scan(&payloadJSON); err != nil {
		t.Fatalf("query guild rename event failed: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("unmarshal payload failed: %v", err)
	}
	if payload["before_name"] != "Lorem" || payload["after_name"] != "Lorem Inc" {
		t.Fatalf("unexpected payload: %+v", payload)
	}
}

func TestHandleChannelUpdate_LogsRenameAndUpdatesMapping(t *testing.T) {
	db, stmts := openTestDB(t)

	handleChannelCreate(stmts, &discordgo.ChannelCreate{
		Channel: &discordgo.Channel{
			ID:      "channel-9",
			GuildID: "guild-9",
			Name:    "general",
			Type:    discordgo.ChannelTypeGuildText,
		},
	})

	handleChannelUpdate(stmts, &discordgo.ChannelUpdate{
		Channel: &discordgo.Channel{
			ID:      "channel-9",
			GuildID: "guild-9",
			Name:    "announcements",
			Type:    discordgo.ChannelTypeGuildText,
		},
		BeforeUpdate: &discordgo.Channel{
			ID:      "channel-9",
			GuildID: "guild-9",
			Name:    "general",
			Type:    discordgo.ChannelTypeGuildText,
		},
	})

	var mappedName string
	if err := db.QueryRow(
		selectNameHumanByTypeEntityGuildQuery,
		nameMappingEntityChannel, "channel-9", "guild-9",
	).Scan(&mappedName); err != nil {
		t.Fatalf("query updated channel name mapping failed: %v", err)
	}
	if mappedName != "announcements" {
		t.Fatalf("mapped name mismatch: got %q want %q", mappedName, "announcements")
	}

	var payloadJSON string
	if err := db.QueryRow(
		selectLifecyclePayloadByTypeGuildChannelLatestQuery,
		string(eventChannelRenamed), "guild-9", "channel-9",
	).Scan(&payloadJSON); err != nil {
		t.Fatalf("query channel rename event failed: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("unmarshal payload failed: %v", err)
	}
	if payload["before_name"] != "general" || payload["after_name"] != "announcements" {
		t.Fatalf("unexpected payload: %+v", payload)
	}
}

func TestHandleThreadUpdate_LogsRenameAndUpdatesMapping(t *testing.T) {
	db, stmts := openTestDB(t)

	handleThreadCreate(stmts, &discordgo.ThreadCreate{
		Channel: &discordgo.Channel{
			ID:       "thread-1",
			GuildID:  "guild-10",
			ParentID: "channel-parent",
			Name:     "topic-a",
			Type:     discordgo.ChannelTypeGuildPublicThread,
		},
	})

	handleThreadUpdate(stmts, &discordgo.ThreadUpdate{
		Channel: &discordgo.Channel{
			ID:       "thread-1",
			GuildID:  "guild-10",
			ParentID: "channel-parent",
			Name:     "topic-b",
			Type:     discordgo.ChannelTypeGuildPublicThread,
		},
		BeforeUpdate: &discordgo.Channel{
			ID:       "thread-1",
			GuildID:  "guild-10",
			ParentID: "channel-parent",
			Name:     "topic-a",
			Type:     discordgo.ChannelTypeGuildPublicThread,
		},
	})

	var payloadJSON string
	if err := db.QueryRow(
		selectLifecyclePayloadByTypeGuildChannelLatestQuery,
		string(eventThreadRenamed), "guild-10", "thread-1",
	).Scan(&payloadJSON); err != nil {
		t.Fatalf("query thread rename event failed: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("unmarshal payload failed: %v", err)
	}
	if payload["before_name"] != "topic-a" || payload["after_name"] != "topic-b" {
		t.Fatalf("unexpected payload: %+v", payload)
	}
}

func TestHandleGuildMemberUpdate_LogsUsernameChangeAndUpdatesMapping(t *testing.T) {
	db, stmts := openTestDB(t)

	now := time.Now().UTC().Format(time.RFC3339Nano)
	if err := upsertNameMappingRow(stmts.upsertIDNameMapping, nameMappingEntityUser, "user-11", "guild-11", "oldname", now); err != nil {
		t.Fatalf("seed user name mapping failed: %v", err)
	}
	if err := upsertNameMappingRow(stmts.upsertIDNameMapping, nameMappingEntityRole, "role-old", "guild-11", "oldname2", now); err != nil {
		t.Fatalf("seed old role name mapping failed: %v", err)
	}
	if err := upsertNameMappingRow(stmts.upsertIDNameMapping, nameMappingEntityRole, "role-new", "guild-11", "Consectetur", now); err != nil {
		t.Fatalf("seed new role name mapping failed: %v", err)
	}

	handleGuildMemberUpdate(db, stmts, &discordgo.GuildMemberUpdate{
		Member: &discordgo.Member{
			GuildID: "guild-11",
			Nick:    "nick-after",
			Roles:   []string{"role-new"},
			User: &discordgo.User{
				ID:            "user-11",
				Username:      "newname",
				GlobalName:    "New Name",
				Discriminator: "0011",
			},
		},
		BeforeUpdate: &discordgo.Member{
			GuildID: "guild-11",
			Nick:    "nick-before",
			Roles:   []string{"role-old"},
			User: &discordgo.User{
				ID:            "user-11",
				Username:      "oldname",
				GlobalName:    "Old Name",
				Discriminator: "0011",
			},
		},
	})

	var mappedName string
	if err := db.QueryRow(
		selectNameHumanByTypeEntityGuildQuery,
		nameMappingEntityUser, "user-11", "guild-11",
	).Scan(&mappedName); err != nil {
		t.Fatalf("query user name mapping failed: %v", err)
	}
	if mappedName != "newname" {
		t.Fatalf("mapped name mismatch: got %q want %q", mappedName, "newname")
	}

	var payloadJSON string
	if err := db.QueryRow(
		selectLifecyclePayloadByTypeGuildActorLatestQuery,
		string(eventUsernameChanged), "guild-11", "user-11",
	).Scan(&payloadJSON); err != nil {
		t.Fatalf("query username changed event failed: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("unmarshal payload failed: %v", err)
	}
	if payload["before_username"] != "oldname" || payload["after_username"] != "newname" {
		t.Fatalf("unexpected payload: %+v", payload)
	}

	var memberRoleCount int
	if err := db.QueryRow(
		countMemberRolesByGuildUserRoleQuery,
		"guild-11", "user-11", "role-new",
	).Scan(&memberRoleCount); err != nil {
		t.Fatalf("query member role failed: %v", err)
	}
	if memberRoleCount != 1 {
		t.Fatalf("expected member role snapshot count=1, got %d", memberRoleCount)
	}

	var roleAssignedPayloadJSON string
	if err := db.QueryRow(
		selectLifecyclePayloadByTypeGuildActorLatestQuery,
		string(eventRoleAssigned), "guild-11", "user-11",
	).Scan(&roleAssignedPayloadJSON); err != nil {
		t.Fatalf("query role assigned event failed: %v", err)
	}
	var roleAssignedPayload map[string]any
	if err := json.Unmarshal([]byte(roleAssignedPayloadJSON), &roleAssignedPayload); err != nil {
		t.Fatalf("unmarshal role assigned payload failed: %v", err)
	}
	if roleAssignedPayload["role_id"] != "role-new" || roleAssignedPayload["role_name"] != "Consectetur" {
		t.Fatalf("unexpected role assigned payload: %+v", roleAssignedPayload)
	}

	var roleRevokedPayloadJSON string
	if err := db.QueryRow(
		selectLifecyclePayloadByTypeGuildActorLatestQuery,
		string(eventRoleRevoked), "guild-11", "user-11",
	).Scan(&roleRevokedPayloadJSON); err != nil {
		t.Fatalf("query role revoked event failed: %v", err)
	}
	var roleRevokedPayload map[string]any
	if err := json.Unmarshal([]byte(roleRevokedPayloadJSON), &roleRevokedPayload); err != nil {
		t.Fatalf("unmarshal role revoked payload failed: %v", err)
	}
	if roleRevokedPayload["role_id"] != "role-old" || roleRevokedPayload["role_name"] != "oldname2" {
		t.Fatalf("unexpected role revoked payload: %+v", roleRevokedPayload)
	}
}

func TestHandleGuildMemberAdd_UpsertsGuildMemberSnapshot(t *testing.T) {
	db, stmts := openTestDB(t)

	handleGuildMemberAdd(stmts, &discordgo.GuildMemberAdd{
		Member: &discordgo.Member{
			GuildID: "guild-15",
			User: &discordgo.User{
				ID:       "user-15",
				Username: "newmember",
			},
		},
	})

	var count int
	if err := db.QueryRow(
		countGuildMembersByGuildUserQuery,
		"guild-15", "user-15",
	).Scan(&count); err != nil {
		t.Fatalf("query guild_members failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected guild_members row count=1, got %d", count)
	}

	var payloadJSON string
	if err := db.QueryRow(
		selectLifecyclePayloadByTypeGuildActorLatestQuery,
		string(eventUserJoined), "guild-15", "user-15",
	).Scan(&payloadJSON); err != nil {
		t.Fatalf("query user joined event failed: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("unmarshal user joined payload failed: %v", err)
	}
	if payload["user_id"] != "user-15" || payload["username"] != "newmember" {
		t.Fatalf("unexpected user joined payload: %+v", payload)
	}
}

func TestHandleGuildMemberRemove_DeletesGuildMemberSnapshot(t *testing.T) {
	db, stmts := openTestDB(t)

	now := time.Now().UTC().Format(time.RFC3339Nano)
	if err := upsertGuildMemberRow(stmts.upsertGuildMember, "guild-16", "user-16", now); err != nil {
		t.Fatalf("seed guild member failed: %v", err)
	}

	handleGuildMemberRemove(nil, stmts, &discordgo.GuildMemberRemove{
		Member: &discordgo.Member{
			GuildID: "guild-16",
			User: &discordgo.User{
				ID:       "user-16",
				Username: "leaver",
			},
		},
	})

	var count int
	if err := db.QueryRow(
		countGuildMembersByGuildUserQuery,
		"guild-16", "user-16",
	).Scan(&count); err != nil {
		t.Fatalf("query guild_members failed: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected guild_members row count=0, got %d", count)
	}

	var payloadJSON string
	if err := db.QueryRow(
		selectLifecyclePayloadByTypeGuildActorLatestQuery,
		string(eventUserLeft), "guild-16", "user-16",
	).Scan(&payloadJSON); err != nil {
		t.Fatalf("query user left event failed: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("unmarshal user left payload failed: %v", err)
	}
	if payload["user_id"] != "user-16" || payload["removal_cause"] != string(memberRemovalCauseLeft) {
		t.Fatalf("unexpected user left payload: %+v", payload)
	}
}

func TestHandleGuildBanAdd_DeletesGuildMemberSnapshot(t *testing.T) {
	db, stmts := openTestDB(t)

	now := time.Now().UTC().Format(time.RFC3339Nano)
	if err := upsertGuildMemberRow(stmts.upsertGuildMember, "guild-17", "user-17", now); err != nil {
		t.Fatalf("seed guild member failed: %v", err)
	}

	handleGuildBanAdd(nil, stmts, &discordgo.GuildBanAdd{
		GuildID: "guild-17",
		User: &discordgo.User{
			ID:       "user-17",
			Username: "banned",
		},
	})

	var count int
	if err := db.QueryRow(
		countGuildMembersByGuildUserQuery,
		"guild-17", "user-17",
	).Scan(&count); err != nil {
		t.Fatalf("query guild_members failed: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected guild_members row count=0, got %d", count)
	}

	var payloadJSON string
	if err := db.QueryRow(
		selectLifecyclePayloadByTypeAndGuildLatestQuery,
		string(eventUserBanned), "guild-17",
	).Scan(&payloadJSON); err != nil {
		t.Fatalf("query user banned event failed: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("unmarshal user banned payload failed: %v", err)
	}
	if payload["user_id"] != "user-17" || payload["username"] != "banned" {
		t.Fatalf("unexpected user banned payload: %+v", payload)
	}
}

func TestHandleGuildMemberRemove_RecordsKickEventWhenAuditLogMatches(t *testing.T) {
	db, stmts := openTestDB(t)

	now := time.Now().UTC().Format(time.RFC3339Nano)
	if err := upsertGuildMemberRow(stmts.upsertGuildMember, "guild-18", "user-18", now); err != nil {
		t.Fatalf("seed guild member failed: %v", err)
	}

	prevFetcher := guildAuditLogFetcher
	guildAuditLogFetcher = func(
		s *discordgo.Session,
		guildID, userID string,
		actionType discordgo.AuditLogAction,
		limit int,
	) (*discordgo.GuildAuditLog, error) {
		if actionType != discordgo.AuditLogActionMemberKick {
			return &discordgo.GuildAuditLog{}, nil
		}
		kickAction := discordgo.AuditLogActionMemberKick
		return &discordgo.GuildAuditLog{
			AuditLogEntries: []*discordgo.AuditLogEntry{
				{
					ID:         testAuditEntryIDForNow(),
					TargetID:   userID,
					UserID:     "mod-18",
					Reason:     "rule violation",
					ActionType: &kickAction,
				},
			},
		}, nil
	}
	t.Cleanup(func() { guildAuditLogFetcher = prevFetcher })

	handleGuildMemberRemove(&discordgo.Session{}, stmts, &discordgo.GuildMemberRemove{
		Member: &discordgo.Member{
			GuildID: "guild-18",
			User: &discordgo.User{
				ID:       "user-18",
				Username: "kicked-user",
			},
		},
	})

	var payloadJSON string
	if err := db.QueryRow(
		selectLifecyclePayloadByTypeGuildActorLatestQuery,
		string(eventUserKicked), "guild-18", "mod-18",
	).Scan(&payloadJSON); err != nil {
		t.Fatalf("query user kicked event failed: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("unmarshal user kicked payload failed: %v", err)
	}
	if payload["user_id"] != "user-18" || payload["removal_cause"] != string(memberRemovalCauseKicked) {
		t.Fatalf("unexpected user kicked payload: %+v", payload)
	}
	if payload["moderator_id"] != "mod-18" || payload["reason"] != "rule violation" {
		t.Fatalf("unexpected user kicked moderation payload: %+v", payload)
	}
}

func TestHandleMessageCreate_RecordsUserPingEvents(t *testing.T) {
	db, stmts := openTestDB(t)

	handleMessageCreate(nil, db, stmts, &discordgo.MessageCreate{
		Message: &discordgo.Message{
			ID:        "7001",
			GuildID:   "guild-12",
			ChannelID: "channel-12",
			Content:   "hey @dolor check this",
			Timestamp: time.Now().UTC(),
			Author: &discordgo.User{
				ID:            "user-12a",
				Username:      "alice",
				Discriminator: "0012",
			},
			Mentions: []*discordgo.User{
				{
					ID:            "user-12b",
					Username:      "dolor",
					Discriminator: "0013",
				},
			},
		},
	})

	var targetID, targetName string
	if err := db.QueryRow(
		selectPingTargetByGuildMessageActorQuery,
		"guild-12", "7001", "user-12a",
	).Scan(&targetID, &targetName); err != nil {
		t.Fatalf("query ping event failed: %v", err)
	}
	if targetID != "user-12b" || targetName != "dolor" {
		t.Fatalf("unexpected ping row: target_id=%q target_name=%q", targetID, targetName)
	}

	var payloadJSON string
	if err := db.QueryRow(
		selectLifecyclePayloadByTypeGuildMessageActorLatestQuery,
		string(eventUserPinged), "guild-12", "7001", "user-12a",
	).Scan(&payloadJSON); err != nil {
		t.Fatalf("query user_pinged event failed: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("unmarshal payload failed: %v", err)
	}
	if payload["target_user_id"] != "user-12b" || payload["target_username"] != "dolor" {
		t.Fatalf("unexpected payload: %+v", payload)
	}
}

func TestHandleMessageCreate_RecordsSelfUserPingEvents(t *testing.T) {
	db, stmts := openTestDB(t)

	handleMessageCreate(nil, db, stmts, &discordgo.MessageCreate{
		Message: &discordgo.Message{
			ID:        "7002",
			GuildID:   "guild-13",
			ChannelID: "channel-13",
			Content:   "pinging myself",
			Timestamp: time.Now().UTC(),
			Author: &discordgo.User{
				ID:            "user-13a",
				Username:      "ipsum_user",
				Discriminator: "0013",
			},
			Mentions: []*discordgo.User{
				{
					ID:            "user-13a",
					Username:      "ipsum_user",
					Discriminator: "0013",
				},
			},
		},
	})

	var targetID, targetName string
	if err := db.QueryRow(
		selectPingTargetByGuildMessageActorQuery,
		"guild-13", "7002", "user-13a",
	).Scan(&targetID, &targetName); err != nil {
		t.Fatalf("query ping event failed: %v", err)
	}
	if targetID != "user-13a" || targetName != "ipsum_user" {
		t.Fatalf("unexpected ping row: target_id=%q target_name=%q", targetID, targetName)
	}

	var payloadJSON string
	if err := db.QueryRow(
		selectLifecyclePayloadByTypeGuildMessageActorLatestQuery,
		string(eventUserPinged), "guild-13", "7002", "user-13a",
	).Scan(&payloadJSON); err != nil {
		t.Fatalf("query user_pinged event failed: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("unmarshal payload failed: %v", err)
	}
	if payload["target_user_id"] != "user-13a" || payload["target_username"] != "ipsum_user" {
		t.Fatalf("unexpected payload: %+v", payload)
	}
}

func TestHandleMessageCreate_RecordsRolePingEvents(t *testing.T) {
	db, stmts := openTestDB(t)

	handleMessageCreate(nil, db, stmts, &discordgo.MessageCreate{
		Message: &discordgo.Message{
			ID:        "7003",
			GuildID:   "guild-14",
			ChannelID: "channel-14",
			Content:   "hey <@&role-14>",
			Timestamp: time.Now().UTC(),
			Author: &discordgo.User{
				ID:            "user-14a",
				Username:      "alice",
				Discriminator: "0014",
			},
			MentionRoles: []string{"role-14"},
		},
	})

	var roleID, roleName string
	if err := db.QueryRow(
		selectRolePingByGuildMessageActorQuery,
		"guild-14", "7003", "user-14a",
	).Scan(&roleID, &roleName); err != nil {
		t.Fatalf("query role ping event failed: %v", err)
	}
	if roleID != "role-14" || roleName != "role-14" {
		t.Fatalf("unexpected role ping row: role_id=%q role_name=%q", roleID, roleName)
	}

	var payloadJSON string
	if err := db.QueryRow(
		selectLifecyclePayloadByTypeGuildMessageActorLatestQuery,
		string(eventRolePinged), "guild-14", "7003", "user-14a",
	).Scan(&payloadJSON); err != nil {
		t.Fatalf("query role_pinged event failed: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("unmarshal payload failed: %v", err)
	}
	if payload["target_role_id"] != "role-14" || payload["target_role_name"] != "role-14" {
		t.Fatalf("unexpected payload: %+v", payload)
	}
}
