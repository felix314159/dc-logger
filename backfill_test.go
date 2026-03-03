// Package main tests startup backfill message persistence behavior.
package main

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
)

func TestPersistBackfillMessage_RecordsPingEvents(t *testing.T) {
	db, stmts := openTestDB(t)

	inserted, err := persistBackfillMessage(db, stmts, nil, "guild-14", "channel-14", "", &discordgo.Message{
		ID:        "8001",
		GuildID:   "guild-14",
		ChannelID: "channel-14",
		Content:   "hello <@user-14b> <@&role-14>",
		Timestamp: time.Now().UTC(),
		Author: &discordgo.User{
			ID:            "user-14a",
			Username:      "alice",
			Discriminator: "0014",
		},
		Mentions: []*discordgo.User{
			{
				ID:            "user-14b",
				Username:      "dolor",
				Discriminator: "0015",
			},
		},
		MentionRoles: []string{"role-14"},
	})
	if err != nil {
		t.Fatalf("persistBackfillMessage failed: %v", err)
	}
	if !inserted {
		t.Fatal("expected inserted=true")
	}

	var targetID, targetName string
	if err := db.QueryRow(
		selectPingTargetByGuildMessageActorQuery,
		"guild-14", "8001", "user-14a",
	).Scan(&targetID, &targetName); err != nil {
		t.Fatalf("query ping event failed: %v", err)
	}
	if targetID != "user-14b" || targetName != "dolor" {
		t.Fatalf("unexpected ping row: target_id=%q target_name=%q", targetID, targetName)
	}

	var messageSentPayload string
	if err := db.QueryRow(
		selectLifecyclePayloadByTypeGuildMessageActorLatestQuery,
		string(eventMessageSent), "guild-14", "8001", "user-14a",
	).Scan(&messageSentPayload); err != nil {
		t.Fatalf("query message_sent lifecycle event failed: %v", err)
	}
	if !strings.Contains(messageSentPayload, `"backfilled":true`) {
		t.Fatalf("expected backfilled marker in message_sent payload, got %s", messageSentPayload)
	}

	var payloadJSON string
	if err := db.QueryRow(
		selectLifecyclePayloadByTypeGuildMessageActorLatestQuery,
		string(eventUserPinged), "guild-14", "8001", "user-14a",
	).Scan(&payloadJSON); err != nil {
		t.Fatalf("query user_pinged event failed: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("unmarshal payload failed: %v", err)
	}
	if payload["target_user_id"] != "user-14b" || payload["target_username"] != "dolor" {
		t.Fatalf("unexpected payload: %+v", payload)
	}

	var roleID, roleName string
	if err := db.QueryRow(
		selectRolePingByGuildMessageActorQuery,
		"guild-14", "8001", "user-14a",
	).Scan(&roleID, &roleName); err != nil {
		t.Fatalf("query role ping event failed: %v", err)
	}
	if roleID != "role-14" || roleName != "role-14" {
		t.Fatalf("unexpected role ping row: role_id=%q role_name=%q", roleID, roleName)
	}
}

func TestBackfillChannel_FullHistoryColdStart_MultiPage(t *testing.T) {
	db, stmts := openTestDB(t)

	prevChannelMessagesFetcher := channelMessagesFetcher
	t.Cleanup(func() { channelMessagesFetcher = prevChannelMessagesFetcher })

	type pageCall struct {
		before string
		after  string
	}
	var calls []pageCall

	channelMessagesFetcher = func(
		s *discordgo.Session,
		channelID string,
		limit int,
		beforeID, afterID, aroundID string,
	) ([]*discordgo.Message, error) {
		calls = append(calls, pageCall{before: beforeID, after: afterID})
		switch len(calls) {
		case 1:
			return []*discordgo.Message{
				testBackfillMessage("5"),
				testBackfillMessage("4"),
				testBackfillMessage("3"),
			}, nil
		case 2:
			if beforeID != "3" || afterID != "" {
				t.Fatalf("unexpected second-page cursor: before=%q after=%q", beforeID, afterID)
			}
			return []*discordgo.Message{
				testBackfillMessage("2"),
				testBackfillMessage("1"),
			}, nil
		case 3:
			if beforeID != "1" || afterID != "" {
				t.Fatalf("unexpected third-page cursor: before=%q after=%q", beforeID, afterID)
			}
			return []*discordgo.Message{}, nil
		default:
			t.Fatalf("unexpected extra ChannelMessages call #%d", len(calls))
			return nil, nil
		}
	}

	inserted, maxID, budgetReached, err := backfillChannel(
		&discordgo.Session{},
		db,
		stmts,
		"guild-cold",
		"channel-cold",
		"",
		"",
		nil,
	)
	if err != nil {
		t.Fatalf("backfillChannel failed: %v", err)
	}
	if budgetReached {
		t.Fatal("expected budgetReached=false")
	}
	if inserted != 5 {
		t.Fatalf("inserted mismatch: got %d want 5", inserted)
	}
	if maxID != "5" {
		t.Fatalf("maxID mismatch: got %q want %q", maxID, "5")
	}

	if got := mustCount(t, db, countMessagesByGuildIDQuery, "guild-cold"); got != 5 {
		t.Fatalf("unexpected guild message count: got %d want 5", got)
	}
	if len(calls) != 3 {
		t.Fatalf("ChannelMessages call count mismatch: got %d want 3", len(calls))
	}
	if calls[0].before != "" || calls[0].after != "" {
		t.Fatalf("first call should be cold-start crawl, got before=%q after=%q", calls[0].before, calls[0].after)
	}
}

func TestBackfillGuild_ResumesFromLatestStoredMessageWhenStateMissing(t *testing.T) {
	db, stmts := openTestDB(t)

	createdAt := time.Now().UTC().Add(-2 * time.Minute).Format(time.RFC3339Nano)
	if _, err := db.Exec(
		insertMessageQuery,
		"500",
		"guild-resume",
		"channel-resume",
		"user-old",
		createdAt,
		"old message",
		"",
		"",
		"",
		"",
		"",
	); err != nil {
		t.Fatalf("seed message insert failed: %v", err)
	}

	if got := mustCount(t, db, countMessagesByChannelIDQuery, "channel-resume"); got != 1 {
		t.Fatalf("unexpected seed count: got %d want 1", got)
	}

	prevGuildChannelsFetcher := guildChannelsFetcher
	prevGuildActiveThreadsFetcher := guildActiveThreadsFetcher
	prevPublicArchivedThreadsFetcher := publicArchivedThreadsFetcher
	prevPrivateArchivedThreadsFetcher := privateArchivedThreadsFetcher
	prevJoinedPrivateArchivedThreadsFetcher := joinedPrivateArchivedThreadsFetcher
	prevChannelMessagesFetcher := channelMessagesFetcher
	t.Cleanup(func() {
		guildChannelsFetcher = prevGuildChannelsFetcher
		guildActiveThreadsFetcher = prevGuildActiveThreadsFetcher
		publicArchivedThreadsFetcher = prevPublicArchivedThreadsFetcher
		privateArchivedThreadsFetcher = prevPrivateArchivedThreadsFetcher
		joinedPrivateArchivedThreadsFetcher = prevJoinedPrivateArchivedThreadsFetcher
		channelMessagesFetcher = prevChannelMessagesFetcher
	})

	guildChannelsFetcher = func(s *discordgo.Session, guildID string) ([]*discordgo.Channel, error) {
		return []*discordgo.Channel{
			{ID: "channel-resume", GuildID: guildID, Type: discordgo.ChannelTypeGuildText, Name: "resume"},
		}, nil
	}
	guildActiveThreadsFetcher = func(s *discordgo.Session, guildID string) ([]*discordgo.Channel, error) {
		return []*discordgo.Channel{}, nil
	}
	publicArchivedThreadsFetcher = func(
		s *discordgo.Session,
		channelID string,
		before *time.Time,
		limit int,
	) (*discordgo.ThreadsList, error) {
		return &discordgo.ThreadsList{}, nil
	}
	privateArchivedThreadsFetcher = func(
		s *discordgo.Session,
		channelID string,
		before *time.Time,
		limit int,
	) (*discordgo.ThreadsList, error) {
		return &discordgo.ThreadsList{}, nil
	}
	joinedPrivateArchivedThreadsFetcher = func(
		s *discordgo.Session,
		channelID string,
		before *time.Time,
		limit int,
	) (*discordgo.ThreadsList, error) {
		return &discordgo.ThreadsList{}, nil
	}

	calls := 0
	channelMessagesFetcher = func(
		s *discordgo.Session,
		channelID string,
		limit int,
		beforeID, afterID, aroundID string,
	) ([]*discordgo.Message, error) {
		calls++
		switch calls {
		case 1:
			if beforeID != "" || afterID != "500" {
				t.Fatalf("expected incremental resume from 500, got before=%q after=%q", beforeID, afterID)
			}
			return []*discordgo.Message{
				{
					ID:        "600",
					GuildID:   "guild-resume",
					ChannelID: "channel-resume",
					Content:   "new message",
					Timestamp: time.Now().UTC(),
					Author: &discordgo.User{
						ID:            "user-new",
						Username:      "new-user",
						Discriminator: "0001",
					},
				},
			}, nil
		case 2:
			if beforeID != "" || afterID != "600" {
				t.Fatalf("expected next incremental cursor after=600, got before=%q after=%q", beforeID, afterID)
			}
			return []*discordgo.Message{}, nil
		default:
			t.Fatalf("unexpected ChannelMessages call #%d", calls)
			return nil, nil
		}
	}

	if err := backfillGuild(&discordgo.Session{}, db, stmts, "guild-resume", nil); err != nil {
		t.Fatalf("backfillGuild failed: %v", err)
	}

	if got := mustCount(t, db, countMessagesByChannelIDQuery, "channel-resume"); got != 2 {
		t.Fatalf("unexpected message count after resume backfill: got %d want 2", got)
	}

	var lastState string
	if err := db.QueryRow(selectChannelStateLastMessageByChannelQuery, "channel-resume").Scan(&lastState); err != nil {
		t.Fatalf("query channel_state failed: %v", err)
	}
	if lastState != "600" {
		t.Fatalf("channel_state last_message_id mismatch: got %q want %q", lastState, "600")
	}
}

func TestBackfillGuild_IncludesArchivedThreads(t *testing.T) {
	db, stmts := openTestDB(t)

	prevGuildChannelsFetcher := guildChannelsFetcher
	prevGuildActiveThreadsFetcher := guildActiveThreadsFetcher
	prevPublicArchivedThreadsFetcher := publicArchivedThreadsFetcher
	prevPrivateArchivedThreadsFetcher := privateArchivedThreadsFetcher
	prevJoinedPrivateArchivedThreadsFetcher := joinedPrivateArchivedThreadsFetcher
	prevChannelMessagesFetcher := channelMessagesFetcher
	t.Cleanup(func() {
		guildChannelsFetcher = prevGuildChannelsFetcher
		guildActiveThreadsFetcher = prevGuildActiveThreadsFetcher
		publicArchivedThreadsFetcher = prevPublicArchivedThreadsFetcher
		privateArchivedThreadsFetcher = prevPrivateArchivedThreadsFetcher
		joinedPrivateArchivedThreadsFetcher = prevJoinedPrivateArchivedThreadsFetcher
		channelMessagesFetcher = prevChannelMessagesFetcher
	})

	guildChannelsFetcher = func(s *discordgo.Session, guildID string) ([]*discordgo.Channel, error) {
		return []*discordgo.Channel{
			{ID: "100", GuildID: guildID, Type: discordgo.ChannelTypeGuildText, Name: "general"},
			{ID: "200", GuildID: guildID, Type: discordgo.ChannelTypeGuildForum, Name: "forum"},
			{ID: "300", GuildID: guildID, Type: discordgo.ChannelTypeGuildVoice, Name: "voice"},
		}, nil
	}
	guildActiveThreadsFetcher = func(s *discordgo.Session, guildID string) ([]*discordgo.Channel, error) {
		return []*discordgo.Channel{}, nil
	}

	var publicArchiveCalls []string
	publicArchivedThreadsFetcher = func(
		s *discordgo.Session,
		channelID string,
		before *time.Time,
		limit int,
	) (*discordgo.ThreadsList, error) {
		publicArchiveCalls = append(publicArchiveCalls, channelID)
		switch channelID {
		case "100":
			return &discordgo.ThreadsList{
				Threads: []*discordgo.Channel{
					{
						ID:       "110",
						GuildID:  "guild-arch",
						ParentID: "100",
						Type:     discordgo.ChannelTypeGuildPublicThread,
						Name:     "archived-text-thread",
						ThreadMetadata: &discordgo.ThreadMetadata{
							Archived:         true,
							ArchiveTimestamp: time.Now().UTC().Add(-2 * time.Hour),
						},
					},
				},
			}, nil
		case "200":
			return &discordgo.ThreadsList{
				Threads: []*discordgo.Channel{
					{
						ID:       "210",
						GuildID:  "guild-arch",
						ParentID: "200",
						Type:     discordgo.ChannelTypeGuildPublicThread,
						Name:     "archived-forum-post",
						ThreadMetadata: &discordgo.ThreadMetadata{
							Archived:         true,
							ArchiveTimestamp: time.Now().UTC().Add(-1 * time.Hour),
						},
					},
				},
			}, nil
		default:
			return &discordgo.ThreadsList{}, nil
		}
	}
	privateArchivedThreadsFetcher = func(
		s *discordgo.Session,
		channelID string,
		before *time.Time,
		limit int,
	) (*discordgo.ThreadsList, error) {
		if channelID != "100" {
			return &discordgo.ThreadsList{}, nil
		}
		return &discordgo.ThreadsList{
			Threads: []*discordgo.Channel{
				{
					ID:       "120",
					GuildID:  "guild-arch",
					ParentID: "100",
					Type:     discordgo.ChannelTypeGuildPrivateThread,
					Name:     "archived-private-thread",
					ThreadMetadata: &discordgo.ThreadMetadata{
						Archived:         true,
						ArchiveTimestamp: time.Now().UTC().Add(-3 * time.Hour),
					},
				},
			},
		}, nil
	}
	joinedPrivateArchivedThreadsFetcher = func(
		s *discordgo.Session,
		channelID string,
		before *time.Time,
		limit int,
	) (*discordgo.ThreadsList, error) {
		return &discordgo.ThreadsList{}, nil
	}

	messagePageCalls := make(map[string]int)
	channelMessagesFetcher = func(
		s *discordgo.Session,
		channelID string,
		limit int,
		beforeID, afterID, aroundID string,
	) ([]*discordgo.Message, error) {
		messagePageCalls[channelID]++
		if messagePageCalls[channelID] == 1 {
			return []*discordgo.Message{
				testBackfillMessage(channelID + "1"),
			}, nil
		}
		return []*discordgo.Message{}, nil
	}

	if err := backfillGuild(&discordgo.Session{}, db, stmts, "guild-arch", nil); err != nil {
		t.Fatalf("backfillGuild failed: %v", err)
	}

	if got := mustCount(t, db, countMessagesByGuildIDQuery, "guild-arch"); got != 4 {
		t.Fatalf("unexpected archived-backfill message count: got %d want 4", got)
	}
	if got := mustCount(t, db, countMessagesByChannelIDQuery, "200"); got != 0 {
		t.Fatalf("forum parent channel should not be directly backfilled, got %d rows", got)
	}
	if got := mustCount(t, db, countMessagesByChannelIDQuery, "110"); got != 1 {
		t.Fatalf("expected archived public thread rows=1, got %d", got)
	}
	if got := mustCount(t, db, countMessagesByChannelIDQuery, "120"); got != 1 {
		t.Fatalf("expected archived private thread rows=1, got %d", got)
	}
	if got := mustCount(t, db, countMessagesByChannelIDQuery, "210"); got != 1 {
		t.Fatalf("expected archived forum thread rows=1, got %d", got)
	}
	if got := mustCount(t, db, countChannelsByChannelIDQuery, "210"); got != 1 {
		t.Fatalf("expected archived thread metadata row for channel 210, got %d", got)
	}
	if !slices.Contains(publicArchiveCalls, "200") {
		t.Fatalf("expected archived thread fetch for forum parent channel 200, calls=%v", publicArchiveCalls)
	}
}

func TestBackfillGuild_ChannelMissingAccessLogsSummaryOnce(t *testing.T) {
	db, stmts := openTestDB(t)

	prevGuildChannelsFetcher := guildChannelsFetcher
	prevGuildActiveThreadsFetcher := guildActiveThreadsFetcher
	prevPublicArchivedThreadsFetcher := publicArchivedThreadsFetcher
	prevPrivateArchivedThreadsFetcher := privateArchivedThreadsFetcher
	prevJoinedPrivateArchivedThreadsFetcher := joinedPrivateArchivedThreadsFetcher
	prevChannelMessagesFetcher := channelMessagesFetcher

	oldWriter := log.Writer()
	oldFlags := log.Flags()
	var logBuf bytes.Buffer

	t.Cleanup(func() {
		guildChannelsFetcher = prevGuildChannelsFetcher
		guildActiveThreadsFetcher = prevGuildActiveThreadsFetcher
		publicArchivedThreadsFetcher = prevPublicArchivedThreadsFetcher
		privateArchivedThreadsFetcher = prevPrivateArchivedThreadsFetcher
		joinedPrivateArchivedThreadsFetcher = prevJoinedPrivateArchivedThreadsFetcher
		channelMessagesFetcher = prevChannelMessagesFetcher
		log.SetOutput(oldWriter)
		log.SetFlags(oldFlags)
	})

	log.SetOutput(&logBuf)
	log.SetFlags(0)

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

	guildChannelsFetcher = func(s *discordgo.Session, guildID string) ([]*discordgo.Channel, error) {
		return []*discordgo.Channel{
			{ID: "100", GuildID: guildID, Type: discordgo.ChannelTypeGuildText, Name: "visible-1"},
			{ID: "200", GuildID: guildID, Type: discordgo.ChannelTypeGuildText, Name: "visible-2"},
			{ID: "300", GuildID: guildID, Type: discordgo.ChannelTypeGuildText, Name: "visible-3"},
		}, nil
	}
	guildActiveThreadsFetcher = func(s *discordgo.Session, guildID string) ([]*discordgo.Channel, error) {
		return []*discordgo.Channel{}, nil
	}
	publicArchivedThreadsFetcher = func(
		s *discordgo.Session,
		channelID string,
		before *time.Time,
		limit int,
	) (*discordgo.ThreadsList, error) {
		return &discordgo.ThreadsList{}, nil
	}
	privateArchivedThreadsFetcher = func(
		s *discordgo.Session,
		channelID string,
		before *time.Time,
		limit int,
	) (*discordgo.ThreadsList, error) {
		return &discordgo.ThreadsList{}, nil
	}
	joinedPrivateArchivedThreadsFetcher = func(
		s *discordgo.Session,
		channelID string,
		before *time.Time,
		limit int,
	) (*discordgo.ThreadsList, error) {
		return &discordgo.ThreadsList{}, nil
	}
	channelMessagesFetcher = func(
		s *discordgo.Session,
		channelID string,
		limit int,
		beforeID, afterID, aroundID string,
	) ([]*discordgo.Message, error) {
		switch channelID {
		case "100", "200":
			return nil, missingAccessErr
		default:
			return []*discordgo.Message{}, nil
		}
	}

	if err := backfillGuild(&discordgo.Session{}, db, stmts, "guild-missing", nil); err != nil {
		t.Fatalf("backfillGuild failed: %v", err)
	}

	logOutput := logBuf.String()
	if strings.Contains(logOutput, "backfill channel=") {
		t.Fatalf("unexpected per-channel backfill error spam in logs: %q", logOutput)
	}
	if strings.Count(logOutput, "backfill skipped 2 channel(s)/thread(s) due to missing access") != 1 {
		t.Fatalf("expected exactly one missing-access summary log, logs=%q", logOutput)
	}
}

func TestPersistBackfillMessage_PersistsRelationshipFields(t *testing.T) {
	db, stmts := openTestDB(t)

	inserted, err := persistBackfillMessage(db, stmts, nil, "guild-rel", "thread-rel", "parent-rel", &discordgo.Message{
		ID:        "8100",
		GuildID:   "guild-rel",
		ChannelID: "thread-rel",
		Content:   "reply in thread",
		Timestamp: time.Now().UTC(),
		MessageReference: &discordgo.MessageReference{
			MessageID: "origin-rel",
			ChannelID: "parent-rel",
			GuildID:   "guild-rel",
		},
		Author: &discordgo.User{
			ID:            "user-rel",
			Username:      "reply-user",
			Discriminator: "0810",
		},
	})
	if err != nil {
		t.Fatalf("persistBackfillMessage failed: %v", err)
	}
	if !inserted {
		t.Fatal("expected inserted=true")
	}

	var (
		refMessageID string
		refChannelID string
		refGuildID   string
		threadID     string
		threadParent string
	)
	if err := db.QueryRow(
		selectMessageRelationshipByMessageIDQuery,
		"8100",
	).Scan(&refMessageID, &refChannelID, &refGuildID, &threadID, &threadParent); err != nil {
		t.Fatalf("query message relationship fields failed: %v", err)
	}
	if refMessageID != "origin-rel" || refChannelID != "parent-rel" || refGuildID != "guild-rel" {
		t.Fatalf(
			"unexpected referenced ids: message=%q channel=%q guild=%q",
			refMessageID,
			refChannelID,
			refGuildID,
		)
	}
	if threadID != "thread-rel" || threadParent != "parent-rel" {
		t.Fatalf("unexpected thread context: thread_id=%q thread_parent_id=%q", threadID, threadParent)
	}
}

func TestPersistBackfillMessage_DedupesMessageSentLifecycleEvent(t *testing.T) {
	db, stmts := openTestDB(t)

	msg := &discordgo.Message{
		ID:        "8101",
		GuildID:   "guild-dedupe",
		ChannelID: "channel-dedupe",
		Content:   "hello once",
		Timestamp: time.Now().UTC(),
		Author: &discordgo.User{
			ID:            "user-dedupe",
			Username:      "dedupe-user",
			Discriminator: "0811",
		},
	}
	inserted, err := persistBackfillMessage(db, stmts, nil, "guild-dedupe", "channel-dedupe", "", msg)
	if err != nil {
		t.Fatalf("first persistBackfillMessage failed: %v", err)
	}
	if !inserted {
		t.Fatal("expected first insert to report inserted=true")
	}

	inserted, err = persistBackfillMessage(db, stmts, nil, "guild-dedupe", "channel-dedupe", "", msg)
	if err != nil {
		t.Fatalf("second persistBackfillMessage failed: %v", err)
	}
	if !inserted {
		t.Fatal("expected second insert to report inserted=true")
	}

	if got := mustCount(t, db, countLifecycleByMessageAndTypeQuery, "8101", string(eventMessageSent)); got != 1 {
		t.Fatalf("expected one message_sent lifecycle row after duplicate backfill, got %d", got)
	}
}

func testBackfillMessage(id string) *discordgo.Message {
	return &discordgo.Message{
		ID:        id,
		GuildID:   "guild-cold",
		ChannelID: "channel-cold",
		Content:   "hello",
		Timestamp: time.Now().UTC(),
		Author: &discordgo.User{
			ID:            "author-" + id,
			Username:      "author-" + id,
			Discriminator: "0001",
		},
	}
}

func TestBackfillTargetLabel(t *testing.T) {
	got := backfillTargetLabel(&discordgo.Channel{
		ID:   "100",
		Name: "announcements",
		Type: discordgo.ChannelTypeGuildText,
	})
	if got != "channel=#announcements (id=100)" {
		t.Fatalf("unexpected channel label: %q", got)
	}

	got = backfillTargetLabel(&discordgo.Channel{
		ID:       "200",
		Name:     "hello-thread",
		ParentID: "100",
		Type:     discordgo.ChannelTypeGuildPublicThread,
	})
	if got != "thread=hello-thread (id=200 parent_id=100)" {
		t.Fatalf("unexpected thread label: %q", got)
	}
}

func TestFetchArchivedThreadsForParent_MissingAccessPrivateLogsOnce(t *testing.T) {
	prevPublicArchivedThreadsFetcher := publicArchivedThreadsFetcher
	prevPrivateArchivedThreadsFetcher := privateArchivedThreadsFetcher
	prevJoinedPrivateArchivedThreadsFetcher := joinedPrivateArchivedThreadsFetcher

	oldWriter := log.Writer()
	oldFlags := log.Flags()
	var logBuf bytes.Buffer

	t.Cleanup(func() {
		publicArchivedThreadsFetcher = prevPublicArchivedThreadsFetcher
		privateArchivedThreadsFetcher = prevPrivateArchivedThreadsFetcher
		joinedPrivateArchivedThreadsFetcher = prevJoinedPrivateArchivedThreadsFetcher
		archivedThreadsPermissionNoticeOnce = sync.Once{}
		log.SetOutput(oldWriter)
		log.SetFlags(oldFlags)
	})

	archivedThreadsPermissionNoticeOnce = sync.Once{}
	log.SetOutput(&logBuf)
	log.SetFlags(0)

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

	publicArchivedThreadsFetcher = func(
		s *discordgo.Session,
		channelID string,
		before *time.Time,
		limit int,
	) (*discordgo.ThreadsList, error) {
		return &discordgo.ThreadsList{}, nil
	}
	privateArchivedThreadsFetcher = func(
		s *discordgo.Session,
		channelID string,
		before *time.Time,
		limit int,
	) (*discordgo.ThreadsList, error) {
		return nil, missingAccessErr
	}
	joinedPrivateArchivedThreadsFetcher = func(
		s *discordgo.Session,
		channelID string,
		before *time.Time,
		limit int,
	) (*discordgo.ThreadsList, error) {
		return nil, missingAccessErr
	}

	parent := &discordgo.Channel{
		ID:   "parent-1",
		Type: discordgo.ChannelTypeGuildText,
	}

	// Call twice; permission notice should still log once.
	if _, err := fetchArchivedThreadsForParent(&discordgo.Session{}, parent, nil); err != nil {
		t.Fatalf("fetchArchivedThreadsForParent #1 failed: %v", err)
	}
	if _, err := fetchArchivedThreadsForParent(&discordgo.Session{}, parent, nil); err != nil {
		t.Fatalf("fetchArchivedThreadsForParent #2 failed: %v", err)
	}

	logOutput := logBuf.String()
	if strings.Count(logOutput, "archived-thread backup is skipped where missing access") != 1 {
		t.Fatalf("expected exactly one archived-thread permission notice, logs=%q", logOutput)
	}
	if strings.Contains(logOutput, "HTTP 403 Forbidden") {
		t.Fatalf("unexpected raw HTTP error log output: %q", logOutput)
	}
}

func TestBackfillGuild_SkipsRecentlyCheckedArchivedParents(t *testing.T) {
	db, stmts := openTestDB(t)

	prevGuildChannelsFetcher := guildChannelsFetcher
	prevGuildActiveThreadsFetcher := guildActiveThreadsFetcher
	prevPublicArchivedThreadsFetcher := publicArchivedThreadsFetcher
	prevPrivateArchivedThreadsFetcher := privateArchivedThreadsFetcher
	prevJoinedPrivateArchivedThreadsFetcher := joinedPrivateArchivedThreadsFetcher
	prevChannelMessagesFetcher := channelMessagesFetcher
	t.Cleanup(func() {
		guildChannelsFetcher = prevGuildChannelsFetcher
		guildActiveThreadsFetcher = prevGuildActiveThreadsFetcher
		publicArchivedThreadsFetcher = prevPublicArchivedThreadsFetcher
		privateArchivedThreadsFetcher = prevPrivateArchivedThreadsFetcher
		joinedPrivateArchivedThreadsFetcher = prevJoinedPrivateArchivedThreadsFetcher
		channelMessagesFetcher = prevChannelMessagesFetcher
	})

	guildChannelsFetcher = func(s *discordgo.Session, guildID string) ([]*discordgo.Channel, error) {
		return []*discordgo.Channel{
			{ID: "100", GuildID: guildID, Type: discordgo.ChannelTypeGuildText, Name: "general"},
			{ID: "200", GuildID: guildID, Type: discordgo.ChannelTypeGuildText, Name: "dev"},
		}, nil
	}
	guildActiveThreadsFetcher = func(s *discordgo.Session, guildID string) ([]*discordgo.Channel, error) {
		return []*discordgo.Channel{}, nil
	}

	var archiveCalls []string
	publicArchivedThreadsFetcher = func(
		s *discordgo.Session,
		channelID string,
		before *time.Time,
		limit int,
	) (*discordgo.ThreadsList, error) {
		archiveCalls = append(archiveCalls, channelID)
		return &discordgo.ThreadsList{}, nil
	}
	privateArchivedThreadsFetcher = func(
		s *discordgo.Session,
		channelID string,
		before *time.Time,
		limit int,
	) (*discordgo.ThreadsList, error) {
		return &discordgo.ThreadsList{}, nil
	}
	joinedPrivateArchivedThreadsFetcher = func(
		s *discordgo.Session,
		channelID string,
		before *time.Time,
		limit int,
	) (*discordgo.ThreadsList, error) {
		return &discordgo.ThreadsList{}, nil
	}
	channelMessagesFetcher = func(
		s *discordgo.Session,
		channelID string,
		limit int,
		beforeID, afterID, aroundID string,
	) ([]*discordgo.Message, error) {
		return []*discordgo.Message{}, nil
	}

	// First run: TTL=1h, both parents should be checked.
	run1 := newBackfillRun(backfillConfig{archivedDiscoveryTTL: time.Hour}, newBackfillMetrics())
	if err := backfillGuild(&discordgo.Session{}, db, stmts, "guild-skip", run1); err != nil {
		t.Fatalf("first backfillGuild failed: %v", err)
	}
	if len(archiveCalls) == 0 {
		t.Fatal("expected archive discovery calls on first run, got none")
	}
	firstRunCalls := len(archiveCalls)

	// Second run: TTL=1h, both parents were just checked so should be skipped.
	archiveCalls = nil
	run2 := newBackfillRun(backfillConfig{archivedDiscoveryTTL: time.Hour}, newBackfillMetrics())
	if err := backfillGuild(&discordgo.Session{}, db, stmts, "guild-skip", run2); err != nil {
		t.Fatalf("second backfillGuild failed: %v", err)
	}
	if len(archiveCalls) != 0 {
		t.Fatalf("expected 0 archive discovery calls on second run (TTL not expired), got %d", len(archiveCalls))
	}

	// Third run: TTL=0 (disabled), should check all parents again.
	archiveCalls = nil
	run3 := newBackfillRun(backfillConfig{archivedDiscoveryTTL: 0}, newBackfillMetrics())
	if err := backfillGuild(&discordgo.Session{}, db, stmts, "guild-skip", run3); err != nil {
		t.Fatalf("third backfillGuild failed: %v", err)
	}
	if len(archiveCalls) != firstRunCalls {
		t.Fatalf("expected %d archive discovery calls with TTL=0, got %d", firstRunCalls, len(archiveCalls))
	}
}

func TestFetchArchivedThreadsForParent_MissingAccessPublicLogsOnce(t *testing.T) {
	prevPublicArchivedThreadsFetcher := publicArchivedThreadsFetcher
	prevPrivateArchivedThreadsFetcher := privateArchivedThreadsFetcher
	prevJoinedPrivateArchivedThreadsFetcher := joinedPrivateArchivedThreadsFetcher

	oldWriter := log.Writer()
	oldFlags := log.Flags()
	var logBuf bytes.Buffer

	t.Cleanup(func() {
		publicArchivedThreadsFetcher = prevPublicArchivedThreadsFetcher
		privateArchivedThreadsFetcher = prevPrivateArchivedThreadsFetcher
		joinedPrivateArchivedThreadsFetcher = prevJoinedPrivateArchivedThreadsFetcher
		archivedThreadsPermissionNoticeOnce = sync.Once{}
		log.SetOutput(oldWriter)
		log.SetFlags(oldFlags)
	})

	archivedThreadsPermissionNoticeOnce = sync.Once{}
	log.SetOutput(&logBuf)
	log.SetFlags(0)

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

	publicArchivedThreadsFetcher = func(
		s *discordgo.Session,
		channelID string,
		before *time.Time,
		limit int,
	) (*discordgo.ThreadsList, error) {
		return nil, missingAccessErr
	}
	privateArchivedThreadsFetcher = func(
		s *discordgo.Session,
		channelID string,
		before *time.Time,
		limit int,
	) (*discordgo.ThreadsList, error) {
		return &discordgo.ThreadsList{}, nil
	}
	joinedPrivateArchivedThreadsFetcher = func(
		s *discordgo.Session,
		channelID string,
		before *time.Time,
		limit int,
	) (*discordgo.ThreadsList, error) {
		return &discordgo.ThreadsList{}, nil
	}

	parent := &discordgo.Channel{
		ID:   "parent-1",
		Type: discordgo.ChannelTypeGuildText,
	}

	if _, err := fetchArchivedThreadsForParent(&discordgo.Session{}, parent, nil); err != nil {
		t.Fatalf("fetchArchivedThreadsForParent #1 failed: %v", err)
	}
	if _, err := fetchArchivedThreadsForParent(&discordgo.Session{}, parent, nil); err != nil {
		t.Fatalf("fetchArchivedThreadsForParent #2 failed: %v", err)
	}

	logOutput := logBuf.String()
	if strings.Count(logOutput, "archived-thread backup is skipped where missing access") != 1 {
		t.Fatalf("expected exactly one archived-thread permission notice, logs=%q", logOutput)
	}
	if strings.Contains(logOutput, "fetchArchivedThreads parent=") {
		t.Fatalf("unexpected scoped fetchArchivedThreads error log output: %q", logOutput)
	}
}
