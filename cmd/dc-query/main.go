// Package main provides the dc-query CLI for read-only assistant retrieval operations.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"example.org/dc-logger/internal/config"
	"example.org/dc-logger/internal/querysvc"
)

var usageText = fmt.Sprintf(`dc-query: read-only query helper for dc-logger SQLite data

Usage:
  dc-query <command> [flags]

Commands:
  recent-messages             Get recent messages across all channels in a guild.
  recent-messages-in-channel  Get recent messages in one channel.
  list-server-members         List current server member usernames.
  recent-messages-by-user     Get recent messages by one user.
  last-message-by-user        Get the latest message by one user.
  roles-of-user               Get current roles assigned to one user.
  users-with-role             Get users currently assigned to one role.
  recent-pings-by-user        Get recent user mentions sent by one user.
  recent-pings-targeting-user Get recent messages that mentioned one user.
  unanswered-pings-targeting-user Get recent mentions targeting one user with no later reply from that user in-channel.
  last-pinged-user-by-user    Get the latest user mentioned by one user.
  recent-events               Get recent lifecycle events (filterable by type/user/channel/time).
  topic-activity-summary      Summarize recent activity for a topic query grouped by author.
  server-activity-summary     Summarize message volume/authors/channels in a time window (use --by-day for daily rows).
  channel-activity-summary    Summarize message volume by channel in a time window.
  author-activity-summary     Summarize message volume by author in a time window.
  keyword-frequency           Return most frequent terms in a time window.
  search-messages             Search message/attachment content by substring.
  json-request                Read one JSON request object (stdin or --input file).

Common flags:
  --db <path>          SQLite database path (default: %s or %s)
  --guild-id <id>      Guild ID filter (optional; defaults to most common guild in DB)
  --guild-name <name>  Guild name filter (optional alternative to --guild-id)
  --limit <n>          Max returned rows, clamped to [1,100] (default: 10)
  --since <rfc3339>    Inclusive lower time bound (optional)
  --until <rfc3339>    Inclusive upper time bound (optional)
  --before-time <rfc3339> Exclusive upper cursor time bound for pagination (optional)
  --before-id <id>     Secondary cursor key within --before-time (optional)
  --resolve-names      Populate author/channel/guild names where supported (default: false)
  --pretty             Pretty-print JSON output (default: false)

JSON request mode:
  dc-query json-request [--input <path>|-]

  Request JSON fields:
    command    string (required; accepts both kebab-case and snake_case)
    db         string (optional)
    guild_id   string (optional)
    guild_name string (optional; alternative to guild_id)
    channel_id string (required for recent-messages-in-channel)
    channel_name string (optional; alternative to channel_id)
    author_id  string (required for recent-messages-by-user, last-message-by-user, roles-of-user, recent-pings-by-user, last-pinged-user-by-user)
    author_name string (optional; alternative to author_id)
    target_id  string (required for recent-pings-targeting-user, unanswered-pings-targeting-user)
    target_name string (optional; alternative to target_id)
    role_id    string (required for users-with-role)
    role_name  string (optional; alternative to role_id)
    event_type string (optional; filter for recent-events)
    query      string (required for search-messages)
    since      string (optional RFC3339 timestamp)
    until      string (optional RFC3339 timestamp)
    by_day     bool (optional; for server-activity-summary daily breakdown)
    before_time string (optional RFC3339 cursor timestamp)
    before_id  string (optional cursor id; requires before_time)
    resolve_names bool (optional; enrich message rows with names)
    min_length int (optional; for keyword-frequency, default 4)
    stopwords  []string (optional; stopwords for keyword-frequency)
    limit      int (optional)
    pretty     bool (optional)

Examples:
  dc-query recent-messages --limit 10
  dc-query recent-messages-in-channel --channel-name general --limit 5
  dc-query list-server-members --guild-name Lorem --limit 50
  dc-query last-message-by-user --author-name ipsum
  dc-query roles-of-user --author-name ipsum --limit 20
  dc-query users-with-role --role-name Consectetur --limit 20
  dc-query recent-pings-targeting-user --target-name dolor --limit 5
  dc-query unanswered-pings-targeting-user --target-name dolor --since 2026-01-01T00:00:00Z --limit 5
  dc-query last-pinged-user-by-user --author-name ipsum
  dc-query recent-events --event-type message_deleted --since 2026-01-01T00:00:00Z --limit 5
  dc-query topic-activity-summary --query "incident" --since 2026-01-01T00:00:00Z --limit 10
  dc-query server-activity-summary --since 2026-01-01T00:00:00Z --until 2026-01-31T23:59:59Z --by-day
  dc-query channel-activity-summary --since 2026-01-01T00:00:00Z --limit 10
  dc-query keyword-frequency --since 2026-01-01T00:00:00Z --min-length 5 --limit 20
  printf '{"command":"search_messages","query":"incident","limit":5}' | dc-query json-request
`, config.EnvDiscordLogDB, config.DefaultLogDBPath)

const (
	codeInternal            = "internal"
	codeInvalidCommand      = "invalid_command"
	codeInvalidArgument     = "invalid_argument"
	codeInvalidRequest      = "invalid_request"
	codeOpenDBFailed        = "open_db_failed"
	codeGuildResolution     = "guild_resolution_failed"
	codeQueryFailed         = "query_failed"
	codeEncodeResponse      = "encode_response_failed"
	codeReadRequestFailed   = "read_request_failed"
	codeDecodeRequestFailed = "decode_request_failed"
)

const (
	exitInternal = 1
	exitInvalid  = 2
	exitOpenDB   = 3
	exitQuery    = 4
)

const queryTimeout = time.Duration(config.DefaultQueryTimeoutSeconds) * time.Second

type response struct {
	Command string            `json:"command"`
	GuildID string            `json:"guild_id"`
	Count   int               `json:"count"`
	Items   []querysvc.Record `json:"items"`
}

type request struct {
	Command      string   `json:"command"`
	DBPath       string   `json:"db,omitempty"`
	GuildID      string   `json:"guild_id,omitempty"`
	GuildName    string   `json:"guild_name,omitempty"`
	ChannelID    string   `json:"channel_id,omitempty"`
	ChannelName  string   `json:"channel_name,omitempty"`
	AuthorID     string   `json:"author_id,omitempty"`
	AuthorName   string   `json:"author_name,omitempty"`
	TargetID     string   `json:"target_id,omitempty"`
	TargetName   string   `json:"target_name,omitempty"`
	RoleID       string   `json:"role_id,omitempty"`
	RoleName     string   `json:"role_name,omitempty"`
	EventType    string   `json:"event_type,omitempty"`
	Query        string   `json:"query,omitempty"`
	Since        string   `json:"since,omitempty"`
	Until        string   `json:"until,omitempty"`
	ByDay        bool     `json:"by_day,omitempty"`
	BeforeTime   string   `json:"before_time,omitempty"`
	BeforeID     string   `json:"before_id,omitempty"`
	ResolveNames bool     `json:"resolve_names,omitempty"`
	MinLength    int      `json:"min_length,omitempty"`
	Stopwords    []string `json:"stopwords,omitempty"`
	Limit        int      `json:"limit,omitempty"`
	Pretty       bool     `json:"pretty,omitempty"`
}

type errorEnvelope struct {
	Error errorPayload `json:"error"`
}

type errorPayload struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type codedError struct {
	code    string
	message string
	err     error
}

func (e *codedError) Error() string {
	if e == nil {
		return ""
	}
	if e.err == nil {
		return e.message
	}
	return fmt.Sprintf("%s: %v", e.message, e.err)
}

func withCode(code, message string, err error) error {
	return &codedError{code: code, message: message, err: err}
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

func run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		return writeError(stderr, withCode(codeInvalidCommand, "missing command", nil))
	}

	command := strings.TrimSpace(args[0])
	switch command {
	case "-h", "--help", "help":
		_, _ = fmt.Fprint(stdout, usageText)
		return 0
	case "recent-messages":
		return runRecentMessages(args[1:], stdout, stderr)
	case "recent-messages-in-channel":
		return runRecentMessagesInChannel(args[1:], stdout, stderr)
	case "list-server-members":
		return runListServerMembers(args[1:], stdout, stderr)
	case "recent-messages-by-user":
		return runRecentMessagesByUser(args[1:], stdout, stderr)
	case "last-message-by-user":
		return runLastMessageByUser(args[1:], stdout, stderr)
	case "roles-of-user":
		return runRolesOfUser(args[1:], stdout, stderr)
	case "users-with-role":
		return runUsersWithRole(args[1:], stdout, stderr)
	case "recent-pings-by-user":
		return runRecentPingsByUser(args[1:], stdout, stderr)
	case "recent-pings-targeting-user":
		return runRecentPingsTargetingUser(args[1:], stdout, stderr)
	case "unanswered-pings-targeting-user":
		return runUnansweredPingsTargetingUser(args[1:], stdout, stderr)
	case "last-pinged-user-by-user":
		return runLastPingedUserByUser(args[1:], stdout, stderr)
	case "recent-events":
		return runRecentEvents(args[1:], stdout, stderr)
	case "topic-activity-summary":
		return runTopicActivitySummary(args[1:], stdout, stderr)
	case "server-activity-summary":
		return runServerActivitySummary(args[1:], stdout, stderr)
	case "channel-activity-summary":
		return runChannelActivitySummary(args[1:], stdout, stderr)
	case "author-activity-summary":
		return runAuthorActivitySummary(args[1:], stdout, stderr)
	case "keyword-frequency":
		return runKeywordFrequency(args[1:], stdout, stderr)
	case "search-messages":
		return runSearchMessages(args[1:], stdout, stderr)
	case "json-request":
		return runJSONRequest(args[1:], stdin, stdout, stderr)
	default:
		return writeError(stderr, withCode(codeInvalidCommand, fmt.Sprintf("unknown command %q", command), nil))
	}
}

func runRecentMessages(args []string, stdout, stderr io.Writer) int {
	fs, common := newCommonFlagSet("recent-messages")
	if err := fs.Parse(args); err != nil {
		return writeError(stderr, withCode(codeInvalidArgument, "failed parsing flags", err))
	}

	req := request{
		Command:      "recent-messages",
		DBPath:       common.dbPath,
		GuildID:      common.guildID,
		GuildName:    common.guildName,
		Since:        common.since,
		Until:        common.until,
		BeforeTime:   common.beforeTime,
		BeforeID:     common.beforeID,
		ResolveNames: common.resolveNames,
		Limit:        common.limit,
		Pretty:       common.pretty,
	}
	return executeAndWrite(req, stdout, stderr)
}

func runRecentMessagesInChannel(args []string, stdout, stderr io.Writer) int {
	fs, common := newCommonFlagSet("recent-messages-in-channel")
	channelID := fs.String("channel-id", "", "channel id (required)")
	channelName := fs.String("channel-name", "", "channel name (required if --channel-id is omitted)")
	if err := fs.Parse(args); err != nil {
		return writeError(stderr, withCode(codeInvalidArgument, "failed parsing flags", err))
	}
	if err := requireAnyNonEmpty(
		fieldAlias{name: "channel-id", value: *channelID},
		fieldAlias{name: "channel-name", value: *channelName},
	); err != nil {
		return writeError(stderr, err)
	}

	req := request{
		Command:      "recent-messages-in-channel",
		DBPath:       common.dbPath,
		GuildID:      common.guildID,
		GuildName:    common.guildName,
		ChannelID:    *channelID,
		ChannelName:  *channelName,
		Since:        common.since,
		Until:        common.until,
		BeforeTime:   common.beforeTime,
		BeforeID:     common.beforeID,
		ResolveNames: common.resolveNames,
		Limit:        common.limit,
		Pretty:       common.pretty,
	}
	return executeAndWrite(req, stdout, stderr)
}

func runListServerMembers(args []string, stdout, stderr io.Writer) int {
	fs, common := newCommonFlagSet("list-server-members")
	if err := fs.Parse(args); err != nil {
		return writeError(stderr, withCode(codeInvalidArgument, "failed parsing flags", err))
	}

	req := request{
		Command:   "list-server-members",
		DBPath:    common.dbPath,
		GuildID:   common.guildID,
		GuildName: common.guildName,
		Limit:     common.limit,
		Pretty:    common.pretty,
	}
	return executeAndWrite(req, stdout, stderr)
}

func runRecentMessagesByUser(args []string, stdout, stderr io.Writer) int {
	fs, common := newCommonFlagSet("recent-messages-by-user")
	authorID := fs.String("author-id", "", "author/user id (required)")
	authorName := fs.String("author-name", "", "author/user name (required if --author-id is omitted)")
	if err := fs.Parse(args); err != nil {
		return writeError(stderr, withCode(codeInvalidArgument, "failed parsing flags", err))
	}
	if err := requireAnyNonEmpty(
		fieldAlias{name: "author-id", value: *authorID},
		fieldAlias{name: "author-name", value: *authorName},
	); err != nil {
		return writeError(stderr, err)
	}

	req := request{
		Command:      "recent-messages-by-user",
		DBPath:       common.dbPath,
		GuildID:      common.guildID,
		GuildName:    common.guildName,
		AuthorID:     *authorID,
		AuthorName:   *authorName,
		Since:        common.since,
		Until:        common.until,
		BeforeTime:   common.beforeTime,
		BeforeID:     common.beforeID,
		ResolveNames: common.resolveNames,
		Limit:        common.limit,
		Pretty:       common.pretty,
	}
	return executeAndWrite(req, stdout, stderr)
}

func runLastMessageByUser(args []string, stdout, stderr io.Writer) int {
	fs, common := newCommonFlagSet("last-message-by-user")
	authorID := fs.String("author-id", "", "author/user id (required)")
	authorName := fs.String("author-name", "", "author/user name (required if --author-id is omitted)")
	if err := fs.Parse(args); err != nil {
		return writeError(stderr, withCode(codeInvalidArgument, "failed parsing flags", err))
	}
	if err := requireAnyNonEmpty(
		fieldAlias{name: "author-id", value: *authorID},
		fieldAlias{name: "author-name", value: *authorName},
	); err != nil {
		return writeError(stderr, err)
	}

	req := request{
		Command:      "last-message-by-user",
		DBPath:       common.dbPath,
		GuildID:      common.guildID,
		GuildName:    common.guildName,
		AuthorID:     *authorID,
		AuthorName:   *authorName,
		ResolveNames: common.resolveNames,
		Pretty:       common.pretty,
	}
	return executeAndWrite(req, stdout, stderr)
}

func runRolesOfUser(args []string, stdout, stderr io.Writer) int {
	fs, common := newCommonFlagSet("roles-of-user")
	authorID := fs.String("author-id", "", "author/user id (required)")
	authorName := fs.String("author-name", "", "author/user name (required if --author-id is omitted)")
	if err := fs.Parse(args); err != nil {
		return writeError(stderr, withCode(codeInvalidArgument, "failed parsing flags", err))
	}
	if err := requireAnyNonEmpty(
		fieldAlias{name: "author-id", value: *authorID},
		fieldAlias{name: "author-name", value: *authorName},
	); err != nil {
		return writeError(stderr, err)
	}

	req := request{
		Command:    "roles-of-user",
		DBPath:     common.dbPath,
		GuildID:    common.guildID,
		GuildName:  common.guildName,
		AuthorID:   *authorID,
		AuthorName: *authorName,
		Limit:      common.limit,
		Pretty:     common.pretty,
	}
	return executeAndWrite(req, stdout, stderr)
}

func runUsersWithRole(args []string, stdout, stderr io.Writer) int {
	fs, common := newCommonFlagSet("users-with-role")
	roleID := fs.String("role-id", "", "role id (required)")
	roleName := fs.String("role-name", "", "role name (required if --role-id is omitted)")
	if err := fs.Parse(args); err != nil {
		return writeError(stderr, withCode(codeInvalidArgument, "failed parsing flags", err))
	}
	if err := requireAnyNonEmpty(
		fieldAlias{name: "role-id", value: *roleID},
		fieldAlias{name: "role-name", value: *roleName},
	); err != nil {
		return writeError(stderr, err)
	}

	req := request{
		Command:   "users-with-role",
		DBPath:    common.dbPath,
		GuildID:   common.guildID,
		GuildName: common.guildName,
		RoleID:    *roleID,
		RoleName:  *roleName,
		Limit:     common.limit,
		Pretty:    common.pretty,
	}
	return executeAndWrite(req, stdout, stderr)
}

func runRecentPingsByUser(args []string, stdout, stderr io.Writer) int {
	fs, common := newCommonFlagSet("recent-pings-by-user")
	authorID := fs.String("author-id", "", "author/user id (required)")
	authorName := fs.String("author-name", "", "author/user name (required if --author-id is omitted)")
	if err := fs.Parse(args); err != nil {
		return writeError(stderr, withCode(codeInvalidArgument, "failed parsing flags", err))
	}
	if err := requireAnyNonEmpty(
		fieldAlias{name: "author-id", value: *authorID},
		fieldAlias{name: "author-name", value: *authorName},
	); err != nil {
		return writeError(stderr, err)
	}

	req := request{
		Command:    "recent-pings-by-user",
		DBPath:     common.dbPath,
		GuildID:    common.guildID,
		GuildName:  common.guildName,
		AuthorID:   *authorID,
		AuthorName: *authorName,
		Limit:      common.limit,
		Pretty:     common.pretty,
	}
	return executeAndWrite(req, stdout, stderr)
}

func runRecentPingsTargetingUser(args []string, stdout, stderr io.Writer) int {
	fs, common := newCommonFlagSet("recent-pings-targeting-user")
	targetID := fs.String("target-id", "", "target/user id (required)")
	targetName := fs.String("target-name", "", "target/user name (required if --target-id is omitted)")
	if err := fs.Parse(args); err != nil {
		return writeError(stderr, withCode(codeInvalidArgument, "failed parsing flags", err))
	}
	if err := requireAnyNonEmpty(
		fieldAlias{name: "target-id", value: *targetID},
		fieldAlias{name: "target-name", value: *targetName},
	); err != nil {
		return writeError(stderr, err)
	}

	req := request{
		Command:    "recent-pings-targeting-user",
		DBPath:     common.dbPath,
		GuildID:    common.guildID,
		GuildName:  common.guildName,
		TargetID:   *targetID,
		TargetName: *targetName,
		Limit:      common.limit,
		Pretty:     common.pretty,
	}
	return executeAndWrite(req, stdout, stderr)
}

func runUnansweredPingsTargetingUser(args []string, stdout, stderr io.Writer) int {
	fs, common := newCommonFlagSet("unanswered-pings-targeting-user")
	targetID := fs.String("target-id", "", "target/user id (required)")
	targetName := fs.String("target-name", "", "target/user name (required if --target-id is omitted)")
	if err := fs.Parse(args); err != nil {
		return writeError(stderr, withCode(codeInvalidArgument, "failed parsing flags", err))
	}
	if err := requireAnyNonEmpty(
		fieldAlias{name: "target-id", value: *targetID},
		fieldAlias{name: "target-name", value: *targetName},
	); err != nil {
		return writeError(stderr, err)
	}

	req := request{
		Command:    "unanswered-pings-targeting-user",
		DBPath:     common.dbPath,
		GuildID:    common.guildID,
		GuildName:  common.guildName,
		TargetID:   *targetID,
		TargetName: *targetName,
		Since:      common.since,
		Until:      common.until,
		BeforeTime: common.beforeTime,
		BeforeID:   common.beforeID,
		Limit:      common.limit,
		Pretty:     common.pretty,
	}
	return executeAndWrite(req, stdout, stderr)
}

func runLastPingedUserByUser(args []string, stdout, stderr io.Writer) int {
	fs, common := newCommonFlagSet("last-pinged-user-by-user")
	authorID := fs.String("author-id", "", "author/user id (required)")
	authorName := fs.String("author-name", "", "author/user name (required if --author-id is omitted)")
	if err := fs.Parse(args); err != nil {
		return writeError(stderr, withCode(codeInvalidArgument, "failed parsing flags", err))
	}
	if err := requireAnyNonEmpty(
		fieldAlias{name: "author-id", value: *authorID},
		fieldAlias{name: "author-name", value: *authorName},
	); err != nil {
		return writeError(stderr, err)
	}

	req := request{
		Command:    "last-pinged-user-by-user",
		DBPath:     common.dbPath,
		GuildID:    common.guildID,
		GuildName:  common.guildName,
		AuthorID:   *authorID,
		AuthorName: *authorName,
		Pretty:     common.pretty,
	}
	return executeAndWrite(req, stdout, stderr)
}

func runRecentEvents(args []string, stdout, stderr io.Writer) int {
	fs, common := newCommonFlagSet("recent-events")
	eventType := fs.String("event-type", "", "lifecycle event type filter (optional)")
	channelID := fs.String("channel-id", "", "channel id filter (optional)")
	channelName := fs.String("channel-name", "", "channel name filter (optional; resolves within guild)")
	authorID := fs.String("author-id", "", "actor/user id filter (optional)")
	authorName := fs.String("author-name", "", "actor/user name filter (optional; resolves within guild)")
	if err := fs.Parse(args); err != nil {
		return writeError(stderr, withCode(codeInvalidArgument, "failed parsing flags", err))
	}

	req := request{
		Command:     "recent-events",
		DBPath:      common.dbPath,
		GuildID:     common.guildID,
		GuildName:   common.guildName,
		EventType:   *eventType,
		ChannelID:   *channelID,
		ChannelName: *channelName,
		AuthorID:    *authorID,
		AuthorName:  *authorName,
		Since:       common.since,
		Until:       common.until,
		BeforeTime:  common.beforeTime,
		BeforeID:    common.beforeID,
		Limit:       common.limit,
		Pretty:      common.pretty,
	}
	return executeAndWrite(req, stdout, stderr)
}

func runSearchMessages(args []string, stdout, stderr io.Writer) int {
	fs, common := newCommonFlagSet("search-messages")
	searchQuery := fs.String("query", "", "search query substring (required)")
	if err := fs.Parse(args); err != nil {
		return writeError(stderr, withCode(codeInvalidArgument, "failed parsing flags", err))
	}
	if err := requireNonEmpty("query", *searchQuery); err != nil {
		return writeError(stderr, err)
	}

	req := request{
		Command:      "search-messages",
		DBPath:       common.dbPath,
		GuildID:      common.guildID,
		GuildName:    common.guildName,
		Query:        *searchQuery,
		Since:        common.since,
		Until:        common.until,
		BeforeTime:   common.beforeTime,
		BeforeID:     common.beforeID,
		ResolveNames: common.resolveNames,
		Limit:        common.limit,
		Pretty:       common.pretty,
	}
	return executeAndWrite(req, stdout, stderr)
}

func runTopicActivitySummary(args []string, stdout, stderr io.Writer) int {
	fs, common := newCommonFlagSet("topic-activity-summary")
	searchQuery := fs.String("query", "", "topic query substring (required)")
	if err := fs.Parse(args); err != nil {
		return writeError(stderr, withCode(codeInvalidArgument, "failed parsing flags", err))
	}
	if err := requireNonEmpty("query", *searchQuery); err != nil {
		return writeError(stderr, err)
	}

	req := request{
		Command:      "topic-activity-summary",
		DBPath:       common.dbPath,
		GuildID:      common.guildID,
		GuildName:    common.guildName,
		Query:        *searchQuery,
		Since:        common.since,
		Until:        common.until,
		ResolveNames: common.resolveNames,
		Limit:        common.limit,
		Pretty:       common.pretty,
	}
	return executeAndWrite(req, stdout, stderr)
}

func runServerActivitySummary(args []string, stdout, stderr io.Writer) int {
	fs, common := newCommonFlagSet("server-activity-summary")
	byDay := fs.Bool("by-day", false, "return one row per UTC day in the selected window")
	if err := fs.Parse(args); err != nil {
		return writeError(stderr, withCode(codeInvalidArgument, "failed parsing flags", err))
	}

	req := request{
		Command:   "server-activity-summary",
		DBPath:    common.dbPath,
		GuildID:   common.guildID,
		GuildName: common.guildName,
		Since:     common.since,
		Until:     common.until,
		ByDay:     *byDay,
		Pretty:    common.pretty,
	}
	return executeAndWrite(req, stdout, stderr)
}

func runChannelActivitySummary(args []string, stdout, stderr io.Writer) int {
	fs, common := newCommonFlagSet("channel-activity-summary")
	if err := fs.Parse(args); err != nil {
		return writeError(stderr, withCode(codeInvalidArgument, "failed parsing flags", err))
	}

	req := request{
		Command:   "channel-activity-summary",
		DBPath:    common.dbPath,
		GuildID:   common.guildID,
		GuildName: common.guildName,
		Since:     common.since,
		Until:     common.until,
		Limit:     common.limit,
		Pretty:    common.pretty,
	}
	return executeAndWrite(req, stdout, stderr)
}

func runAuthorActivitySummary(args []string, stdout, stderr io.Writer) int {
	fs, common := newCommonFlagSet("author-activity-summary")
	if err := fs.Parse(args); err != nil {
		return writeError(stderr, withCode(codeInvalidArgument, "failed parsing flags", err))
	}

	req := request{
		Command:   "author-activity-summary",
		DBPath:    common.dbPath,
		GuildID:   common.guildID,
		GuildName: common.guildName,
		Since:     common.since,
		Until:     common.until,
		Limit:     common.limit,
		Pretty:    common.pretty,
	}
	return executeAndWrite(req, stdout, stderr)
}

func runKeywordFrequency(args []string, stdout, stderr io.Writer) int {
	fs, common := newCommonFlagSet("keyword-frequency")
	minLength := fs.Int("min-length", 4, "minimum token length")
	stopwords := fs.String("stopwords", "", "comma-separated stopwords")
	if err := fs.Parse(args); err != nil {
		return writeError(stderr, withCode(codeInvalidArgument, "failed parsing flags", err))
	}

	req := request{
		Command:   "keyword-frequency",
		DBPath:    common.dbPath,
		GuildID:   common.guildID,
		GuildName: common.guildName,
		Since:     common.since,
		Until:     common.until,
		MinLength: *minLength,
		Stopwords: parseCSVList(*stopwords),
		Limit:     common.limit,
		Pretty:    common.pretty,
	}
	return executeAndWrite(req, stdout, stderr)
}

func runJSONRequest(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("json-request", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	inputPath := fs.String("input", "-", "JSON request file path; use '-' for stdin")
	pretty := fs.Bool("pretty", false, "pretty-print JSON output")
	if err := fs.Parse(args); err != nil {
		return writeError(stderr, withCode(codeInvalidArgument, "failed parsing flags", err))
	}

	req, err := decodeJSONRequest(*inputPath, stdin)
	if err != nil {
		return writeError(stderr, err)
	}
	if *pretty {
		req.Pretty = true
	}
	return executeAndWrite(req, stdout, stderr)
}

func executeAndWrite(req request, stdout, stderr io.Writer) int {
	out, err := executeRequest(req)
	if err != nil {
		return writeError(stderr, err)
	}
	if err := printResponse(req.Pretty, out, stdout); err != nil {
		return writeError(stderr, withCode(codeEncodeResponse, "failed encoding response", err))
	}
	return 0
}

func executeRequest(req request) (response, error) {
	command, err := normalizeCommand(req.Command)
	if err != nil {
		return response{}, err
	}

	dbPath := strings.TrimSpace(req.DBPath)
	if dbPath == "" {
		dbPath = defaultDBPath()
	}
	svc, err := querysvc.Open(dbPath)
	if err != nil {
		return response{}, withCode(codeOpenDBFailed, fmt.Sprintf("failed opening database %q", dbPath), err)
	}
	defer func() { _ = svc.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout)
	defer cancel()

	guildID, err := resolveGuildID(ctx, svc, req)
	if err != nil {
		return response{}, err
	}

	switch command {
	case "recent-messages":
		items, err := svc.RecentMessagesWindow(ctx, guildID, req.Since, req.Until, req.BeforeTime, req.BeforeID, req.Limit)
		if err != nil {
			return response{}, withCode(codeQueryFailed, "recent-messages query failed", err)
		}
		if req.ResolveNames {
			if err := svc.EnrichNames(ctx, guildID, items); err != nil {
				return response{}, withCode(codeQueryFailed, "recent-messages name enrichment failed", err)
			}
		}
		return response{Command: command, GuildID: guildID, Count: len(items), Items: items}, nil
	case "recent-messages-in-channel":
		channelID, err := resolveChannelID(ctx, svc, guildID, req)
		if err != nil {
			return response{}, err
		}
		items, err := svc.RecentMessagesInChannelWindow(ctx, guildID, channelID, req.Since, req.Until, req.BeforeTime, req.BeforeID, req.Limit)
		if err != nil {
			return response{}, withCode(codeQueryFailed, "recent-messages-in-channel query failed", err)
		}
		if req.ResolveNames {
			if err := svc.EnrichNames(ctx, guildID, items); err != nil {
				return response{}, withCode(codeQueryFailed, "recent-messages-in-channel name enrichment failed", err)
			}
		}
		return response{Command: command, GuildID: guildID, Count: len(items), Items: items}, nil
	case "list-server-members":
		items, err := svc.ListGuildMembers(ctx, guildID, req.Limit)
		if err != nil {
			return response{}, withCode(codeQueryFailed, "list-server-members query failed", err)
		}
		return response{Command: command, GuildID: guildID, Count: len(items), Items: items}, nil
	case "recent-messages-by-user":
		authorID, err := resolveAuthorID(ctx, svc, guildID, req)
		if err != nil {
			return response{}, err
		}
		items, err := svc.RecentMessagesByUserWindow(ctx, guildID, authorID, req.Since, req.Until, req.BeforeTime, req.BeforeID, req.Limit)
		if err != nil {
			return response{}, withCode(codeQueryFailed, "recent-messages-by-user query failed", err)
		}
		if req.ResolveNames {
			if err := svc.EnrichNames(ctx, guildID, items); err != nil {
				return response{}, withCode(codeQueryFailed, "recent-messages-by-user name enrichment failed", err)
			}
		}
		return response{Command: command, GuildID: guildID, Count: len(items), Items: items}, nil
	case "last-message-by-user":
		authorID, err := resolveAuthorID(ctx, svc, guildID, req)
		if err != nil {
			return response{}, err
		}
		item, err := svc.LastMessageByUser(ctx, guildID, authorID)
		if err != nil {
			return response{}, withCode(codeQueryFailed, "last-message-by-user query failed", err)
		}
		items := make([]querysvc.Record, 0, 1)
		if item != nil {
			items = append(items, *item)
		}
		if req.ResolveNames && len(items) > 0 {
			if err := svc.EnrichNames(ctx, guildID, items); err != nil {
				return response{}, withCode(codeQueryFailed, "last-message-by-user name enrichment failed", err)
			}
		}
		return response{Command: command, GuildID: guildID, Count: len(items), Items: items}, nil
	case "roles-of-user":
		authorID, err := resolveAuthorID(ctx, svc, guildID, req)
		if err != nil {
			return response{}, err
		}
		items, err := svc.RolesOfUser(ctx, guildID, authorID, req.Limit)
		if err != nil {
			return response{}, withCode(codeQueryFailed, "roles-of-user query failed", err)
		}
		return response{Command: command, GuildID: guildID, Count: len(items), Items: items}, nil
	case "users-with-role":
		roleID, err := resolveRoleID(ctx, svc, guildID, req)
		if err != nil {
			return response{}, err
		}
		items, err := svc.UsersWithRole(ctx, guildID, roleID, req.Limit)
		if err != nil {
			return response{}, withCode(codeQueryFailed, "users-with-role query failed", err)
		}
		return response{Command: command, GuildID: guildID, Count: len(items), Items: items}, nil
	case "recent-pings-by-user":
		authorID, err := resolveAuthorID(ctx, svc, guildID, req)
		if err != nil {
			return response{}, err
		}
		items, err := svc.RecentPingsByUser(ctx, guildID, authorID, req.Limit)
		if err != nil {
			return response{}, withCode(codeQueryFailed, "recent-pings-by-user query failed", err)
		}
		return response{Command: command, GuildID: guildID, Count: len(items), Items: items}, nil
	case "recent-pings-targeting-user":
		targetID, err := resolveTargetID(ctx, svc, guildID, req)
		if err != nil {
			return response{}, err
		}
		items, err := svc.RecentPingsTargetingUser(ctx, guildID, targetID, req.Limit)
		if err != nil {
			return response{}, withCode(codeQueryFailed, "recent-pings-targeting-user query failed", err)
		}
		return response{Command: command, GuildID: guildID, Count: len(items), Items: items}, nil
	case "unanswered-pings-targeting-user":
		targetID, err := resolveTargetID(ctx, svc, guildID, req)
		if err != nil {
			return response{}, err
		}
		items, err := svc.UnansweredPingsTargetingUser(
			ctx,
			guildID,
			targetID,
			req.Since,
			req.Until,
			req.BeforeTime,
			req.BeforeID,
			req.Limit,
		)
		if err != nil {
			return response{}, withCode(codeQueryFailed, "unanswered-pings-targeting-user query failed", err)
		}
		return response{Command: command, GuildID: guildID, Count: len(items), Items: items}, nil
	case "last-pinged-user-by-user":
		authorID, err := resolveAuthorID(ctx, svc, guildID, req)
		if err != nil {
			return response{}, err
		}
		item, err := svc.LastPingedUserByUser(ctx, guildID, authorID)
		if err != nil {
			return response{}, withCode(codeQueryFailed, "last-pinged-user-by-user query failed", err)
		}
		items := make([]querysvc.Record, 0, 1)
		if item != nil {
			items = append(items, *item)
		}
		return response{Command: command, GuildID: guildID, Count: len(items), Items: items}, nil
	case "recent-events":
		actorID := strings.TrimSpace(req.AuthorID)
		if actorID == "" && strings.TrimSpace(req.AuthorName) != "" {
			resolved, err := svc.ResolveAuthorIDByName(ctx, guildID, req.AuthorName)
			if err != nil {
				return response{}, withCode(codeGuildResolution, "failed resolving actor id from author name", err)
			}
			if resolved == "" {
				return response{}, withCode(codeInvalidArgument, fmt.Sprintf("could not resolve author name %q in guild %q", req.AuthorName, guildID), nil)
			}
			actorID = resolved
		}

		channelID := strings.TrimSpace(req.ChannelID)
		if channelID == "" && strings.TrimSpace(req.ChannelName) != "" {
			resolved, err := svc.ResolveChannelIDByName(ctx, guildID, req.ChannelName)
			if err != nil {
				return response{}, withCode(codeGuildResolution, "failed resolving channel id from channel name", err)
			}
			if resolved == "" {
				return response{}, withCode(codeInvalidArgument, fmt.Sprintf("could not resolve channel name %q in guild %q", req.ChannelName, guildID), nil)
			}
			channelID = resolved
		}

		items, err := svc.RecentLifecycleEventsWindow(
			ctx,
			guildID,
			req.EventType,
			actorID,
			channelID,
			req.Since,
			req.Until,
			req.BeforeTime,
			req.BeforeID,
			req.Limit,
		)
		if err != nil {
			return response{}, withCode(codeQueryFailed, "recent-events query failed", err)
		}
		return response{Command: command, GuildID: guildID, Count: len(items), Items: items}, nil
	case "topic-activity-summary":
		searchQuery := strings.TrimSpace(req.Query)
		if searchQuery == "" {
			return response{}, withCode(codeInvalidArgument, "missing required field query", nil)
		}
		items, err := svc.TopicActivitySummary(
			ctx,
			guildID,
			searchQuery,
			req.Since,
			req.Until,
			req.Limit,
		)
		if err != nil {
			return response{}, withCode(codeQueryFailed, "topic-activity-summary query failed", err)
		}
		if req.ResolveNames {
			if err := svc.EnrichNames(ctx, guildID, items); err != nil {
				return response{}, withCode(codeQueryFailed, "topic-activity-summary name enrichment failed", err)
			}
		}
		return response{Command: command, GuildID: guildID, Count: len(items), Items: items}, nil
	case "server-activity-summary":
		var items []querysvc.Record
		if req.ByDay {
			items, err = svc.ServerActivitySummaryByDay(ctx, guildID, req.Since, req.Until)
		} else {
			items, err = svc.ServerActivitySummary(ctx, guildID, req.Since, req.Until)
		}
		if err != nil {
			return response{}, withCode(codeQueryFailed, "server-activity-summary query failed", err)
		}
		return response{Command: command, GuildID: guildID, Count: len(items), Items: items}, nil
	case "channel-activity-summary":
		items, err := svc.ChannelActivitySummary(ctx, guildID, req.Since, req.Until, req.Limit)
		if err != nil {
			return response{}, withCode(codeQueryFailed, "channel-activity-summary query failed", err)
		}
		return response{Command: command, GuildID: guildID, Count: len(items), Items: items}, nil
	case "author-activity-summary":
		items, err := svc.AuthorActivitySummary(ctx, guildID, req.Since, req.Until, req.Limit)
		if err != nil {
			return response{}, withCode(codeQueryFailed, "author-activity-summary query failed", err)
		}
		return response{Command: command, GuildID: guildID, Count: len(items), Items: items}, nil
	case "keyword-frequency":
		items, err := svc.KeywordFrequency(ctx, guildID, req.Since, req.Until, req.MinLength, req.Stopwords, req.Limit)
		if err != nil {
			return response{}, withCode(codeQueryFailed, "keyword-frequency query failed", err)
		}
		return response{Command: command, GuildID: guildID, Count: len(items), Items: items}, nil
	case "search-messages":
		searchQuery := strings.TrimSpace(req.Query)
		if searchQuery == "" {
			return response{}, withCode(codeInvalidArgument, "missing required field query", nil)
		}
		items, err := svc.SearchMessagesWindow(ctx, guildID, searchQuery, req.Since, req.Until, req.BeforeTime, req.BeforeID, req.Limit)
		if err != nil {
			return response{}, withCode(codeQueryFailed, "search-messages query failed", err)
		}
		if req.ResolveNames {
			if err := svc.EnrichNames(ctx, guildID, items); err != nil {
				return response{}, withCode(codeQueryFailed, "search-messages name enrichment failed", err)
			}
		}
		return response{Command: command, GuildID: guildID, Count: len(items), Items: items}, nil
	default:
		return response{}, withCode(codeInvalidCommand, fmt.Sprintf("unsupported command %q", command), nil)
	}
}

func normalizeCommand(command string) (string, error) {
	switch strings.TrimSpace(strings.ToLower(command)) {
	case "recent-messages", "recent_messages":
		return "recent-messages", nil
	case "recent-messages-in-channel", "recent_messages_in_channel":
		return "recent-messages-in-channel", nil
	case "list-server-members", "list_server_members":
		return "list-server-members", nil
	case "recent-messages-by-user", "recent_messages_by_user":
		return "recent-messages-by-user", nil
	case "last-message-by-user", "last_message_by_user":
		return "last-message-by-user", nil
	case "roles-of-user", "roles_of_user":
		return "roles-of-user", nil
	case "users-with-role", "users_with_role":
		return "users-with-role", nil
	case "recent-pings-by-user", "recent_pings_by_user":
		return "recent-pings-by-user", nil
	case "recent-pings-targeting-user", "recent_pings_targeting_user":
		return "recent-pings-targeting-user", nil
	case "unanswered-pings-targeting-user", "unanswered_pings_targeting_user":
		return "unanswered-pings-targeting-user", nil
	case "last-pinged-user-by-user", "last_pinged_user_by_user":
		return "last-pinged-user-by-user", nil
	case "recent-events", "recent_events":
		return "recent-events", nil
	case "topic-activity-summary", "topic_activity_summary":
		return "topic-activity-summary", nil
	case "server-activity-summary", "server_activity_summary":
		return "server-activity-summary", nil
	case "channel-activity-summary", "channel_activity_summary":
		return "channel-activity-summary", nil
	case "author-activity-summary", "author_activity_summary":
		return "author-activity-summary", nil
	case "keyword-frequency", "keyword_frequency":
		return "keyword-frequency", nil
	case "search-messages", "search_messages":
		return "search-messages", nil
	default:
		return "", withCode(codeInvalidCommand, fmt.Sprintf("unsupported command %q", command), nil)
	}
}

func resolveGuildID(ctx context.Context, svc *querysvc.Service, req request) (string, error) {
	guildID := strings.TrimSpace(req.GuildID)
	if guildID != "" {
		return guildID, nil
	}

	guildName := strings.TrimSpace(req.GuildName)
	if guildName != "" {
		resolved, err := svc.ResolveGuildIDByName(ctx, guildName)
		if err != nil {
			return "", withCode(codeGuildResolution, "failed resolving guild id from guild name", err)
		}
		if resolved == "" {
			return "", withCode(codeInvalidArgument, fmt.Sprintf("could not resolve guild name %q", guildName), nil)
		}
		return resolved, nil
	}

	resolved, err := svc.MostCommonGuildID(ctx)
	if err != nil {
		return "", withCode(codeGuildResolution, "failed resolving guild id", err)
	}
	if resolved == "" {
		return "", withCode(codeInvalidArgument, "could not infer guild id from database; pass --guild-id or --guild-name explicitly", nil)
	}
	return resolved, nil
}

func resolveChannelID(ctx context.Context, svc *querysvc.Service, guildID string, req request) (string, error) {
	channelID := strings.TrimSpace(req.ChannelID)
	if channelID != "" {
		return channelID, nil
	}

	channelName := strings.TrimSpace(req.ChannelName)
	if channelName == "" {
		return "", withCode(codeInvalidArgument, "missing required field channel-id or channel-name", nil)
	}
	resolved, err := svc.ResolveChannelIDByName(ctx, guildID, channelName)
	if err != nil {
		return "", withCode(codeGuildResolution, "failed resolving channel id from channel name", err)
	}
	if resolved == "" {
		return "", withCode(codeInvalidArgument, fmt.Sprintf("could not resolve channel name %q in guild %q", channelName, guildID), nil)
	}
	return resolved, nil
}

func resolveAuthorID(ctx context.Context, svc *querysvc.Service, guildID string, req request) (string, error) {
	authorID := strings.TrimSpace(req.AuthorID)
	if authorID != "" {
		return authorID, nil
	}

	authorName := strings.TrimSpace(req.AuthorName)
	if authorName == "" {
		return "", withCode(codeInvalidArgument, "missing required field author-id or author-name", nil)
	}
	resolved, err := svc.ResolveAuthorIDByName(ctx, guildID, authorName)
	if err != nil {
		return "", withCode(codeGuildResolution, "failed resolving author id from author name", err)
	}
	if resolved == "" {
		return "", withCode(codeInvalidArgument, fmt.Sprintf("could not resolve author name %q in guild %q", authorName, guildID), nil)
	}
	return resolved, nil
}

func resolveTargetID(ctx context.Context, svc *querysvc.Service, guildID string, req request) (string, error) {
	targetID := strings.TrimSpace(req.TargetID)
	if targetID != "" {
		return targetID, nil
	}

	targetName := strings.TrimSpace(req.TargetName)
	if targetName == "" {
		return "", withCode(codeInvalidArgument, "missing required field target-id or target-name", nil)
	}
	resolved, err := svc.ResolveAuthorIDByName(ctx, guildID, targetName)
	if err != nil {
		return "", withCode(codeGuildResolution, "failed resolving target id from target name", err)
	}
	if resolved == "" {
		return "", withCode(codeInvalidArgument, fmt.Sprintf("could not resolve target name %q in guild %q", targetName, guildID), nil)
	}
	return resolved, nil
}

func resolveRoleID(ctx context.Context, svc *querysvc.Service, guildID string, req request) (string, error) {
	roleID := strings.TrimSpace(req.RoleID)
	if roleID != "" {
		return roleID, nil
	}

	roleName := strings.TrimSpace(req.RoleName)
	if roleName == "" {
		return "", withCode(codeInvalidArgument, "missing required field role-id or role-name", nil)
	}
	resolved, err := svc.ResolveRoleIDByName(ctx, guildID, roleName)
	if err != nil {
		return "", withCode(codeGuildResolution, "failed resolving role id from role name", err)
	}
	if resolved == "" {
		return "", withCode(codeInvalidArgument, fmt.Sprintf("could not resolve role name %q in guild %q", roleName, guildID), nil)
	}
	return resolved, nil
}

func decodeJSONRequest(inputPath string, stdin io.Reader) (request, error) {
	var reader io.Reader = stdin
	if strings.TrimSpace(inputPath) != "" && inputPath != "-" {
		f, err := os.Open(inputPath)
		if err != nil {
			return request{}, withCode(codeReadRequestFailed, fmt.Sprintf("failed opening request file %q", inputPath), err)
		}
		defer func() { _ = f.Close() }()
		reader = f
	}

	var req request
	dec := json.NewDecoder(reader)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		return request{}, withCode(codeDecodeRequestFailed, "failed decoding request JSON", err)
	}
	var extra json.RawMessage
	if err := dec.Decode(&extra); err != io.EOF {
		return request{}, withCode(codeDecodeRequestFailed, "request JSON must contain exactly one object", nil)
	}
	if strings.TrimSpace(req.Command) == "" {
		return request{}, withCode(codeInvalidRequest, "missing required field command", nil)
	}
	return req, nil
}

type commonFlags struct {
	dbPath       string
	guildID      string
	guildName    string
	since        string
	until        string
	beforeTime   string
	beforeID     string
	resolveNames bool
	limit        int
	pretty       bool
}

func newCommonFlagSet(name string) (*flag.FlagSet, *commonFlags) {
	cfg := &commonFlags{}
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&cfg.dbPath, "db", defaultDBPath(), "sqlite database path")
	fs.StringVar(&cfg.guildID, "guild-id", "", "guild id (optional)")
	fs.StringVar(&cfg.guildName, "guild-name", "", "guild name (optional)")
	fs.StringVar(&cfg.since, "since", "", "inclusive lower RFC3339 timestamp filter")
	fs.StringVar(&cfg.until, "until", "", "inclusive upper RFC3339 timestamp filter")
	fs.StringVar(&cfg.beforeTime, "before-time", "", "exclusive upper RFC3339 cursor timestamp")
	fs.StringVar(&cfg.beforeID, "before-id", "", "cursor id within before-time")
	fs.BoolVar(&cfg.resolveNames, "resolve-names", false, "populate author/channel/guild names in message-like rows")
	fs.IntVar(&cfg.limit, "limit", config.DefaultQueryLimit, "max returned rows")
	fs.BoolVar(&cfg.pretty, "pretty", false, "pretty-print JSON")
	return fs, cfg
}

func defaultDBPath() string {
	if v := strings.TrimSpace(os.Getenv(config.EnvDiscordLogDB)); v != "" {
		return v
	}
	return config.DefaultLogDBPath
}

func requireNonEmpty(name, value string) error {
	if strings.TrimSpace(value) == "" {
		return withCode(codeInvalidArgument, fmt.Sprintf("missing required field %s", name), nil)
	}
	return nil
}

type fieldAlias struct {
	name  string
	value string
}

func requireAnyNonEmpty(fields ...fieldAlias) error {
	for _, f := range fields {
		if strings.TrimSpace(f.value) != "" {
			return nil
		}
	}
	names := make([]string, 0, len(fields))
	for _, f := range fields {
		names = append(names, f.name)
	}
	return withCode(codeInvalidArgument, fmt.Sprintf("missing required field %s", strings.Join(names, " or ")), nil)
}

func parseCSVList(input string) []string {
	if strings.TrimSpace(input) == "" {
		return nil
	}
	parts := strings.Split(input, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		out = append(out, part)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func printResponse(pretty bool, v response, out io.Writer) error {
	if v.Items == nil {
		v.Items = []querysvc.Record{}
	}
	var (
		b   []byte
		err error
	)
	if pretty {
		b, err = json.MarshalIndent(v, "", "  ")
	} else {
		b, err = json.Marshal(v)
	}
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(out, string(b))
	return err
}

func writeError(stderr io.Writer, err error) int {
	code, message := codeAndMessage(err)
	payload := errorEnvelope{
		Error: errorPayload{
			Code:    code,
			Message: message,
		},
	}
	b, marshalErr := json.Marshal(payload)
	if marshalErr != nil {
		_, _ = fmt.Fprintf(stderr, `{"error":{"code":"%s","message":"%s"}}`+"\n", codeInternal, "failed to encode error payload")
		return exitInternal
	}
	_, _ = fmt.Fprintln(stderr, string(b))
	return exitCode(code)
}

func codeAndMessage(err error) (string, string) {
	var cErr *codedError
	if errors.As(err, &cErr) {
		return cErr.code, cErr.Error()
	}
	return codeInternal, err.Error()
}

func exitCode(code string) int {
	switch code {
	case codeInvalidCommand, codeInvalidArgument, codeInvalidRequest, codeReadRequestFailed, codeDecodeRequestFailed:
		return exitInvalid
	case codeOpenDBFailed:
		return exitOpenDB
	case codeGuildResolution, codeQueryFailed:
		return exitQuery
	default:
		return exitInternal
	}
}
