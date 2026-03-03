# dc-logger

Discord server backup tool that stores message and lifecycle data in SQLite.

## First-Time Setup (From Zero)

Use this if you have never run the bot before.

### 1) Install Go

- Install latest Go version.
- Verify:

```bash
go version
```

### 2) Create Discord Application + Bot

1. Open Discord Developer Portal and create a new application.
2. Open the **Bot** tab and create/add a bot user.
3. In **Privileged Gateway Intents**, enable:
   - `Server Members Intent`
   - `Message Content Intent`
4. Copy the bot token (you will put it into `.env` below).

### 3) Invite Bot With Minimal Read-Only Permissions

In **OAuth2 -> URL Generator**:

1. Scopes: check `bot`.
2. Bot Permissions:
   - `View Channels`
   - `Read Message History`
3. Integration Type: `Guild Install`.
4. Open the generated invite URL and add the bot to your server.

Optional permissions:

- `View Audit Log` (for reliable kick/moderator attribution).
- `Manage Threads` (for private archived-thread backfill coverage).

Reference: [PERMISSIONS.md](./PERMISSIONS.md)

### 4) Configure Local Environment (Persistent File)

```bash
cp .env.example .env
```

Edit `.env` and set at minimum:

- `DISCORD_BOT_TOKEN`
- `DISCORD_LOG_DB` (or keep default)
- `DISCORD_SYNC_GUILD_IDS="*"` for first run

`.env` is gitignored in this repository so bot tokens and local IDs are not committed.

Load env vars from file:

```bash
set -a
source .env
set +a
```

### 5) First Run to Discover Guild ID(s)

```bash
go run .
```

Look for startup log lines:

- `syncing guilds: <Guild Name> (<GuildID>)`
- `setup hint: currently syncing all guilds ... set DISCORD_SYNC_GUILD_IDS="<id1,id2,...>"`

### 6) Pin Guild ID(s) and Persist

Update `.env`:

```bash
DISCORD_SYNC_GUILD_IDS="<GuildID>"
```

Or for multiple guilds:

```bash
DISCORD_SYNC_GUILD_IDS="<GuildID1>,<GuildID2>"
```

Reload env vars (`set -a; source .env; set +a`) and run again.

## What It Tracks

- Messages sent (non-bot, non-empty content)
- Message relationship metadata for replies/thread context (`referenced_*`, `thread_*`)
- Attachment metadata (including attachment-only messages)
- Message updates (latest content in `messages`, full edit history in `lifecycle_events`)
- Message deletes
- Thread create/delete
- Channel create/delete
- ID-to-name mappings for guilds/channels/users (`id_name_mappings`)
- Role metadata and role name mappings (`roles`, `id_name_mappings`)
- Current member-role snapshot (`member_roles`)
- Current server-member snapshot (`guild_members`)
- Name-change lifecycle events (`guild_renamed`, `channel_renamed`, `thread_renamed`, `username_changed`)
- Membership lifecycle events (`user_joined_server`, `user_left_server`, `user_kicked_from_server`, `user_banned_from_server`)
- Structured mention events for users and roles (`user_pinged`, `role_pinged`, `ping_events`, `role_ping_events`)
- Startup backfill for missed history (with resumable channel high-water marks, full-history cold starts, and archived-thread coverage)

## Configuration

This project currently uses **environment variables** (no CLI flags yet).
Canonical env-var keys/defaults are centralized in `internal/config/env.go`.

### Required

- `DISCORD_BOT_TOKEN`
  - Bot token used to connect to Discord.
  - No default. Process exits if missing.

### Optional

- `DISCORD_LOG_DB`
  - Path to SQLite database file.
  - Default: `./database/database.db`

- `DISCORD_BACKFILL_MAX_PAGES_PER_RUN`
  - Startup backfill budget: max number of message-page requests per run.
  - `0` means unlimited.
  - Default: `0`

- `DISCORD_BACKFILL_MAX_MINUTES`
  - Startup backfill budget: max backfill runtime in minutes.
  - `0` means unlimited.
  - Default: `0`

- `DISCORD_ATTACHMENT_TEXT_MAX_BYTES`
  - Max bytes to fetch/store from text-like attachments (e.g. `.txt`, `text/plain`).
  - Helps capture Discord "send long message as file" content.
  - Default: `1048576` (1 MiB)

- `DISCORD_SYNC_GUILD_IDS`
  - Comma-separated guild IDs to sync.
  - Default: `*` (sync all guilds the bot is in)
  - Use `*` to sync all guilds the bot is in.
  - If you do not know your guild ID yet, leave this unset for first run and copy the ID(s) from startup logs (`syncing guilds: <name> (<id>)`).
  - Persist chosen ID(s) in `DISCORD_SYNC_GUILD_IDS` (for example in your shell profile or service env file).

If a backfill budget value is invalid (non-integer or negative), the app logs a warning and falls back to default.

## Usage

### 1) Set environment variables

```bash
cp .env.example .env
# edit .env with your token and desired guild IDs
set -a
source .env
set +a
```

Optional startup budget:

```bash
export DISCORD_BACKFILL_MAX_PAGES_PER_RUN=500
export DISCORD_BACKFILL_MAX_MINUTES=10
```

Or set those keys directly in `.env`.

### 2) Run

```bash
go run .
```

Or build and run:

```bash
go build -o dc-logger .
./dc-logger
```

## Programmatic Retrieval (`dc-query`)

Use the standalone query helper for read-only retrieval that is LLM/tool friendly. Alternatively, you can also instruct an LLM to fallback to sqlite3 usage (read-only advised) for querying directly from the db.

Build:

```bash
go build -o dc-query ./cmd/dc-query
```

Commands:

- `recent-messages`
- `recent-messages-in-channel`
- `list-server-members`
- `recent-messages-by-user`
- `last-message-by-user`
- `roles-of-user`
- `users-with-role`
- `recent-pings-by-user`
- `recent-pings-targeting-user`
- `unanswered-pings-targeting-user`
- `last-pinged-user-by-user`
- `recent-events`
- `topic-activity-summary`
- `server-activity-summary`
- `channel-activity-summary`
- `author-activity-summary`
- `keyword-frequency`
- `search-messages`
- `json-request`

Examples:

```bash
./dc-query recent-messages --limit 10 --pretty
./dc-query recent-messages-in-channel --channel-name general --guild-name "Lorem" --limit 5 --pretty
./dc-query list-server-members --guild-name "Lorem" --limit 50 --pretty
./dc-query last-message-by-user --author-name ipsum --guild-name "Lorem" --pretty
./dc-query roles-of-user --author-name ipsum --guild-name "Lorem" --limit 20 --pretty
./dc-query users-with-role --role-name Consectetur --guild-name "Lorem" --limit 20 --pretty
./dc-query recent-pings-targeting-user --target-name dolor --guild-name "Lorem" --limit 5 --pretty
./dc-query unanswered-pings-targeting-user --target-name dolor --guild-name "Lorem" --since 2026-01-01T00:00:00Z --limit 5 --pretty
./dc-query last-pinged-user-by-user --author-name ipsum --guild-name "Lorem" --pretty
./dc-query recent-events --event-type message_deleted --since 2026-01-01T00:00:00Z --limit 5 --pretty
./dc-query topic-activity-summary --query "incident" --guild-name "Lorem" --since 2026-01-01T00:00:00Z --limit 10 --pretty
./dc-query server-activity-summary --guild-name "Lorem" --since 2026-01-01T00:00:00Z --until 2026-01-31T23:59:59Z --pretty
./dc-query server-activity-summary --guild-name "Lorem" --since 2026-01-01T00:00:00Z --until 2026-01-31T23:59:59Z --by-day --pretty
./dc-query channel-activity-summary --guild-name "Lorem" --since 2026-01-01T00:00:00Z --limit 10 --pretty
./dc-query author-activity-summary --guild-name "Lorem" --since 2026-01-01T00:00:00Z --limit 10 --pretty
./dc-query keyword-frequency --guild-name "Lorem" --since 2026-01-01T00:00:00Z --min-length 5 --stopwords "devnet,client" --limit 20 --pretty
./dc-query search-messages --query "incident" --limit 5 --pretty
printf '{"command":"last_message_by_user","guild_name":"Lorem","author_name":"ipsum"}' | ./dc-query json-request
```

Notes:

- Output is JSON.
- Failures are JSON with stable `error.code` values and non-zero exit codes.
- Default DB path is `DISCORD_LOG_DB` or `./database/database.db`.
- `--guild-id`/`--guild-name` are optional; when both are omitted, `dc-query` uses the most common guild ID found in the database.
- `recent-messages-in-channel` accepts `--channel-id` or `--channel-name`.
- `list-server-members` returns current known guild members (current-state snapshot).
- `recent-messages-by-user` and `last-message-by-user` accept `--author-id` or `--author-name`.
- `roles-of-user` accepts `--author-id` or `--author-name`.
- `users-with-role` accepts `--role-id` or `--role-name`.
- `recent-pings-by-user` and `last-pinged-user-by-user` accept `--author-id` or `--author-name`.
- `recent-pings-targeting-user` accepts `--target-id` or `--target-name`.
- `recent-pings-targeting-user` includes direct user mentions and role-based mentions (when the target user currently has the mentioned role).
- `unanswered-pings-targeting-user` accepts `--target-id` or `--target-name` and returns mentions where that user has no later message in the same channel.
- Ping query rows include message text in `content` when available.
- In ping query rows, Discord user-mention tokens are rendered as `@username`.
- In ping query rows, Discord role-mention tokens are rendered as `@role-name`.
- Ping detection uses Discord mention metadata (`Message.Mentions`), not raw string parsing.
- Self-mentions are recorded as ping events as well.
- `recent-events` returns records from `lifecycle_events` and supports `--event-type`, `--author-id`/`--author-name`, `--channel-id`/`--channel-name`, `--since`, and `--until`.
- `topic-activity-summary` groups topic-query matches by author and returns each author's latest matching item plus `payload_json.hit_count`.
- `server-activity-summary` returns one row with `message_count`, `unique_authors`, `unique_channels`, `first_seen_at`, and `last_seen_at`; with `--by-day`, it returns one row per UTC day with those same fields plus `day`.
- `channel-activity-summary` and `author-activity-summary` return per-channel/per-author message counts, percentages, and first/last timestamps for the selected window.
- `keyword-frequency` returns top terms (`term`, `hit_count`) from message + attachment text in the selected window; supports `--min-length` and optional comma-separated `--stopwords`.
- `recent-messages`, `recent-messages-in-channel`, `recent-messages-by-user`, `recent-events`, `search-messages`, `topic-activity-summary`, `server-activity-summary`, `channel-activity-summary`, `author-activity-summary`, `keyword-frequency`, and `unanswered-pings-targeting-user` support optional `--since`/`--until` RFC3339 time bounds.
- Cursor pagination is supported with `--before-time` and optional `--before-id` (requires `--before-time`).
- Query rows now include relationship fields when available: `referenced_message_id`, `referenced_channel_id`, `referenced_guild_id`, `thread_id`, `thread_parent_id`.
- Message-style commands (`recent-messages*`, `last-message-by-user`, `search-messages`, `topic-activity-summary`) support `--resolve-names` to populate `guild_name` / `channel_name` / `author_name`.
- Search uses SQLite FTS5 (`message_search_fts`) when available; otherwise it falls back to `LIKE` matching.
- `search-messages` now returns `items: []` when there are zero matches.
- Queries are read-only (`PRAGMA query_only=ON`) and limits are clamped to `1..100`.
- `json-request` accepts one request object with fields: `command`, `db`, `guild_id`, `guild_name`, `channel_id`, `channel_name`, `author_id`, `author_name`, `target_id`, `target_name`, `role_id`, `role_name`, `event_type`, `query`, `since`, `until`, `by_day`, `before_time`, `before_id`, `resolve_names`, `min_length`, `stopwords`, `limit`, `pretty`.

## Discord App Requirements

- Bot must be in the server(s) you want to back up.
- Required gateway intents in code:
  - `GUILDS`
  - `GUILD_MEMBERS` (for prompt username-change mapping updates)
  - `GUILD_BANS` (to remove users from current-member snapshot when banned)
  - `GUILD_MESSAGES`
  - `MESSAGE_CONTENT`
- Ensure Message Content intent is enabled for your bot in the Discord Developer Portal where required.
- For reliable kick/moderator attribution, grant the bot `View Audit Log` in the server.
- Optional: grant the bot `Manage Threads` if you want private archived-thread backfill coverage.
  - If granted: startup backfill includes private archived threads (where channel visibility allows).
  - If not granted: startup backfill still proceeds; private archived threads are skipped and a single startup notice is logged.

## Backfill + Rate Limit Notes

- Startup backfill runs on `READY`.
- Backfill progress and summary are logged (requests, 429 count, retry-after total, channels remaining, messages inserted).
- Discordgo retries on 429 responses; this project additionally records rate-limit metrics for visibility.
- If private archived-thread access is missing, backfill logs one concise notice and continues without private archived threads.
- Human-readable `message_sent` logs are enabled only after startup completes ("we are live-logging now"), so startup sync events are intentionally suppressed.
- If the DB file disappears during runtime, the process aborts instead of continuing on a detached SQLite file handle.
- On restart, startup backfill resumes incrementally from `channel_state`; if `channel_state` is missing but messages already exist for a channel, it seeds from the latest stored message and catches up.

### Backfill Budget

In logs, **budget** means a self-imposed startup backfill limit.

- `DISCORD_BACKFILL_MAX_PAGES_PER_RUN`: max number of message-page fetches per run.
- `DISCORD_BACKFILL_MAX_MINUTES`: max startup backfill runtime in minutes.

A **budget stop** means one of those limits was reached, so startup backfill pauses early on purpose and the bot continues with live event syncing.

The startup summary includes `budget_reason`:

- `none`: no limit was hit (or limits are unlimited).
- `max_pages_per_run`: page budget was hit.
- `max_backfill_minutes`: time budget was hit.

## Quality Checks

```bash
make check
```

Equivalent individual commands:

```bash
gofmt -w .
go vet ./...
go test ./...
```
