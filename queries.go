// Package main declares runtime write-path SQL (schema, migrations, upserts, lifecycle inserts).
// Read-only retrieval SQL for dc-query lives in internal/querysvc/service_queries.go.
package main

var sqlitePragmas = []string{
	`PRAGMA journal_mode = WAL;`,
	`PRAGMA synchronous = NORMAL;`,
	`PRAGMA foreign_keys = ON;`,
	`PRAGMA busy_timeout = 5000;`,
	`PRAGMA temp_store = MEMORY;`,
}

const sqliteSchemaQuery = `
CREATE TABLE IF NOT EXISTS messages (
	id          INTEGER PRIMARY KEY AUTOINCREMENT,
	message_id  TEXT NOT NULL UNIQUE,
	guild_id    TEXT NOT NULL,
	channel_id  TEXT NOT NULL,
	author_id   TEXT NOT NULL,
	created_at  TEXT NOT NULL,
	content     TEXT NOT NULL,
	referenced_message_id TEXT NOT NULL DEFAULT '',
	referenced_channel_id TEXT NOT NULL DEFAULT '',
	referenced_guild_id   TEXT NOT NULL DEFAULT '',
	thread_id             TEXT NOT NULL DEFAULT '',
	thread_parent_id      TEXT NOT NULL DEFAULT '',
	edited_at   TEXT NOT NULL DEFAULT '',
	deleted_at  TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_messages_channel_id
	ON messages(channel_id, id);

CREATE INDEX IF NOT EXISTS idx_messages_author_id
	ON messages(author_id, id);

CREATE INDEX IF NOT EXISTS idx_messages_guild_id
	ON messages(guild_id, id);

CREATE INDEX IF NOT EXISTS idx_messages_guild_created
	ON messages(guild_id, created_at, message_id);

CREATE INDEX IF NOT EXISTS idx_messages_channel_created
	ON messages(channel_id, created_at, message_id);

CREATE INDEX IF NOT EXISTS idx_messages_author_created
	ON messages(author_id, created_at, message_id);

CREATE TABLE IF NOT EXISTS attachments (
	id             INTEGER PRIMARY KEY AUTOINCREMENT,
	attachment_id  TEXT NOT NULL UNIQUE,
	message_id     TEXT NOT NULL,
	guild_id       TEXT NOT NULL,
	channel_id     TEXT NOT NULL,
	author_id      TEXT NOT NULL,
	created_at     TEXT NOT NULL,
	filename       TEXT NOT NULL,
	content_type   TEXT NOT NULL,
	size           INTEGER NOT NULL,
	url            TEXT NOT NULL,
	proxy_url      TEXT NOT NULL,
	content_text   TEXT NOT NULL,
	referenced_message_id TEXT NOT NULL DEFAULT '',
	referenced_channel_id TEXT NOT NULL DEFAULT '',
	referenced_guild_id   TEXT NOT NULL DEFAULT '',
	thread_id             TEXT NOT NULL DEFAULT '',
	thread_parent_id      TEXT NOT NULL DEFAULT '',
	updated_at     TEXT NOT NULL,
	deleted_at     TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_attachments_message_id
	ON attachments(message_id, id);

CREATE INDEX IF NOT EXISTS idx_attachments_channel_id
	ON attachments(channel_id, id);

CREATE INDEX IF NOT EXISTS idx_attachments_guild_id
	ON attachments(guild_id, id);

CREATE INDEX IF NOT EXISTS idx_attachments_deleted_at
	ON attachments(deleted_at, id);

CREATE INDEX IF NOT EXISTS idx_attachments_guild_created
	ON attachments(guild_id, created_at, attachment_id);

CREATE INDEX IF NOT EXISTS idx_attachments_channel_created
	ON attachments(channel_id, created_at, attachment_id);

CREATE INDEX IF NOT EXISTS idx_attachments_author_created
	ON attachments(author_id, created_at, attachment_id);

CREATE TABLE IF NOT EXISTS channel_state (
	channel_id       TEXT PRIMARY KEY,
	guild_id         TEXT NOT NULL,
	last_message_id  TEXT NOT NULL,
	updated_at       TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_channel_state_guild
	ON channel_state(guild_id);

CREATE TABLE IF NOT EXISTS channels (
	channel_id  TEXT PRIMARY KEY,
	guild_id    TEXT NOT NULL,
	name        TEXT NOT NULL,
	type        INTEGER NOT NULL,
	parent_id   TEXT NOT NULL,
	is_thread   INTEGER NOT NULL,
	updated_at  TEXT NOT NULL,
	deleted_at  TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_channels_guild
	ON channels(guild_id);

CREATE INDEX IF NOT EXISTS idx_channels_parent
	ON channels(parent_id);

CREATE TABLE IF NOT EXISTS users (
	author_id     TEXT PRIMARY KEY,
	username      TEXT NOT NULL,
	global_name   TEXT NOT NULL,
	discriminator TEXT NOT NULL,
	is_bot        INTEGER NOT NULL,
	updated_at    TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_users_updated
	ON users(updated_at);

CREATE TABLE IF NOT EXISTS id_name_mappings (
	entity_type      TEXT NOT NULL,
	entity_id        TEXT NOT NULL,
	guild_id         TEXT NOT NULL,
	human_name       TEXT NOT NULL,
	normalized_name  TEXT NOT NULL,
	updated_at       TEXT NOT NULL,
	PRIMARY KEY(entity_type, entity_id, guild_id)
);

CREATE INDEX IF NOT EXISTS idx_id_name_mappings_lookup
	ON id_name_mappings(entity_type, guild_id, normalized_name, updated_at, entity_id);

CREATE INDEX IF NOT EXISTS idx_id_name_mappings_entity
	ON id_name_mappings(entity_type, entity_id, guild_id);

CREATE TABLE IF NOT EXISTS roles (
	role_id     TEXT NOT NULL,
	guild_id    TEXT NOT NULL,
	name        TEXT NOT NULL,
	updated_at  TEXT NOT NULL,
	deleted_at  TEXT NOT NULL DEFAULT '',
	PRIMARY KEY(role_id, guild_id)
);

CREATE INDEX IF NOT EXISTS idx_roles_guild
	ON roles(guild_id, updated_at, role_id);

CREATE TABLE IF NOT EXISTS guild_members (
	guild_id    TEXT NOT NULL,
	user_id     TEXT NOT NULL,
	updated_at  TEXT NOT NULL,
	PRIMARY KEY(guild_id, user_id)
);

CREATE INDEX IF NOT EXISTS idx_guild_members_guild
	ON guild_members(guild_id, updated_at, user_id);

CREATE TABLE IF NOT EXISTS member_roles (
	guild_id    TEXT NOT NULL,
	user_id     TEXT NOT NULL,
	role_id     TEXT NOT NULL,
	updated_at  TEXT NOT NULL,
	PRIMARY KEY(guild_id, user_id, role_id)
);

CREATE INDEX IF NOT EXISTS idx_member_roles_user
	ON member_roles(guild_id, user_id, updated_at, role_id);

CREATE INDEX IF NOT EXISTS idx_member_roles_role
	ON member_roles(guild_id, role_id, updated_at, user_id);

CREATE TABLE IF NOT EXISTS ping_events (
	id          INTEGER PRIMARY KEY AUTOINCREMENT,
	guild_id    TEXT NOT NULL,
	channel_id  TEXT NOT NULL,
	message_id  TEXT NOT NULL,
	actor_id    TEXT NOT NULL,
	target_id   TEXT NOT NULL,
	target_name TEXT NOT NULL,
	occurred_at TEXT NOT NULL,
	UNIQUE(message_id, actor_id, target_id)
);

CREATE INDEX IF NOT EXISTS idx_ping_events_actor_time
	ON ping_events(guild_id, actor_id, occurred_at, id);

CREATE INDEX IF NOT EXISTS idx_ping_events_target_time
	ON ping_events(guild_id, target_id, occurred_at, id);

CREATE INDEX IF NOT EXISTS idx_ping_events_message
	ON ping_events(message_id, id);

CREATE TABLE IF NOT EXISTS role_ping_events (
	id          INTEGER PRIMARY KEY AUTOINCREMENT,
	guild_id    TEXT NOT NULL,
	channel_id  TEXT NOT NULL,
	message_id  TEXT NOT NULL,
	actor_id    TEXT NOT NULL,
	role_id     TEXT NOT NULL,
	role_name   TEXT NOT NULL,
	occurred_at TEXT NOT NULL,
	UNIQUE(message_id, actor_id, role_id)
);

CREATE INDEX IF NOT EXISTS idx_role_ping_events_actor_time
	ON role_ping_events(guild_id, actor_id, occurred_at, id);

CREATE INDEX IF NOT EXISTS idx_role_ping_events_role_time
	ON role_ping_events(guild_id, role_id, occurred_at, id);

CREATE INDEX IF NOT EXISTS idx_role_ping_events_message
	ON role_ping_events(message_id, id);

CREATE TABLE IF NOT EXISTS lifecycle_events (
	id           INTEGER PRIMARY KEY AUTOINCREMENT,
	event_type   TEXT NOT NULL,
	guild_id     TEXT NOT NULL,
	channel_id   TEXT NOT NULL,
	message_id   TEXT NOT NULL,
	actor_id     TEXT NOT NULL,
	occurred_at  TEXT NOT NULL,
	payload_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_lifecycle_events_type_time
	ON lifecycle_events(event_type, occurred_at, id);

CREATE INDEX IF NOT EXISTS idx_lifecycle_events_guild_time
	ON lifecycle_events(guild_id, occurred_at, id);

CREATE INDEX IF NOT EXISTS idx_lifecycle_events_channel_time
	ON lifecycle_events(channel_id, occurred_at, id);

CREATE INDEX IF NOT EXISTS idx_lifecycle_events_message_time
	ON lifecycle_events(message_id, occurred_at, id);

CREATE INDEX IF NOT EXISTS idx_lifecycle_events_guild_type_time
	ON lifecycle_events(guild_id, event_type, occurred_at, id);

CREATE INDEX IF NOT EXISTS idx_lifecycle_events_guild_actor_time
	ON lifecycle_events(guild_id, actor_id, occurred_at, id);
`

var sqliteSchemaMigrations = []string{
	`ALTER TABLE messages ADD COLUMN edited_at TEXT NOT NULL DEFAULT '';`,
	`ALTER TABLE messages ADD COLUMN deleted_at TEXT NOT NULL DEFAULT '';`,
	`ALTER TABLE channels ADD COLUMN deleted_at TEXT NOT NULL DEFAULT '';`,
	`ALTER TABLE messages ADD COLUMN referenced_message_id TEXT NOT NULL DEFAULT '';`,
	`ALTER TABLE messages ADD COLUMN referenced_channel_id TEXT NOT NULL DEFAULT '';`,
	`ALTER TABLE messages ADD COLUMN referenced_guild_id TEXT NOT NULL DEFAULT '';`,
	`ALTER TABLE messages ADD COLUMN thread_id TEXT NOT NULL DEFAULT '';`,
	`ALTER TABLE messages ADD COLUMN thread_parent_id TEXT NOT NULL DEFAULT '';`,
	`ALTER TABLE attachments ADD COLUMN referenced_message_id TEXT NOT NULL DEFAULT '';`,
	`ALTER TABLE attachments ADD COLUMN referenced_channel_id TEXT NOT NULL DEFAULT '';`,
	`ALTER TABLE attachments ADD COLUMN referenced_guild_id TEXT NOT NULL DEFAULT '';`,
	`ALTER TABLE attachments ADD COLUMN thread_id TEXT NOT NULL DEFAULT '';`,
	`ALTER TABLE attachments ADD COLUMN thread_parent_id TEXT NOT NULL DEFAULT '';`,
	`CREATE TABLE IF NOT EXISTS archived_discovery_state (
		parent_channel_id TEXT PRIMARY KEY,
		guild_id          TEXT NOT NULL,
		last_checked_at   TEXT NOT NULL,
		threads_found     INTEGER NOT NULL
	);`,
	`CREATE INDEX IF NOT EXISTS idx_archived_discovery_state_guild
		ON archived_discovery_state(guild_id);`,
}

var sqlitePostMigrationIndexes = []string{
	`CREATE INDEX IF NOT EXISTS idx_channels_deleted_at ON channels(deleted_at);`,
	`CREATE INDEX IF NOT EXISTS idx_messages_edited_at ON messages(edited_at, id);`,
	`CREATE INDEX IF NOT EXISTS idx_messages_deleted_at ON messages(deleted_at, id);`,
	`CREATE INDEX IF NOT EXISTS idx_messages_referenced_message_id ON messages(referenced_message_id, id);`,
	`CREATE INDEX IF NOT EXISTS idx_messages_thread_id ON messages(thread_id, id);`,
	`CREATE INDEX IF NOT EXISTS idx_messages_guild_created ON messages(guild_id, created_at, message_id);`,
	`CREATE INDEX IF NOT EXISTS idx_messages_channel_created ON messages(channel_id, created_at, message_id);`,
	`CREATE INDEX IF NOT EXISTS idx_messages_author_created ON messages(author_id, created_at, message_id);`,
	`CREATE INDEX IF NOT EXISTS idx_attachments_referenced_message_id ON attachments(referenced_message_id, id);`,
	`CREATE INDEX IF NOT EXISTS idx_attachments_thread_id ON attachments(thread_id, id);`,
	`CREATE INDEX IF NOT EXISTS idx_attachments_guild_created ON attachments(guild_id, created_at, attachment_id);`,
	`CREATE INDEX IF NOT EXISTS idx_attachments_channel_created ON attachments(channel_id, created_at, attachment_id);`,
	`CREATE INDEX IF NOT EXISTS idx_attachments_author_created ON attachments(author_id, created_at, attachment_id);`,
	`CREATE INDEX IF NOT EXISTS idx_lifecycle_events_guild_type_time ON lifecycle_events(guild_id, event_type, occurred_at, id);`,
	`CREATE INDEX IF NOT EXISTS idx_lifecycle_events_guild_actor_time ON lifecycle_events(guild_id, actor_id, occurred_at, id);`,
}

const createMessageSearchFTSTableQuery = `
CREATE VIRTUAL TABLE IF NOT EXISTS message_search_fts USING fts5(
	row_key UNINDEXED,
	source UNINDEXED,
	message_id UNINDEXED,
	attachment_id UNINDEXED,
	guild_id UNINDEXED,
	channel_id UNINDEXED,
	author_id UNINDEXED,
	created_at UNINDEXED,
	content,
	filename UNINDEXED,
	content_type UNINDEXED,
	url UNINDEXED
);
`

const createMessageSearchFTSMessageInsertTriggerQuery = `
CREATE TRIGGER IF NOT EXISTS trg_message_search_fts_messages_ai
AFTER INSERT ON messages
WHEN NEW.deleted_at = ''
BEGIN
	INSERT INTO message_search_fts(
		row_key, source, message_id, attachment_id, guild_id, channel_id, author_id, created_at, content, filename, content_type, url
	) VALUES (
		'm:' || NEW.message_id, 'message', NEW.message_id, '', NEW.guild_id, NEW.channel_id, NEW.author_id, NEW.created_at, NEW.content, '', '', ''
	);
END;
`

const createMessageSearchFTSMessageUpdateTriggerQuery = `
CREATE TRIGGER IF NOT EXISTS trg_message_search_fts_messages_au
AFTER UPDATE ON messages
BEGIN
	DELETE FROM message_search_fts
	WHERE row_key = 'm:' || OLD.message_id;

	INSERT INTO message_search_fts(
		row_key, source, message_id, attachment_id, guild_id, channel_id, author_id, created_at, content, filename, content_type, url
	)
	SELECT
		'm:' || NEW.message_id, 'message', NEW.message_id, '', NEW.guild_id, NEW.channel_id, NEW.author_id, NEW.created_at, NEW.content, '', '', ''
	WHERE NEW.deleted_at = '';
END;
`

const createMessageSearchFTSMessageDeleteTriggerQuery = `
CREATE TRIGGER IF NOT EXISTS trg_message_search_fts_messages_ad
AFTER DELETE ON messages
BEGIN
	DELETE FROM message_search_fts
	WHERE row_key = 'm:' || OLD.message_id;
END;
`

const createMessageSearchFTSAttachmentInsertTriggerQuery = `
CREATE TRIGGER IF NOT EXISTS trg_message_search_fts_attachments_ai
AFTER INSERT ON attachments
WHEN NEW.deleted_at = ''
BEGIN
	INSERT INTO message_search_fts(
		row_key, source, message_id, attachment_id, guild_id, channel_id, author_id, created_at, content, filename, content_type, url
	) VALUES (
		'a:' || NEW.attachment_id,
		'attachment',
		NEW.message_id,
		NEW.attachment_id,
		NEW.guild_id,
		NEW.channel_id,
		NEW.author_id,
		NEW.created_at,
		CASE
			WHEN NEW.content_text != '' THEN NEW.content_text
			ELSE '[attachment] ' || NEW.filename
		END,
		NEW.filename,
		NEW.content_type,
		NEW.url
	);
END;
`

const createMessageSearchFTSAttachmentUpdateTriggerQuery = `
CREATE TRIGGER IF NOT EXISTS trg_message_search_fts_attachments_au
AFTER UPDATE ON attachments
BEGIN
	DELETE FROM message_search_fts
	WHERE row_key = 'a:' || OLD.attachment_id;

	INSERT INTO message_search_fts(
		row_key, source, message_id, attachment_id, guild_id, channel_id, author_id, created_at, content, filename, content_type, url
	)
	SELECT
		'a:' || NEW.attachment_id,
		'attachment',
		NEW.message_id,
		NEW.attachment_id,
		NEW.guild_id,
		NEW.channel_id,
		NEW.author_id,
		NEW.created_at,
		CASE
			WHEN NEW.content_text != '' THEN NEW.content_text
			ELSE '[attachment] ' || NEW.filename
		END,
		NEW.filename,
		NEW.content_type,
		NEW.url
	WHERE NEW.deleted_at = '';
END;
`

const createMessageSearchFTSAttachmentDeleteTriggerQuery = `
CREATE TRIGGER IF NOT EXISTS trg_message_search_fts_attachments_ad
AFTER DELETE ON attachments
BEGIN
	DELETE FROM message_search_fts
	WHERE row_key = 'a:' || OLD.attachment_id;
END;
`

var sqliteFTSSetup = []string{
	createMessageSearchFTSTableQuery,
	createMessageSearchFTSMessageInsertTriggerQuery,
	createMessageSearchFTSMessageUpdateTriggerQuery,
	createMessageSearchFTSMessageDeleteTriggerQuery,
	createMessageSearchFTSAttachmentInsertTriggerQuery,
	createMessageSearchFTSAttachmentUpdateTriggerQuery,
	createMessageSearchFTSAttachmentDeleteTriggerQuery,
}

const hasAnyMessageSearchFTSRowsQuery = `
SELECT EXISTS(SELECT 1 FROM message_search_fts LIMIT 1);
`

const hasAnyLiveMessagesQuery = `
SELECT EXISTS(SELECT 1 FROM messages WHERE deleted_at = '' LIMIT 1);
`

const hasAnyLiveAttachmentsQuery = `
SELECT EXISTS(SELECT 1 FROM attachments WHERE deleted_at = '' LIMIT 1);
`

const backfillMessageSearchFTSFromMessagesQuery = `
INSERT INTO message_search_fts(
	row_key, source, message_id, attachment_id, guild_id, channel_id, author_id, created_at, content, filename, content_type, url
)
SELECT
	'm:' || m.message_id,
	'message',
	m.message_id,
	'',
	m.guild_id,
	m.channel_id,
	m.author_id,
	m.created_at,
	m.content,
	'',
	'',
	''
FROM messages m
WHERE m.deleted_at = '';
`

const backfillMessageSearchFTSFromAttachmentsQuery = `
INSERT INTO message_search_fts(
	row_key, source, message_id, attachment_id, guild_id, channel_id, author_id, created_at, content, filename, content_type, url
)
SELECT
	'a:' || a.attachment_id,
	'attachment',
	a.message_id,
	a.attachment_id,
	a.guild_id,
	a.channel_id,
	a.author_id,
	a.created_at,
	CASE
		WHEN a.content_text != '' THEN a.content_text
		ELSE '[attachment] ' || a.filename
	END,
	a.filename,
	a.content_type,
	a.url
FROM attachments a
WHERE a.deleted_at = '';
`

const pruneMessageSearchFTSDeletedMessagesQuery = `
DELETE FROM message_search_fts
WHERE source = 'message'
	AND NOT EXISTS (
		SELECT 1
		FROM messages m
		WHERE m.message_id = message_search_fts.message_id
			AND m.deleted_at = ''
	);
`

const pruneMessageSearchFTSDeletedAttachmentsQuery = `
DELETE FROM message_search_fts
WHERE source = 'attachment'
	AND NOT EXISTS (
		SELECT 1
		FROM attachments a
		WHERE a.attachment_id = message_search_fts.attachment_id
			AND a.deleted_at = ''
	);
`

var sqliteFTSBootstrap = []string{
	backfillMessageSearchFTSFromMessagesQuery,
	backfillMessageSearchFTSFromAttachmentsQuery,
}

var sqliteFTSPrune = []string{
	pruneMessageSearchFTSDeletedMessagesQuery,
	pruneMessageSearchFTSDeletedAttachmentsQuery,
}

const insertMessageQuery = `
INSERT INTO messages(
	message_id, guild_id, channel_id, author_id, created_at, content,
	referenced_message_id, referenced_channel_id, referenced_guild_id, thread_id, thread_parent_id
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(message_id) DO NOTHING;
`

const upsertAttachmentQuery = `
INSERT INTO attachments(
	attachment_id, message_id, guild_id, channel_id, author_id, created_at,
	filename, content_type, size, url, proxy_url, content_text,
	referenced_message_id, referenced_channel_id, referenced_guild_id, thread_id, thread_parent_id,
	updated_at, deleted_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(attachment_id) DO UPDATE SET
	message_id=excluded.message_id,
	guild_id=excluded.guild_id,
	channel_id=excluded.channel_id,
	author_id=excluded.author_id,
	created_at=excluded.created_at,
	filename=excluded.filename,
	content_type=excluded.content_type,
	size=excluded.size,
	url=excluded.url,
	proxy_url=excluded.proxy_url,
	content_text=CASE
		WHEN excluded.content_text != '' THEN excluded.content_text
		ELSE attachments.content_text
	END,
	referenced_message_id=excluded.referenced_message_id,
	referenced_channel_id=excluded.referenced_channel_id,
	referenced_guild_id=excluded.referenced_guild_id,
	thread_id=excluded.thread_id,
	thread_parent_id=excluded.thread_parent_id,
	updated_at=excluded.updated_at,
	deleted_at=excluded.deleted_at;
`

const markAttachmentsDeletedByMessageQuery = `
UPDATE attachments
SET deleted_at = ?, updated_at = ?
WHERE message_id = ?;
`

const markMessageEditedQuery = `
UPDATE messages
SET content = ?, edited_at = ?
WHERE message_id = ? AND deleted_at = '';
`

const markMessageDeletedQuery = `
UPDATE messages
SET deleted_at = ?
WHERE message_id = ?;
`

const upsertChannelStateQuery = `
INSERT INTO channel_state(channel_id, guild_id, last_message_id, updated_at)
VALUES (?, ?, ?, ?)
ON CONFLICT(channel_id) DO UPDATE SET
	guild_id=excluded.guild_id,
	last_message_id=excluded.last_message_id,
	updated_at=excluded.updated_at;
`

const upsertChannelQuery = `
INSERT INTO channels(channel_id, guild_id, name, type, parent_id, is_thread, updated_at, deleted_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(channel_id) DO UPDATE SET
	guild_id=excluded.guild_id,
	name=excluded.name,
	type=excluded.type,
	parent_id=excluded.parent_id,
	is_thread=excluded.is_thread,
	updated_at=excluded.updated_at,
	deleted_at=excluded.deleted_at;
`

const markChannelDeletedQuery = `
UPDATE channels
SET deleted_at = ?, updated_at = ?
WHERE channel_id = ?;
`

const upsertUserQuery = `
INSERT INTO users(author_id, username, global_name, discriminator, is_bot, updated_at)
VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT(author_id) DO UPDATE SET
	username=excluded.username,
	global_name=excluded.global_name,
	discriminator=excluded.discriminator,
	is_bot=excluded.is_bot,
	updated_at=excluded.updated_at;
`

const upsertIDNameMappingQuery = `
INSERT INTO id_name_mappings(
	entity_type, entity_id, guild_id, human_name, normalized_name, updated_at
) VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT(entity_type, entity_id, guild_id) DO UPDATE SET
	human_name=CASE
		WHEN excluded.human_name != '' THEN excluded.human_name
		ELSE id_name_mappings.human_name
	END,
	normalized_name=CASE
		WHEN excluded.normalized_name != '' THEN excluded.normalized_name
		ELSE id_name_mappings.normalized_name
	END,
	updated_at=excluded.updated_at;
`

const upsertPingEventQuery = `
INSERT INTO ping_events(
	guild_id, channel_id, message_id, actor_id, target_id, target_name, occurred_at
) VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(message_id, actor_id, target_id) DO UPDATE SET
	target_name=excluded.target_name,
	occurred_at=excluded.occurred_at;
`

const upsertRoleQuery = `
INSERT INTO roles(role_id, guild_id, name, updated_at, deleted_at)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT(role_id, guild_id) DO UPDATE SET
	name=CASE
		WHEN excluded.name != '' THEN excluded.name
		ELSE roles.name
	END,
	updated_at=excluded.updated_at,
	deleted_at=excluded.deleted_at;
`

const markRoleDeletedQuery = `
UPDATE roles
SET deleted_at = ?, updated_at = ?
WHERE role_id = ? AND guild_id = ?;
`

const upsertMemberRoleQuery = `
INSERT INTO member_roles(guild_id, user_id, role_id, updated_at)
VALUES (?, ?, ?, ?)
ON CONFLICT(guild_id, user_id, role_id) DO UPDATE SET
	updated_at=excluded.updated_at;
`

const deleteMemberRolesByUserQuery = `
DELETE FROM member_roles
WHERE guild_id = ? AND user_id = ?;
`

const upsertRolePingEventQuery = `
INSERT INTO role_ping_events(
	guild_id, channel_id, message_id, actor_id, role_id, role_name, occurred_at
) VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(message_id, actor_id, role_id) DO UPDATE SET
	role_name=excluded.role_name,
	occurred_at=excluded.occurred_at;
`

const upsertGuildMemberQuery = `
INSERT INTO guild_members(guild_id, user_id, updated_at)
VALUES (?, ?, ?)
ON CONFLICT(guild_id, user_id) DO UPDATE SET
	updated_at=excluded.updated_at;
`

const deleteGuildMemberQuery = `
DELETE FROM guild_members
WHERE guild_id = ? AND user_id = ?;
`

const insertLifecycleEventQuery = `
INSERT INTO lifecycle_events(
	event_type, guild_id, channel_id, message_id, actor_id, occurred_at, payload_json
) VALUES (?, ?, ?, ?, ?, ?, ?);
`

const insertMessageUpdatedLifecycleEventDedupQuery = `
INSERT INTO lifecycle_events(
	event_type, guild_id, channel_id, message_id, actor_id, occurred_at, payload_json
)
SELECT ?, ?, ?, ?, ?, ?, ?
WHERE NOT EXISTS (
	SELECT 1
	FROM lifecycle_events
	WHERE event_type = 'message_updated'
		AND guild_id = ?
		AND channel_id = ?
		AND message_id = ?
		AND actor_id = ?
		AND json_extract(payload_json, '$.content') = ?
		AND COALESCE(json_extract(payload_json, '$.edited_at'), '') = ?
);
`

const getLastMessageIDByChannelQuery = `
SELECT last_message_id
FROM channel_state
WHERE channel_id = ?;
`

const getLatestStoredMessageIDByChannelQuery = `
SELECT message_id
FROM messages
WHERE channel_id = ?
ORDER BY created_at DESC, message_id DESC
LIMIT 1;
`

const selectCurrentMappedNameQuery = `
SELECT human_name
FROM id_name_mappings
WHERE entity_type = ? AND entity_id = ? AND guild_id = ?;
`

const selectChannelNameAndThreadByGuildAndChannelQuery = `
SELECT name, is_thread, parent_id
FROM channels
WHERE guild_id = ? AND channel_id = ?
ORDER BY updated_at DESC
LIMIT 1;
`

const selectMessageContentEditedByIDQuery = `
SELECT content, edited_at
FROM messages
WHERE message_id = ?;
`

const selectLifecyclePayloadByTypeAndMessageLatestQuery = `
SELECT payload_json
FROM lifecycle_events
WHERE event_type = ? AND message_id = ?
ORDER BY id DESC LIMIT 1;
`

const countMessagesByMessageIDQuery = `
SELECT COUNT(*)
FROM messages
WHERE message_id = ?;
`

const countLifecycleEventsByMessageIDQuery = `
SELECT COUNT(*)
FROM lifecycle_events
WHERE message_id = ?;
`

const selectChannelStateLastMessageByChannelQuery = `
SELECT last_message_id
FROM channel_state
WHERE channel_id = ?;
`

const countLifecycleByMessageAndTypeQuery = `
SELECT COUNT(*)
FROM lifecycle_events
WHERE message_id = ? AND event_type = ?;
`

const countAttachmentsByMessageIDQuery = `
SELECT COUNT(*)
FROM attachments
WHERE message_id = ?;
`

const countMessagesByGuildIDQuery = `
SELECT COUNT(*)
FROM messages
WHERE guild_id = ?;
`

const countMessagesByChannelIDQuery = `
SELECT COUNT(*)
FROM messages
WHERE channel_id = ?;
`

const countChannelsByChannelIDQuery = `
SELECT COUNT(*)
FROM channels
WHERE channel_id = ?;
`

const selectAttachmentContentTextByIDQuery = `
SELECT content_text
FROM attachments
WHERE attachment_id = ?;
`

const selectMessageContentEditedDeletedByIDQuery = `
SELECT content, edited_at, deleted_at
FROM messages
WHERE message_id = ?;
`

const countLifecycleByMessageAndTypesQuery = `
SELECT COUNT(*)
FROM lifecycle_events
WHERE message_id = ? AND event_type IN (?, ?);
`

const selectAttachmentDeletedAtByIDQuery = `
SELECT deleted_at
FROM attachments
WHERE attachment_id = ?;
`

const selectNameHumanAndNormalizedByTypeEntityGuildQuery = `
SELECT human_name, normalized_name
FROM id_name_mappings
WHERE entity_type = ? AND entity_id = ? AND guild_id = ?;
`

const selectNameEntityByTypeGuildNormalizedQuery = `
SELECT entity_id
FROM id_name_mappings
WHERE entity_type = ? AND guild_id = ? AND normalized_name = ?;
`

const selectNameHumanByTypeEntityGuildQuery = `
SELECT human_name
FROM id_name_mappings
WHERE entity_type = ? AND entity_id = ? AND guild_id = ?;
`

const selectLifecyclePayloadByTypeAndGuildLatestQuery = `
SELECT payload_json
FROM lifecycle_events
WHERE event_type = ? AND guild_id = ?
ORDER BY id DESC LIMIT 1;
`

const selectLifecyclePayloadByTypeGuildChannelLatestQuery = `
SELECT payload_json
FROM lifecycle_events
WHERE event_type = ? AND guild_id = ? AND channel_id = ?
ORDER BY id DESC LIMIT 1;
`

const countMemberRolesByGuildUserRoleQuery = `
SELECT COUNT(*)
FROM member_roles
WHERE guild_id = ? AND user_id = ? AND role_id = ?;
`

const selectLifecyclePayloadByTypeGuildActorLatestQuery = `
SELECT payload_json
FROM lifecycle_events
WHERE event_type = ? AND guild_id = ? AND actor_id = ?
ORDER BY id DESC LIMIT 1;
`

const countGuildMembersByGuildUserQuery = `
SELECT COUNT(*)
FROM guild_members
WHERE guild_id = ? AND user_id = ?;
`

const selectPingTargetByGuildMessageActorQuery = `
SELECT target_id, target_name
FROM ping_events
WHERE guild_id = ? AND message_id = ? AND actor_id = ?;
`

const selectLifecyclePayloadByTypeGuildMessageActorLatestQuery = `
SELECT payload_json
FROM lifecycle_events
WHERE event_type = ? AND guild_id = ? AND message_id = ? AND actor_id = ?
ORDER BY id DESC LIMIT 1;
`

const selectRolePingByGuildMessageActorQuery = `
SELECT role_id, role_name
FROM role_ping_events
WHERE guild_id = ? AND message_id = ? AND actor_id = ?;
`

const selectMessageRelationshipByMessageIDQuery = `
SELECT referenced_message_id, referenced_channel_id, referenced_guild_id, thread_id, thread_parent_id
FROM messages
WHERE message_id = ?;
`

const selectAttachmentRelationshipByIDQuery = `
SELECT referenced_message_id, referenced_channel_id, referenced_guild_id, thread_id, thread_parent_id
FROM attachments
WHERE attachment_id = ?;
`

const upsertArchivedDiscoveryStateQuery = `
INSERT INTO archived_discovery_state(parent_channel_id, guild_id, last_checked_at, threads_found)
VALUES (?, ?, ?, ?)
ON CONFLICT(parent_channel_id) DO UPDATE SET
	guild_id=excluded.guild_id,
	last_checked_at=excluded.last_checked_at,
	threads_found=excluded.threads_found;
`

const selectArchivedDiscoveryStateByGuildQuery = `
SELECT parent_channel_id, last_checked_at, threads_found
FROM archived_discovery_state
WHERE guild_id = ?;
`
