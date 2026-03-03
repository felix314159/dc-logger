// Package main declares CLI test-only schema/seed SQL fixtures for dc-query.
// Runtime write-path SQL and querysvc read-path SQL live in separate query files.
package main

const cliTestSchemaQuery = `
CREATE TABLE messages (
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

CREATE TABLE attachments (
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

CREATE TABLE id_name_mappings (
	entity_type      TEXT NOT NULL,
	entity_id        TEXT NOT NULL,
	guild_id         TEXT NOT NULL,
	human_name       TEXT NOT NULL,
	normalized_name  TEXT NOT NULL,
	updated_at       TEXT NOT NULL,
	PRIMARY KEY(entity_type, entity_id, guild_id)
);

CREATE TABLE ping_events (
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

CREATE TABLE guild_members (
	guild_id    TEXT NOT NULL,
	user_id     TEXT NOT NULL,
	updated_at  TEXT NOT NULL,
	PRIMARY KEY(guild_id, user_id)
);

CREATE TABLE member_roles (
	guild_id    TEXT NOT NULL,
	user_id     TEXT NOT NULL,
	role_id     TEXT NOT NULL,
	updated_at  TEXT NOT NULL,
	PRIMARY KEY(guild_id, user_id, role_id)
);

CREATE TABLE role_ping_events (
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

CREATE TABLE lifecycle_events (
	id           INTEGER PRIMARY KEY AUTOINCREMENT,
	event_type   TEXT NOT NULL,
	guild_id     TEXT NOT NULL,
	channel_id   TEXT NOT NULL,
	message_id   TEXT NOT NULL,
	actor_id     TEXT NOT NULL,
	occurred_at  TEXT NOT NULL,
	payload_json TEXT NOT NULL
);
`

const cliTestSeedMessagesQuery = `
INSERT INTO messages(
	message_id, guild_id, channel_id, author_id, created_at, content,
	referenced_message_id, referenced_channel_id, referenced_guild_id, thread_id, thread_parent_id,
	edited_at, deleted_at
)
VALUES
	('m1','g1','c1','u1','2026-01-01T00:00:01Z','hello <@u2> incident','m0','c0','g1','','','',''),
	('m2','g1','c2','u2','2026-01-01T00:00:02Z','normal <@&r1>','','','','','','','');
`

const cliTestSeedAttachmentsQuery = `
INSERT INTO attachments(
	attachment_id, message_id, guild_id, channel_id, author_id, created_at,
	filename, content_type, size, url, proxy_url, content_text,
	referenced_message_id, referenced_channel_id, referenced_guild_id, thread_id, thread_parent_id,
	updated_at, deleted_at
)
VALUES
	('a1','m3','g1','c1','u1','2026-01-01T00:00:03Z','note.txt','text/plain',100,'https://x/a1','https://x/p1','incident details','m1','c1','g1','th1','c1','2026-01-01T00:00:03Z',''),
	('a2','m4','g1','c2','u2','2026-01-01T00:00:04Z','incident.png','image/png',100,'https://x/a2','https://x/p2','','','','','','','2026-01-01T00:00:04Z','');
`

const cliTestSeedIDNameMappingsQuery = `
INSERT INTO id_name_mappings(entity_type, entity_id, guild_id, human_name, normalized_name, updated_at)
VALUES
	('guild','g1','g1','Lorem','lorem','2026-01-01T00:00:05Z'),
	('channel','c1','g1','general','general','2026-01-01T00:00:05Z'),
	('user','u1','g1','ipsum','ipsum','2026-01-01T00:00:05Z'),
	('user','u2','g1','dolor','dolor','2026-01-01T00:00:05Z'),
	('role','r1','g1','Consectetur','consectetur','2026-01-01T00:00:05Z');
`

const cliTestSeedPingEventsQuery = `
INSERT INTO ping_events(guild_id, channel_id, message_id, actor_id, target_id, target_name, occurred_at)
VALUES
	('g1','c1','m1','u1','u2','dolor','2026-01-01T00:00:11Z');
`

const cliTestSeedGuildMembersQuery = `
INSERT INTO guild_members(guild_id, user_id, updated_at)
VALUES
	('g1','u1','2026-01-01T00:00:12Z'),
	('g1','u2','2026-01-01T00:00:13Z');
`

const cliTestSeedMemberRolesQuery = `
INSERT INTO member_roles(guild_id, user_id, role_id, updated_at)
VALUES
	('g1','u1','r1','2026-01-01T00:00:12Z');
`

const cliTestSeedRolePingEventsQuery = `
INSERT INTO role_ping_events(guild_id, channel_id, message_id, actor_id, role_id, role_name, occurred_at)
VALUES
	('g1','c2','m2','u2','r1','Consectetur','2026-01-01T00:00:13Z');
`

const cliTestSeedLifecycleEventsQuery = `
INSERT INTO lifecycle_events(event_type, guild_id, channel_id, message_id, actor_id, occurred_at, payload_json)
VALUES
	('message_sent','g1','c1','m1','u1','2026-01-01T00:00:01Z','{"content":"hello"}'),
	('message_deleted','g1','c2','m2','u2','2026-01-01T00:00:15Z','{"reason":"cleanup"}');
`
