// Package querysvc declares read-only retrieval SQL used by the query service.
// Runtime write-path SQL (schema/upserts/events) lives in the root queries.go.
package querysvc

var readOnlyPragmas = []string{
	`PRAGMA query_only = ON;`,
	`PRAGMA busy_timeout = 5000;`,
}

const (
	messageTimeSinceFilterQuery    = " AND m.created_at >= ?"
	messageTimeUntilFilterQuery    = " AND m.created_at <= ?"
	messageBeforeTimeFilterQuery   = " AND m.created_at < ?"
	messageBeforeTimeIDFilterQuery = " AND (m.created_at < ? OR (m.created_at = ? AND m.message_id < ?))"

	attachmentTimeSinceFilterQuery    = " AND a.created_at >= ?"
	attachmentTimeUntilFilterQuery    = " AND a.created_at <= ?"
	attachmentBeforeTimeFilterQuery   = " AND a.created_at < ?"
	attachmentBeforeTimeIDFilterQuery = " AND (a.created_at < ? OR (a.created_at = ? AND a.attachment_id < ?))"

	searchFTSTimeSinceFilterQuery    = " AND f.created_at >= ?"
	searchFTSTimeUntilFilterQuery    = " AND f.created_at <= ?"
	searchFTSBeforeTimeFilterQuery   = " AND f.created_at < ?"
	searchFTSBeforeTimeIDFilterQuery = " AND (f.created_at < ? OR (f.created_at = ? AND CASE WHEN f.attachment_id != '' THEN f.attachment_id ELSE f.message_id END < ?))"

	lifecycleTimeSinceFilterQuery    = " AND le.occurred_at >= ?"
	lifecycleTimeUntilFilterQuery    = " AND le.occurred_at <= ?"
	lifecycleBeforeTimeFilterQuery   = " AND le.occurred_at < ?"
	lifecycleBeforeTimeIDFilterQuery = " AND (le.occurred_at < ? OR (le.occurred_at = ? AND le.id < ?))"
	lifecycleTypeFilterQuery         = " AND le.event_type = ?"
	lifecycleActorFilterQuery        = " AND le.actor_id = ?"
	lifecycleChannelFilterQuery      = " AND le.channel_id = ?"
	lifecycleOrderByLimitQuery       = " ORDER BY le.occurred_at DESC, le.id DESC LIMIT ?;"
	lifecycleEventSourceValue        = "lifecycle_event"
	lifecycleEventAuthorNameDefault  = "''"
	lifecycleEventAuthorNameMapped   = "COALESCE(u_map.human_name, '')"
)

const recentPingsByUserQuery = `
SELECT
	'ping' AS source,
	pe.message_id,
	'' AS attachment_id,
	pe.guild_id,
	pe.channel_id,
	pe.actor_id AS author_id,
	'' AS author_name,
	pe.target_id,
	pe.target_name,
	'' AS role_id,
	'' AS role_name,
	pe.occurred_at AS created_at,
	COALESCE(m.content, '') AS content,
	'' AS filename,
	'' AS content_type,
	'' AS url,
	'' AS referenced_message_id,
	'' AS referenced_channel_id,
	'' AS referenced_guild_id,
	'' AS thread_id,
	'' AS thread_parent_id
FROM ping_events pe
LEFT JOIN messages m ON m.message_id = pe.message_id
WHERE pe.guild_id = ? AND pe.actor_id = ?
ORDER BY pe.occurred_at DESC, pe.id DESC
LIMIT ?;
`

const searchMessagesFTSQuery = `
SELECT
	f.source,
	f.message_id,
	f.attachment_id,
	f.guild_id,
	f.channel_id,
	f.author_id,
	'' AS author_name,
	'' AS target_id,
	'' AS target_name,
	'' AS role_id,
	'' AS role_name,
	f.created_at,
	f.content,
	f.filename,
	f.content_type,
	f.url,
	COALESCE(a_rel.referenced_message_id, m_rel.referenced_message_id, '') AS referenced_message_id,
	COALESCE(a_rel.referenced_channel_id, m_rel.referenced_channel_id, '') AS referenced_channel_id,
	COALESCE(a_rel.referenced_guild_id, m_rel.referenced_guild_id, '') AS referenced_guild_id,
	COALESCE(a_rel.thread_id, m_rel.thread_id, '') AS thread_id,
	COALESCE(a_rel.thread_parent_id, m_rel.thread_parent_id, '') AS thread_parent_id
FROM message_search_fts f
LEFT JOIN messages m_rel
	ON f.source = 'message'
	AND m_rel.message_id = f.message_id
LEFT JOIN attachments a_rel
	ON f.source = 'attachment'
	AND a_rel.attachment_id = f.attachment_id
WHERE f.guild_id = ?
  AND message_search_fts MATCH ?
%s%s%s
ORDER BY f.created_at DESC, f.message_id DESC, f.attachment_id DESC
LIMIT ?;
`

const topicActivitySummaryFTSQueryTemplate = `
WITH matched AS (
	SELECT
		f.guild_id,
		f.author_id,
		f.created_at,
		f.message_id,
		f.attachment_id,
		f.content
	FROM message_search_fts f
	WHERE f.guild_id = ?
	  AND message_search_fts MATCH ?
%s%s
),
ranked AS (
	SELECT
		m.guild_id,
		m.author_id,
		m.created_at,
		m.message_id,
		m.attachment_id,
		m.content,
		COUNT(*) OVER (PARTITION BY m.author_id) AS hit_count,
		ROW_NUMBER() OVER (
			PARTITION BY m.author_id
			ORDER BY m.created_at DESC, m.message_id DESC, m.attachment_id DESC
		) AS rn
	FROM matched m
)
SELECT
	'topic_summary' AS source,
	r.message_id,
	r.attachment_id,
	r.guild_id,
	'' AS channel_id,
	r.author_id,
	'' AS author_name,
	'' AS target_id,
	'' AS target_name,
	'' AS role_id,
	'' AS role_name,
	r.created_at,
	r.content,
	'' AS filename,
	'' AS content_type,
	'' AS url,
	'' AS referenced_message_id,
	'' AS referenced_channel_id,
	'' AS referenced_guild_id,
	'' AS thread_id,
	'' AS thread_parent_id,
	'' AS event_id,
	'' AS event_type,
	'{"hit_count":' || r.hit_count || '}' AS payload_json
FROM ranked r
WHERE r.rn = 1
ORDER BY r.created_at DESC, r.author_id ASC
LIMIT ?;
`

const topicActivitySummaryLikeQueryTemplate = `
WITH matched AS (
	SELECT
		m.guild_id,
		m.author_id,
		m.created_at,
		m.message_id,
		'' AS attachment_id,
		m.content
	FROM messages m
	WHERE m.deleted_at = ''
	  AND m.guild_id = ?
	  AND m.content LIKE ? ESCAPE '\'
%s%s

	UNION ALL

	SELECT
		a.guild_id,
		a.author_id,
		a.created_at,
		a.message_id,
		a.attachment_id,
		CASE
			WHEN a.content_text != '' THEN a.content_text
			ELSE '[attachment] ' || a.filename
		END AS content
	FROM attachments a
	WHERE a.deleted_at = ''
	  AND a.guild_id = ?
	  AND (
		(a.content_text != '' AND a.content_text LIKE ? ESCAPE '\')
		OR (a.content_text = '' AND a.filename LIKE ? ESCAPE '\')
	  )
%s%s
),
ranked AS (
	SELECT
		m.guild_id,
		m.author_id,
		m.created_at,
		m.message_id,
		m.attachment_id,
		m.content,
		COUNT(*) OVER (PARTITION BY m.author_id) AS hit_count,
		ROW_NUMBER() OVER (
			PARTITION BY m.author_id
			ORDER BY m.created_at DESC, m.message_id DESC, m.attachment_id DESC
		) AS rn
	FROM matched m
)
SELECT
	'topic_summary' AS source,
	r.message_id,
	r.attachment_id,
	r.guild_id,
	'' AS channel_id,
	r.author_id,
	'' AS author_name,
	'' AS target_id,
	'' AS target_name,
	'' AS role_id,
	'' AS role_name,
	r.created_at,
	r.content,
	'' AS filename,
	'' AS content_type,
	'' AS url,
	'' AS referenced_message_id,
	'' AS referenced_channel_id,
	'' AS referenced_guild_id,
	'' AS thread_id,
	'' AS thread_parent_id,
	'' AS event_id,
	'' AS event_type,
	'{"hit_count":' || r.hit_count || '}' AS payload_json
FROM ranked r
WHERE r.rn = 1
ORDER BY r.created_at DESC, r.author_id ASC
LIMIT ?;
`

const recentLifecycleEventsQueryTemplate = `
SELECT
	'%s' AS source,
	le.message_id,
	le.guild_id,
	le.channel_id,
	le.actor_id,
	%s AS author_name,
	le.occurred_at,
	le.id,
	le.event_type,
	le.payload_json
FROM lifecycle_events le
%s
WHERE le.guild_id = ?
`

const recentLifecycleEventsUserJoinQuery = `
LEFT JOIN id_name_mappings u_map
	ON u_map.entity_type = 'user'
	AND u_map.guild_id = le.guild_id
	AND u_map.entity_id = le.actor_id
`

const recentPingsTargetingUserDirectBranchQuery = `
SELECT
	'ping' AS source,
	pe.message_id,
	'' AS attachment_id,
	pe.guild_id,
	pe.channel_id,
	pe.actor_id AS author_id,
	'' AS author_name,
	pe.target_id,
	pe.target_name,
	'' AS role_id,
	'' AS role_name,
	pe.occurred_at AS created_at,
	COALESCE(m.content, '') AS content,
	'' AS filename,
	'' AS content_type,
	'' AS url,
	'' AS referenced_message_id,
	'' AS referenced_channel_id,
	'' AS referenced_guild_id,
	'' AS thread_id,
	'' AS thread_parent_id
FROM ping_events pe
LEFT JOIN messages m ON m.message_id = pe.message_id
WHERE pe.guild_id = ? AND pe.target_id = ?
`

const recentPingsTargetingUserRoleBranchQuery = `
SELECT
	'ping' AS source,
	rpe.message_id,
	'' AS attachment_id,
	rpe.guild_id,
	rpe.channel_id,
	rpe.actor_id AS author_id,
	'' AS author_name,
	mr.user_id AS target_id,
	'' AS target_name,
	rpe.role_id,
	rpe.role_name,
	rpe.occurred_at AS created_at,
	COALESCE(m.content, '') AS content,
	'' AS filename,
	'' AS content_type,
	'' AS url,
	'' AS referenced_message_id,
	'' AS referenced_channel_id,
	'' AS referenced_guild_id,
	'' AS thread_id,
	'' AS thread_parent_id
FROM role_ping_events rpe
JOIN member_roles mr
	ON mr.guild_id = rpe.guild_id
	AND mr.role_id = rpe.role_id
LEFT JOIN messages m ON m.message_id = rpe.message_id
WHERE rpe.guild_id = ? AND mr.user_id = ?
`

const recentPingsTargetingUserCombinedQueryTemplate = `
WITH combined AS (
%s
)
SELECT
	source,
	message_id,
	attachment_id,
	guild_id,
	channel_id,
	author_id,
	author_name,
	target_id,
	target_name,
	role_id,
	role_name,
	created_at,
	content,
	filename,
	content_type,
	url,
	referenced_message_id,
	referenced_channel_id,
	referenced_guild_id,
	thread_id,
	thread_parent_id
FROM combined
ORDER BY created_at DESC, message_id DESC
LIMIT ?;
`

const unansweredPingsTargetingUserDirectBranchQueryTemplate = `
SELECT
	'ping_unanswered' AS source,
	pe.message_id,
	'' AS attachment_id,
	pe.guild_id,
	pe.channel_id,
	pe.actor_id AS author_id,
	'' AS author_name,
	pe.target_id,
	pe.target_name,
	'' AS role_id,
	'' AS role_name,
	pe.occurred_at AS created_at,
	COALESCE(m.content, '') AS content,
	'' AS filename,
	'' AS content_type,
	'' AS url,
	'' AS referenced_message_id,
	'' AS referenced_channel_id,
	'' AS referenced_guild_id,
	'' AS thread_id,
	'' AS thread_parent_id
FROM ping_events pe
LEFT JOIN messages m ON m.message_id = pe.message_id
WHERE pe.guild_id = ? AND pe.target_id = ?
	AND NOT EXISTS (
		SELECT 1
		FROM messages m_resp
		WHERE m_resp.guild_id = pe.guild_id
			AND m_resp.channel_id = pe.channel_id
			AND m_resp.author_id = pe.target_id
			AND m_resp.deleted_at = ''
			AND (
				m_resp.created_at > pe.occurred_at
				OR (m_resp.created_at = pe.occurred_at AND m_resp.message_id > pe.message_id)
			)
	)
%s
`

const unansweredPingsTargetingUserRoleBranchQueryTemplate = `
SELECT
	'ping_unanswered' AS source,
	rpe.message_id,
	'' AS attachment_id,
	rpe.guild_id,
	rpe.channel_id,
	rpe.actor_id AS author_id,
	'' AS author_name,
	mr.user_id AS target_id,
	'' AS target_name,
	rpe.role_id,
	rpe.role_name,
	rpe.occurred_at AS created_at,
	COALESCE(m.content, '') AS content,
	'' AS filename,
	'' AS content_type,
	'' AS url,
	'' AS referenced_message_id,
	'' AS referenced_channel_id,
	'' AS referenced_guild_id,
	'' AS thread_id,
	'' AS thread_parent_id
FROM role_ping_events rpe
JOIN member_roles mr
	ON mr.guild_id = rpe.guild_id
	AND mr.role_id = rpe.role_id
LEFT JOIN messages m ON m.message_id = rpe.message_id
WHERE rpe.guild_id = ? AND mr.user_id = ?
	AND NOT EXISTS (
		SELECT 1
		FROM messages m_resp
		WHERE m_resp.guild_id = rpe.guild_id
			AND m_resp.channel_id = rpe.channel_id
			AND m_resp.author_id = mr.user_id
			AND m_resp.deleted_at = ''
			AND (
				m_resp.created_at > rpe.occurred_at
				OR (m_resp.created_at = rpe.occurred_at AND m_resp.message_id > rpe.message_id)
			)
	)
%s
`

const rolesOfUserQueryTemplate = `
SELECT
	'member_role' AS source,
	'' AS message_id,
	'' AS attachment_id,
	mr.guild_id,
	'' AS channel_id,
	mr.user_id AS author_id,
	%s AS author_name,
	'' AS target_id,
	'' AS target_name,
	mr.role_id,
	%s AS role_name,
	mr.updated_at AS created_at,
	'' AS content,
	'' AS filename,
	'' AS content_type,
	'' AS url,
	'' AS referenced_message_id,
	'' AS referenced_channel_id,
	'' AS referenced_guild_id,
	'' AS thread_id,
	'' AS thread_parent_id
FROM member_roles mr
%s
%s
%s
WHERE mr.guild_id = ? AND mr.user_id = ?
ORDER BY mr.updated_at DESC, mr.role_id ASC
LIMIT ?;
`

const usersWithRoleQueryTemplate = `
SELECT
	'member_role' AS source,
	'' AS message_id,
	'' AS attachment_id,
	mr.guild_id,
	'' AS channel_id,
	mr.user_id AS author_id,
	%s AS author_name,
	'' AS target_id,
	'' AS target_name,
	mr.role_id,
	%s AS role_name,
	mr.updated_at AS created_at,
	'' AS content,
	'' AS filename,
	'' AS content_type,
	'' AS url,
	'' AS referenced_message_id,
	'' AS referenced_channel_id,
	'' AS referenced_guild_id,
	'' AS thread_id,
	'' AS thread_parent_id
FROM member_roles mr
%s
%s
%s
WHERE mr.guild_id = ? AND mr.role_id = ?
ORDER BY mr.updated_at DESC, mr.user_id ASC
LIMIT ?;
`

const listGuildMembersQueryTemplate = `
SELECT
	'member' AS source,
	'' AS message_id,
	'' AS attachment_id,
	gm.guild_id,
	'' AS channel_id,
	gm.user_id AS author_id,
	%s AS author_name,
	'' AS target_id,
	'' AS target_name,
	'' AS role_id,
	'' AS role_name,
	gm.updated_at AS created_at,
	'' AS content,
	'' AS filename,
	'' AS content_type,
	'' AS url,
	'' AS referenced_message_id,
	'' AS referenced_channel_id,
	'' AS referenced_guild_id,
	'' AS thread_id,
	'' AS thread_parent_id
FROM guild_members gm
%s
WHERE gm.guild_id = ?
ORDER BY gm.updated_at DESC, gm.user_id ASC
LIMIT ?;
`

const mostCommonGuildIDFromMessagesQuery = `
SELECT guild_id
FROM messages
WHERE deleted_at = ''
GROUP BY guild_id
ORDER BY COUNT(*) DESC, guild_id ASC
LIMIT 1;
`

const mostCommonGuildIDWithAttachmentsQuery = `
WITH counts AS (
	SELECT guild_id, COUNT(*) AS c
	FROM messages
	WHERE deleted_at = ''
	GROUP BY guild_id

	UNION ALL

	SELECT guild_id, COUNT(*) AS c
	FROM attachments
	WHERE deleted_at = ''
	GROUP BY guild_id
)
SELECT guild_id
FROM counts
GROUP BY guild_id
ORDER BY SUM(c) DESC, guild_id ASC
LIMIT 1;
`

const resolveEntityIDByNameGuildQuery = `
SELECT entity_id
FROM id_name_mappings
WHERE entity_type = ? AND normalized_name = ?
ORDER BY updated_at DESC, entity_id ASC
LIMIT 1;
`

const resolveEntityIDByNameScopedQuery = `
SELECT entity_id
FROM id_name_mappings
WHERE entity_type = ? AND guild_id = ? AND normalized_name = ?
ORDER BY updated_at DESC, entity_id ASC
LIMIT 1;
`

const resolveChannelIDByNameFallbackQuery = `
SELECT channel_id
FROM channels
WHERE guild_id = ? AND deleted_at = '' AND lower(name) = ?
ORDER BY updated_at DESC, channel_id ASC
LIMIT 1;
`

const resolveAuthorIDByNameFallbackQuery = `
SELECT u.author_id
FROM users u
WHERE lower(CASE WHEN u.global_name != '' THEN u.global_name ELSE u.username END) = ?
  AND EXISTS (
  	SELECT 1
  	FROM messages m
  	WHERE m.guild_id = ? AND m.author_id = u.author_id
  )
ORDER BY u.updated_at DESC, u.author_id ASC
LIMIT 1;
`

const resolveRoleIDByNameFallbackQuery = `
SELECT role_id
FROM roles
WHERE guild_id = ? AND deleted_at = '' AND lower(name) = ?
ORDER BY updated_at DESC, role_id ASC
LIMIT 1;
`

const combinedMessagesOnlyQueryTemplate = `
SELECT
	'message' AS source,
	m.message_id,
	'' AS attachment_id,
	m.guild_id,
	m.channel_id,
	m.author_id,
	'' AS author_name,
	'' AS target_id,
	'' AS target_name,
	'' AS role_id,
	'' AS role_name,
	m.created_at,
	m.content,
	'' AS filename,
	'' AS content_type,
	'' AS url,
	m.referenced_message_id,
	m.referenced_channel_id,
	m.referenced_guild_id,
	m.thread_id,
	m.thread_parent_id
FROM messages m
WHERE m.deleted_at = '' AND %s
ORDER BY m.created_at DESC, m.message_id DESC
LIMIT ?;
`

const combinedMessagesAndAttachmentsQueryTemplate = `
WITH combined AS (
	SELECT
		'message' AS source,
		m.message_id AS message_id,
		'' AS attachment_id,
		m.guild_id,
		m.channel_id,
		m.author_id,
		'' AS author_name,
		'' AS target_id,
		'' AS target_name,
		'' AS role_id,
		'' AS role_name,
		m.created_at,
		m.content,
		'' AS filename,
		'' AS content_type,
		'' AS url,
		m.referenced_message_id,
		m.referenced_channel_id,
		m.referenced_guild_id,
		m.thread_id,
		m.thread_parent_id
	FROM messages m
	WHERE m.deleted_at = '' AND %s

	UNION ALL

	SELECT
		'attachment' AS source,
		a.message_id AS message_id,
		a.attachment_id AS attachment_id,
		a.guild_id,
		a.channel_id,
		a.author_id,
		'' AS author_name,
		'' AS target_id,
		'' AS target_name,
		'' AS role_id,
		'' AS role_name,
		a.created_at,
		CASE
			WHEN a.content_text != '' THEN a.content_text
			ELSE '[attachment] ' || a.filename
		END AS content,
		a.filename,
		a.content_type,
		a.url,
		a.referenced_message_id,
		a.referenced_channel_id,
		a.referenced_guild_id,
		a.thread_id,
		a.thread_parent_id
	FROM attachments a
	WHERE a.deleted_at = '' AND %s
)
SELECT
	source,
	message_id,
	attachment_id,
	guild_id,
	channel_id,
	author_id,
	author_name,
	target_id,
	target_name,
	role_id,
	role_name,
	created_at,
	content,
	filename,
	content_type,
	url,
	referenced_message_id,
	referenced_channel_id,
	referenced_guild_id,
	thread_id,
	thread_parent_id
FROM combined
ORDER BY created_at DESC, message_id DESC, attachment_id DESC
LIMIT ?;
`

const tableExistsQuery = `
SELECT COUNT(*)
FROM sqlite_master
WHERE type='table' AND name = ?;
`

const lookupNamesByIDQueryTemplate = `
SELECT entity_id, human_name
FROM id_name_mappings
WHERE entity_type = ? AND guild_id = ? AND entity_id IN (%s);
`

const lookupNamesByPingEventsQueryTemplate = `
SELECT target_id, target_name
FROM ping_events
WHERE guild_id = ? AND target_id IN (%s) AND target_name != ''
ORDER BY occurred_at DESC, id DESC;
`

const serverActivitySummaryQueryTemplate = `
SELECT
	COUNT(*) AS message_count,
	COUNT(DISTINCT m.author_id) AS unique_authors,
	COUNT(DISTINCT m.channel_id) AS unique_channels,
	COALESCE(MIN(m.created_at), '') AS first_seen_at,
	COALESCE(MAX(m.created_at), '') AS last_seen_at
FROM messages m
WHERE m.deleted_at = ''
	AND m.guild_id = ?%s%s;
`

const serverActivitySummaryByDayQueryTemplate = `
SELECT
	substr(m.created_at, 1, 10) AS day,
	COUNT(*) AS message_count,
	COUNT(DISTINCT m.author_id) AS unique_authors,
	COUNT(DISTINCT m.channel_id) AS unique_channels,
	COALESCE(MIN(m.created_at), '') AS first_seen_at,
	COALESCE(MAX(m.created_at), '') AS last_seen_at
FROM messages m
WHERE m.deleted_at = ''
	AND m.guild_id = ?%s%s
GROUP BY day
ORDER BY day ASC;
`

const channelActivitySummaryQueryTemplate = `
WITH filtered AS (
	SELECT
		m.guild_id,
		m.channel_id,
		m.created_at
	FROM messages m
	WHERE m.deleted_at = ''
		AND m.guild_id = ?%s%s
),
counts AS (
	SELECT
		f.guild_id,
		f.channel_id,
		COUNT(*) AS message_count,
		MIN(f.created_at) AS first_seen_at,
		MAX(f.created_at) AS last_seen_at
	FROM filtered f
	GROUP BY f.guild_id, f.channel_id
),
totals AS (
	SELECT COUNT(*) AS total_messages FROM filtered
)
SELECT
	c.channel_id,
	%s AS channel_name,
	c.message_count,
	CASE
		WHEN t.total_messages = 0 THEN 0.0
		ELSE (CAST(c.message_count AS REAL) * 100.0 / CAST(t.total_messages AS REAL))
	END AS percentage,
	c.first_seen_at,
	c.last_seen_at
FROM counts c
CROSS JOIN totals t
%s
ORDER BY c.message_count DESC, c.last_seen_at DESC, c.channel_id ASC
LIMIT ?;
`

const authorActivitySummaryQueryTemplate = `
WITH filtered AS (
	SELECT
		m.guild_id,
		m.author_id,
		m.created_at
	FROM messages m
	WHERE m.deleted_at = ''
		AND m.guild_id = ?%s%s
),
counts AS (
	SELECT
		f.guild_id,
		f.author_id,
		COUNT(*) AS message_count,
		MIN(f.created_at) AS first_seen_at,
		MAX(f.created_at) AS last_seen_at
	FROM filtered f
	GROUP BY f.guild_id, f.author_id
),
totals AS (
	SELECT COUNT(*) AS total_messages FROM filtered
)
SELECT
	c.author_id,
	%s AS author_name,
	c.message_count,
	CASE
		WHEN t.total_messages = 0 THEN 0.0
		ELSE (CAST(c.message_count AS REAL) * 100.0 / CAST(t.total_messages AS REAL))
	END AS percentage,
	c.first_seen_at,
	c.last_seen_at
FROM counts c
CROSS JOIN totals t
%s
ORDER BY c.message_count DESC, c.last_seen_at DESC, c.author_id ASC
LIMIT ?;
`

const keywordFrequencyMessagesQueryTemplate = `
SELECT m.content
FROM messages m
WHERE m.deleted_at = ''
	AND m.guild_id = ?%s%s;
`

const keywordFrequencyAttachmentsQueryTemplate = `
SELECT CASE WHEN a.content_text != '' THEN a.content_text ELSE a.filename END AS content
FROM attachments a
WHERE a.deleted_at = ''
	AND a.guild_id = ?%s%s;
`

const testServiceSchemaQuery = `
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

CREATE TABLE channels (
	channel_id  TEXT PRIMARY KEY,
	guild_id    TEXT NOT NULL,
	name        TEXT NOT NULL,
	type        INTEGER NOT NULL,
	parent_id   TEXT NOT NULL,
	is_thread   INTEGER NOT NULL,
	updated_at  TEXT NOT NULL,
	deleted_at  TEXT NOT NULL DEFAULT ''
);

CREATE TABLE users (
	author_id     TEXT PRIMARY KEY,
	username      TEXT NOT NULL,
	global_name   TEXT NOT NULL,
	discriminator TEXT NOT NULL,
	is_bot        INTEGER NOT NULL,
	updated_at    TEXT NOT NULL
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

const testServiceSeedMessagesQuery = `
INSERT INTO messages(
	message_id, guild_id, channel_id, author_id, created_at, content,
	referenced_message_id, referenced_channel_id, referenced_guild_id, thread_id, thread_parent_id,
	edited_at, deleted_at
)
VALUES
 ('m1','g1','c1','u1','2026-01-01T00:00:01Z','hello <@u2>','m0','c0','g1','','','',''),
 ('m2','g1','c2','u2','2026-01-01T00:00:03Z','bye <@!u3> <@&r1>','','','','','','',''),
 ('m3','g1','c1','u1','2026-01-01T00:00:04Z','deleted','','','','','','','2026-01-01T00:01:00Z');
`

const testServiceSeedAttachmentsQuery = `
INSERT INTO attachments(
	attachment_id, message_id, guild_id, channel_id, author_id, created_at,
	filename, content_type, size, url, proxy_url, content_text,
	referenced_message_id, referenced_channel_id, referenced_guild_id, thread_id, thread_parent_id,
	updated_at, deleted_at
)
VALUES
 ('a1','m4','g1','c1','u1','2026-01-01T00:00:02Z','long.txt','text/plain',100,'https://x/a1','https://x/p1','long body','m1','c1','g1','th1','c1','2026-01-01T00:00:02Z',''),
 ('a2','m5','g1','c2','u2','2026-01-01T00:00:05Z','img.png','image/png',100,'https://x/a2','https://x/p2','','','','','','','2026-01-01T00:00:05Z',''),
 ('a3','m6','g1','c1','u1','2026-01-01T00:00:06Z','gone.txt','text/plain',100,'https://x/a3','https://x/p3','gone','','','','','','2026-01-01T00:00:06Z','2026-01-01T00:01:10Z');
`

const testServiceSeedChannelsQuery = `
INSERT INTO channels(channel_id, guild_id, name, type, parent_id, is_thread, updated_at, deleted_at)
VALUES
 ('c1','g1','general',0,'',0,'2026-01-01T00:00:01Z',''),
 ('c2','g1','random',0,'',0,'2026-01-01T00:00:02Z','');
`

const testServiceSeedUsersQuery = `
INSERT INTO users(author_id, username, global_name, discriminator, is_bot, updated_at)
VALUES
 ('u1','ipsum','','0001',0,'2026-01-01T00:00:01Z'),
 ('u2','dolor','','0002',0,'2026-01-01T00:00:02Z');
`

const testServiceSeedNameMappingsQuery = `
INSERT INTO id_name_mappings(entity_type, entity_id, guild_id, human_name, normalized_name, updated_at)
VALUES
 ('guild','g1','g1','Lorem','lorem','2026-01-01T00:00:01Z'),
 ('channel','c1','g1','general','general','2026-01-01T00:00:01Z'),
 ('channel','c2','g1','random','random','2026-01-01T00:00:01Z'),
 ('user','u1','g1','ipsum','ipsum','2026-01-01T00:00:01Z'),
 ('user','u2','g1','dolor','dolor','2026-01-01T00:00:01Z'),
 ('role','r1','g1','Consectetur','consectetur','2026-01-01T00:00:01Z');
`

const testServiceSeedPingEventsQuery = `
INSERT INTO ping_events(guild_id, channel_id, message_id, actor_id, target_id, target_name, occurred_at)
VALUES
 ('g1','c1','m1','u1','u2','dolor','2026-01-01T00:00:07Z'),
 ('g1','c1','m2','u1','u3','amet','2026-01-01T00:00:08Z');
`

const testServiceSeedGuildMembersQuery = `
INSERT INTO guild_members(guild_id, user_id, updated_at)
VALUES
 ('g1','u1','2026-01-01T00:00:01Z'),
 ('g1','u2','2026-01-01T00:00:02Z');
`

const testServiceSeedMemberRolesQuery = `
INSERT INTO member_roles(guild_id, user_id, role_id, updated_at)
VALUES
 ('g1','u1','r1','2026-01-01T00:00:01Z');
`

const testServiceSeedRolePingEventsQuery = `
INSERT INTO role_ping_events(guild_id, channel_id, message_id, actor_id, role_id, role_name, occurred_at)
VALUES
 ('g1','c2','m2','u2','r1','Consectetur','2026-01-01T00:00:09Z');
`

const testServiceSeedLifecycleEventsQuery = `
INSERT INTO lifecycle_events(event_type, guild_id, channel_id, message_id, actor_id, occurred_at, payload_json)
VALUES
 ('message_sent','g1','c1','m1','u1','2026-01-01T00:00:01Z','{"content":"hello"}'),
 ('message_deleted','g1','c2','m2','u2','2026-01-01T00:00:10Z','{"reason":"cleanup"}');
`

const testServiceCreateFTSTableQuery = `
CREATE VIRTUAL TABLE message_search_fts USING fts5(
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

const testServiceSeedFTSRowsQuery = `
INSERT INTO message_search_fts(row_key, source, message_id, attachment_id, guild_id, channel_id, author_id, created_at, content, filename, content_type, url)
VALUES
 ('m:m1','message','m1','','g1','c1','u1','2026-01-01T00:00:01Z','hello incident','','',''),
 ('a:a1','attachment','m4','a1','g1','c1','u1','2026-01-01T00:00:02Z','incident details','note.txt','text/plain','https://x/a1'),
 ('m:m3','message','m3','','g1','c1','u2','2026-01-01T00:00:03Z','tracking lorem-ipsum progress','','','');
`

const testServiceSchemaWithoutAttachmentsAndSeedQuery = `
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
INSERT INTO messages(
	message_id, guild_id, channel_id, author_id, created_at, content,
	referenced_message_id, referenced_channel_id, referenced_guild_id, thread_id, thread_parent_id,
	edited_at, deleted_at
)
VALUES ('m-only','g2','c2','u2','2026-01-01T00:00:01Z','hello','','','','','','','');
`
