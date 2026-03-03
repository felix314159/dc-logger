// Package config defines shared environment variable keys and defaults.
package config

const (
	EnvDiscordBotToken                    = "DISCORD_BOT_TOKEN"
	EnvDiscordLogDB                       = "DISCORD_LOG_DB"
	EnvDiscordBackfillMaxPagesPerRun      = "DISCORD_BACKFILL_MAX_PAGES_PER_RUN"
	EnvDiscordBackfillMaxMinutes          = "DISCORD_BACKFILL_MAX_MINUTES"
	EnvDiscordAttachmentTextMaxBytes      = "DISCORD_ATTACHMENT_TEXT_MAX_BYTES"
	EnvDiscordSyncGuildIDs                = "DISCORD_SYNC_GUILD_IDS"
	EnvDiscordArchivedDiscoveryTTLHours   = "DISCORD_ARCHIVED_DISCOVERY_TTL_HOURS"
)

const (
	DefaultLogDBPath                   = "./database/database.db"
	DefaultBackfillMaxPagesPerRun      = 0
	DefaultBackfillMaxMinutes          = 0
	DefaultAttachmentTextMaxBytes      = 1048576
	DefaultSyncGuildIDs                = "*"
	DefaultQueryLimit                  = 10
	DefaultQueryTimeoutSeconds         = 10
	DefaultArchivedDiscoveryTTLHours   = 24
)
