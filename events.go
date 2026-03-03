// Package main defines lifecycle event types and persistence helpers.
package main

import (
	"database/sql"
	"encoding/json"
	"time"
)

type lifecycleEventType string

const (
	eventMessageSent     lifecycleEventType = "message_sent"
	eventMessageSkipped  lifecycleEventType = "message_skipped"
	eventMessageUpdated  lifecycleEventType = "message_updated"
	eventMessageDeleted  lifecycleEventType = "message_deleted"
	eventThreadCreated   lifecycleEventType = "thread_created"
	eventThreadDeleted   lifecycleEventType = "thread_deleted"
	eventThreadRenamed   lifecycleEventType = "thread_renamed"
	eventChannelCreated  lifecycleEventType = "channel_created"
	eventChannelDeleted  lifecycleEventType = "channel_deleted"
	eventChannelRenamed  lifecycleEventType = "channel_renamed"
	eventGuildRenamed    lifecycleEventType = "guild_renamed"
	eventUsernameChanged lifecycleEventType = "username_changed"
	eventUserJoined      lifecycleEventType = "user_joined_server"
	eventUserLeft        lifecycleEventType = "user_left_server"
	eventUserKicked      lifecycleEventType = "user_kicked_from_server"
	eventUserBanned      lifecycleEventType = "user_banned_from_server"
	eventUserPinged      lifecycleEventType = "user_pinged"
	eventRolePinged      lifecycleEventType = "role_pinged"
	eventRoleAssigned    lifecycleEventType = "role_assigned"
	eventRoleRevoked     lifecycleEventType = "role_revoked"
	eventRoleRenamed     lifecycleEventType = "role_renamed"
	eventRoleDeleted     lifecycleEventType = "role_deleted"
)

func recordLifecycleEvent(
	stmt *sql.Stmt,
	eventType lifecycleEventType,
	guildID, channelID, messageID, actorID, occurredAt string,
	payload any,
) error {
	if occurredAt == "" {
		occurredAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	if payload == nil {
		payload = map[string]any{}
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = stmt.Exec(
		string(eventType),
		guildID,
		channelID,
		messageID,
		actorID,
		occurredAt,
		string(payloadJSON),
	)
	return err
}
