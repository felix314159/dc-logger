// Package main defines lifecycle event types and persistence helpers.
package main

import (
	"database/sql"
	"encoding/json"
	"time"
)

type lifecycleEventType string

const (
	eventMessageSent     lifecycleEventType = "message_sent"     // working well
	eventMessageUpdated  lifecycleEventType = "message_updated"  // internal
	eventMessageModified lifecycleEventType = "message_modified" // working well
	eventMessageDeleted  lifecycleEventType = "message_deleted"  // working well
	eventReactionAdded   lifecycleEventType = "reaction_added"   // working well
	eventReactionRemoved lifecycleEventType = "reaction_removed" // working well
	eventThreadCreated   lifecycleEventType = "thread_created"   // working well
	eventThreadDeleted   lifecycleEventType = "thread_deleted"   // working well
	eventThreadRenamed   lifecycleEventType = "thread_renamed"   // working well
	eventChannelCreated  lifecycleEventType = "channel_created"  // working well
	eventChannelDeleted  lifecycleEventType = "channel_deleted"  // working well
	eventChannelRenamed  lifecycleEventType = "channel_renamed"  // working well
	eventGuildRenamed    lifecycleEventType = "guild_renamed"    // working well
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
