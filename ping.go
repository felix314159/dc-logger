// Package main records structured @mention ("ping") events.
package main

import (
	"log"
	"strings"

	"github.com/bwmarrin/discordgo"
)

func recordUserPings(
	stmts *preparedStatements,
	guildID, channelID, messageID, actorID, occurredAt string,
	mentions []*discordgo.User,
) {
	if stmts == nil || len(mentions) == 0 || strings.TrimSpace(guildID) == "" || strings.TrimSpace(actorID) == "" {
		return
	}

	seen := make(map[string]struct{}, len(mentions))
	for _, u := range mentions {
		if u == nil || strings.TrimSpace(u.ID) == "" {
			continue
		}
		if _, exists := seen[u.ID]; exists {
			continue
		}
		seen[u.ID] = struct{}{}

		if err := upsertUserRow(stmts.upsertUser, stmts.upsertIDNameMapping, guildID, u, occurredAt); err != nil {
			log.Printf("mentioned-user upsert failed (author=%s): %v", u.ID, err)
		}

		if _, err := stmts.upsertPingEvent.Exec(
			guildID,
			channelID,
			messageID,
			actorID,
			u.ID,
			strings.TrimSpace(u.Username),
			occurredAt,
		); err != nil {
			log.Printf("ping event upsert failed (msg=%s actor=%s target=%s): %v", messageID, actorID, u.ID, err)
			continue
		}

		payload := map[string]any{
			"target_user_id":  u.ID,
			"target_username": strings.TrimSpace(u.Username),
		}
		if globalName := strings.TrimSpace(u.GlobalName); globalName != "" {
			payload["target_global_name"] = globalName
		}
		if err := recordLifecycleEvent(
			stmts.insertEvent,
			eventUserPinged,
			guildID,
			channelID,
			messageID,
			actorID,
			occurredAt,
			payload,
		); err != nil {
			log.Printf("event insert failed (type=%s msg=%s actor=%s target=%s): %v", eventUserPinged, messageID, actorID, u.ID, err)
			continue
		}
		logTrackedEvent(eventUserPinged, guildID, channelID, messageID, actorID, payload)
	}
}

func recordRolePings(
	stmts *preparedStatements,
	s *discordgo.Session,
	guildID, channelID, messageID, actorID, occurredAt string,
	mentionRoleIDs []string,
) {
	if stmts == nil || len(mentionRoleIDs) == 0 || strings.TrimSpace(guildID) == "" || strings.TrimSpace(actorID) == "" {
		return
	}

	seen := make(map[string]struct{}, len(mentionRoleIDs))
	for _, roleID := range mentionRoleIDs {
		roleID = strings.TrimSpace(roleID)
		if roleID == "" {
			continue
		}
		if _, exists := seen[roleID]; exists {
			continue
		}
		seen[roleID] = struct{}{}

		roleName := strings.TrimSpace(roleNameFromState(s, guildID, roleID))
		if roleName == "" {
			roleName = roleID
		}

		if err := upsertRoleName(stmts.upsertRole, stmts.upsertIDNameMapping, guildID, roleID, roleName, occurredAt); err != nil {
			log.Printf("mentioned-role upsert failed (role=%s): %v", roleID, err)
		}

		if _, err := stmts.upsertRolePingEvent.Exec(
			guildID,
			channelID,
			messageID,
			actorID,
			roleID,
			roleName,
			occurredAt,
		); err != nil {
			log.Printf("role ping event upsert failed (msg=%s actor=%s role=%s): %v", messageID, actorID, roleID, err)
			continue
		}

		payload := map[string]any{
			"target_role_id":   roleID,
			"target_role_name": roleName,
		}
		if err := recordLifecycleEvent(
			stmts.insertEvent,
			eventRolePinged,
			guildID,
			channelID,
			messageID,
			actorID,
			occurredAt,
			payload,
		); err != nil {
			log.Printf("event insert failed (type=%s msg=%s actor=%s role=%s): %v", eventRolePinged, messageID, actorID, roleID, err)
			continue
		}
		logTrackedEvent(eventRolePinged, guildID, channelID, messageID, actorID, payload)
	}
}
