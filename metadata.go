// Package main provides guild, user, and channel metadata upsert helpers.
package main

import (
	"database/sql"
	"strings"

	"github.com/bwmarrin/discordgo"
)

const (
	nameMappingEntityGuild   = "guild"
	nameMappingEntityChannel = "channel"
	nameMappingEntityUser    = "user"
	nameMappingEntityRole    = "role"
)

func upsertUserRow(userStmt, mappingStmt *sql.Stmt, guildID string, u *discordgo.User, now string) error {
	if u == nil {
		return nil
	}

	globalName := u.GlobalName
	isBot := 0
	if u.Bot {
		isBot = 1
	}
	if userStmt != nil {
		if _, err := userStmt.Exec(u.ID, u.Username, globalName, u.Discriminator, isBot, now); err != nil {
			return err
		}
	}

	return upsertNameMappingRow(mappingStmt, nameMappingEntityUser, u.ID, guildID, u.Username, now)
}

func upsertChannelFromMessage(
	s *discordgo.Session,
	channelStmt, mappingStmt *sql.Stmt,
	guildID, channelID, now string,
) error {
	// Best effort: try cache/state
	if s != nil && s.State != nil {
		if ch, err := s.State.Channel(channelID); err == nil && ch != nil {
			return upsertChannelRow(channelStmt, mappingStmt, guildID, ch, now)
		}
	}

	// Fallback minimal row if not in cache (still keeps referential integrity for LLM joins)
	// type=-1 indicates unknown; parent_id empty.
	if channelStmt == nil {
		return nil
	}
	_, err := channelStmt.Exec(channelID, guildID, "", -1, "", 0, now, "")
	return err
}

func upsertChannelRow(channelStmt, mappingStmt *sql.Stmt, guildID string, c *discordgo.Channel, now string) error {
	if c == nil || c.ID == "" {
		return nil
	}

	parentID := ""
	// Thread channels have ParentID set.
	if c.ParentID != "" {
		parentID = c.ParentID
	}

	isThread := 0
	if parentID != "" {
		isThread = 1
	}

	name := c.Name // may be empty for some channel types; OK
	if channelStmt != nil {
		if _, err := channelStmt.Exec(c.ID, guildID, name, int(c.Type), parentID, isThread, now, ""); err != nil {
			return err
		}
	}

	return upsertNameMappingRow(mappingStmt, nameMappingEntityChannel, c.ID, guildID, name, now)
}

func upsertGuildFromState(s *discordgo.Session, mappingStmt *sql.Stmt, guildID, now string) error {
	if s == nil || s.State == nil || strings.TrimSpace(guildID) == "" {
		return nil
	}
	g, err := s.State.Guild(guildID)
	if err != nil || g == nil {
		return nil
	}
	return upsertGuildName(mappingStmt, g.ID, g.Name, now)
}

func upsertGuildName(mappingStmt *sql.Stmt, guildID, guildName, now string) error {
	return upsertNameMappingRow(mappingStmt, nameMappingEntityGuild, guildID, guildID, guildName, now)
}

func upsertRoleRow(roleStmt, mappingStmt *sql.Stmt, guildID string, r *discordgo.Role, now string) error {
	if r == nil || strings.TrimSpace(r.ID) == "" {
		return nil
	}
	name := strings.TrimSpace(r.Name)
	if roleStmt != nil {
		if _, err := roleStmt.Exec(r.ID, guildID, name, now, ""); err != nil {
			return err
		}
	}
	return upsertNameMappingRow(mappingStmt, nameMappingEntityRole, r.ID, guildID, name, now)
}

func upsertRoleName(roleStmt, mappingStmt *sql.Stmt, guildID, roleID, roleName, now string) error {
	roleID = strings.TrimSpace(roleID)
	roleName = strings.TrimSpace(roleName)
	if roleID == "" || guildID == "" {
		return nil
	}
	if roleStmt != nil {
		if _, err := roleStmt.Exec(roleID, guildID, roleName, now, ""); err != nil {
			return err
		}
	}
	return upsertNameMappingRow(mappingStmt, nameMappingEntityRole, roleID, guildID, roleName, now)
}

func upsertNameMappingRow(
	stmt *sql.Stmt,
	entityType, entityID, guildID, humanName, now string,
) error {
	if stmt == nil {
		return nil
	}
	entityType = strings.TrimSpace(entityType)
	entityID = strings.TrimSpace(entityID)
	guildID = strings.TrimSpace(guildID)
	humanName = strings.TrimSpace(humanName)
	if entityType == "" || entityID == "" || guildID == "" || humanName == "" {
		return nil
	}
	normalized := normalizeHumanName(humanName)
	if normalized == "" {
		return nil
	}
	_, err := stmt.Exec(entityType, entityID, guildID, humanName, normalized, now)
	return err
}

func normalizeHumanName(v string) string {
	return strings.ToLower(strings.TrimSpace(v))
}
