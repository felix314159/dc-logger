// Package main provides helpers for guild role metadata and member-role snapshots.
package main

import (
	"fmt"
	"log"
	"sort"
	"strings"

	"github.com/bwmarrin/discordgo"
)

var guildMembersFetcher = func(
	s *discordgo.Session,
	guildID, after string,
	limit int,
) ([]*discordgo.Member, error) {
	if s == nil || strings.TrimSpace(guildID) == "" || limit <= 0 {
		return nil, fmt.Errorf("invalid guild member fetch input")
	}
	return s.GuildMembers(guildID, after, limit)
}

func roleNameFromState(s *discordgo.Session, guildID, roleID string) string {
	if s == nil || s.State == nil || strings.TrimSpace(guildID) == "" || strings.TrimSpace(roleID) == "" {
		return ""
	}
	g, err := s.State.Guild(guildID)
	if err != nil || g == nil {
		return ""
	}
	for _, r := range g.Roles {
		if r == nil || r.ID != roleID {
			continue
		}
		return strings.TrimSpace(r.Name)
	}
	return ""
}

func upsertRolesFromGuild(stmts *preparedStatements, g *discordgo.Guild, now string) {
	if stmts == nil || g == nil || strings.TrimSpace(g.ID) == "" {
		return
	}
	for _, r := range g.Roles {
		if err := upsertRoleRow(stmts.upsertRole, stmts.upsertIDNameMapping, g.ID, r, now); err != nil {
			log.Printf("role upsert failed (guild=%s role=%s): %v", g.ID, roleIDOf(r), err)
		}
	}
}

func syncMemberRolesFromGuild(stmts *preparedStatements, g *discordgo.Guild, now string) {
	if stmts == nil || g == nil || strings.TrimSpace(g.ID) == "" {
		return
	}
	for _, m := range g.Members {
		if m == nil || m.User == nil || strings.TrimSpace(m.User.ID) == "" || m.User.Bot {
			continue
		}
		if err := upsertGuildMemberRow(stmts.upsertGuildMember, g.ID, m.User.ID, now); err != nil {
			log.Printf("guild member upsert failed (guild=%s user=%s): %v", g.ID, m.User.ID, err)
		}
		if err := setMemberRolesSnapshot(stmts, g.ID, m.User.ID, m.Roles, now); err != nil {
			log.Printf("member role snapshot failed (guild=%s user=%s): %v", g.ID, m.User.ID, err)
		}
	}
}

func syncMemberRolesFromGuildAPI(s *discordgo.Session, stmts *preparedStatements, guildID, now string) error {
	if stmts == nil || strings.TrimSpace(guildID) == "" {
		return nil
	}

	const pageSize = 1000
	after := ""
	totalMembers := 0
	totalRoles := 0
	page := 0

	for {
		page++
		members, err := guildMembersFetcher(s, guildID, after, pageSize)
		if err != nil {
			return fmt.Errorf("GuildMembers(after=%s,page=%d): %w", after, page, err)
		}
		if len(members) == 0 {
			break
		}
		nextAfter := after

		for _, m := range members {
			if m == nil || m.User == nil || strings.TrimSpace(m.User.ID) == "" {
				continue
			}
			nextAfter = m.User.ID
			if m.User.Bot {
				continue
			}
			if err := upsertUserRow(stmts.upsertUser, stmts.upsertIDNameMapping, guildID, m.User, now); err != nil {
				log.Printf("user upsert failed (guild=%s user=%s): %v", guildID, m.User.ID, err)
			}
			if err := upsertGuildMemberRow(stmts.upsertGuildMember, guildID, m.User.ID, now); err != nil {
				log.Printf("guild member upsert failed (guild=%s user=%s): %v", guildID, m.User.ID, err)
			}
			if err := setMemberRolesSnapshot(stmts, guildID, m.User.ID, m.Roles, now); err != nil {
				log.Printf("member role snapshot failed (guild=%s user=%s): %v", guildID, m.User.ID, err)
			}
			totalMembers++
			totalRoles += len(normalizeUniqueIDs(m.Roles))
		}

		log.Printf(
			"guild member-role snapshot progress guild=%s page=%d fetched=%d members_synced=%d role_assignments_synced=%d",
			guildID,
			page,
			len(members),
			totalMembers,
			totalRoles,
		)

		if nextAfter == after {
			log.Printf("guild member-role snapshot stopped early guild=%s page=%d reason=no-cursor-advance", guildID, page)
			break
		}
		after = nextAfter
	}

	log.Printf(
		"guild member-role snapshot complete guild=%s members_synced=%d role_assignments_synced=%d pages=%d",
		guildID,
		totalMembers,
		totalRoles,
		page,
	)
	return nil
}

func setMemberRolesSnapshot(stmts *preparedStatements, guildID, userID string, roleIDs []string, now string) error {
	if stmts == nil || stmts.deleteMemberRolesByUser == nil || stmts.upsertMemberRole == nil {
		return nil
	}
	guildID = strings.TrimSpace(guildID)
	userID = strings.TrimSpace(userID)
	if guildID == "" || userID == "" {
		return nil
	}
	if _, err := stmts.deleteMemberRolesByUser.Exec(guildID, userID); err != nil {
		return err
	}
	for _, roleID := range normalizeUniqueIDs(roleIDs) {
		if _, err := stmts.upsertMemberRole.Exec(guildID, userID, roleID, now); err != nil {
			return err
		}
	}
	return nil
}

func roleDiff(beforeRoleIDs, afterRoleIDs []string) (added, removed []string) {
	before := make(map[string]struct{}, len(beforeRoleIDs))
	for _, roleID := range normalizeUniqueIDs(beforeRoleIDs) {
		before[roleID] = struct{}{}
	}
	after := make(map[string]struct{}, len(afterRoleIDs))
	for _, roleID := range normalizeUniqueIDs(afterRoleIDs) {
		after[roleID] = struct{}{}
		if _, exists := before[roleID]; !exists {
			added = append(added, roleID)
		}
	}
	for roleID := range before {
		if _, exists := after[roleID]; !exists {
			removed = append(removed, roleID)
		}
	}
	sort.Strings(added)
	sort.Strings(removed)
	return added, removed
}

func normalizeUniqueIDs(ids []string) []string {
	if len(ids) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(ids))
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func roleIDOf(r *discordgo.Role) string {
	if r == nil {
		return ""
	}
	return r.ID
}
