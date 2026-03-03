// Package main provides helpers for maintaining current guild membership snapshots.
package main

import (
	"database/sql"
	"strings"
)

func upsertGuildMemberRow(stmt *sql.Stmt, guildID, userID, now string) error {
	if stmt == nil {
		return nil
	}
	guildID = strings.TrimSpace(guildID)
	userID = strings.TrimSpace(userID)
	if guildID == "" || userID == "" {
		return nil
	}
	_, err := stmt.Exec(guildID, userID, now)
	return err
}

func deleteGuildMemberRow(stmt *sql.Stmt, guildID, userID string) error {
	if stmt == nil {
		return nil
	}
	guildID = strings.TrimSpace(guildID)
	userID = strings.TrimSpace(userID)
	if guildID == "" || userID == "" {
		return nil
	}
	_, err := stmt.Exec(guildID, userID)
	return err
}
