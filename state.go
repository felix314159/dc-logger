// Package main manages per-channel high-water-mark state reads and updates.
package main

import (
	"database/sql"
	"time"
)

func getLastMessageID(db *sql.DB, channelID string) (string, error) {
	var last string
	err := db.QueryRow(getLastMessageIDByChannelQuery, channelID).Scan(&last)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return last, err
}

func getLatestStoredMessageID(db *sql.DB, channelID string) (string, error) {
	var last string
	err := db.QueryRow(getLatestStoredMessageIDByChannelQuery, channelID).Scan(&last)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return last, err
}

func updateHighWaterMark(db *sql.DB, upsertState *sql.Stmt, guildID, channelID, messageID string) error {
	current, err := getLastMessageID(db, channelID)
	if err != nil {
		return err
	}
	if current == "" || snowflakeGreater(messageID, current) {
		return setHighWaterMark(upsertState, guildID, channelID, messageID)
	}
	return nil
}

func setHighWaterMark(upsertState *sql.Stmt, guildID, channelID, messageID string) error {
	_, err := upsertState.Exec(channelID, guildID, messageID, time.Now().UTC().Format(time.RFC3339Nano))
	return err
}
