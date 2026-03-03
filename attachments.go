// Package main handles attachment metadata persistence and text extraction.
package main

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"example.org/dc-logger/internal/config"
	"github.com/bwmarrin/discordgo"
)

var (
	attachmentHTTPClient  = &http.Client{Timeout: 20 * time.Second}
	attachmentTextFetcher = fetchAttachmentText

	attachmentTextMaxBytesOnce sync.Once
	attachmentTextMaxBytes     int
)

const attachmentLogMaxChars = 2000

func upsertMessageAttachments(
	stmt *sql.Stmt,
	attachments []*discordgo.MessageAttachment,
	guildID, channelID, messageID, authorID, createdAt, now string,
	rel messageRelationship,
) (int, string) {
	if stmt == nil || len(attachments) == 0 {
		return 0, ""
	}

	var inserted int
	logParts := make([]string, 0, len(attachments))
	for _, a := range attachments {
		if a == nil || a.ID == "" {
			continue
		}

		contentText := ""
		if shouldExtractAttachmentText(a) && a.URL != "" {
			text, err := attachmentTextFetcher(a.URL, getAttachmentTextMaxBytes())
			if err != nil {
				log.Printf("attachment text fetch failed (attachment=%s message=%s): %v", a.ID, messageID, err)
			} else {
				contentText = text
			}
		}

		if _, err := stmt.Exec(
			a.ID,
			messageID,
			guildID,
			channelID,
			authorID,
			createdAt,
			a.Filename,
			a.ContentType,
			a.Size,
			a.URL,
			a.ProxyURL,
			contentText,
			rel.referencedMessageID,
			rel.referencedChannelID,
			rel.referencedGuildID,
			rel.threadID,
			rel.threadParentID,
			now,
			"",
		); err != nil {
			log.Printf("attachment upsert failed (attachment=%s message=%s): %v", a.ID, messageID, err)
			continue
		}
		inserted++

		if text := strings.TrimSpace(contentText); text != "" {
			logParts = append(logParts, text)
			continue
		}
		if name := strings.TrimSpace(a.Filename); name != "" {
			logParts = append(logParts, "[attachment] "+name)
		}
	}

	return inserted, summarizeAttachmentLogContent(logParts)
}

func summarizeAttachmentLogContent(parts []string) string {
	if len(parts) == 0 {
		return ""
	}

	combined := strings.Join(parts, "\n\n")
	if len(combined) <= attachmentLogMaxChars {
		return combined
	}
	return combined[:attachmentLogMaxChars] + "\n...[truncated in log; full attachment text stored in DB]"
}

func shouldExtractAttachmentText(a *discordgo.MessageAttachment) bool {
	if a == nil {
		return false
	}

	filename := strings.ToLower(a.Filename)
	contentType := strings.ToLower(a.ContentType)

	if strings.HasPrefix(contentType, "text/") {
		return true
	}
	if contentType == "application/json" {
		return true
	}

	switch {
	case strings.HasSuffix(filename, ".txt"),
		strings.HasSuffix(filename, ".md"),
		strings.HasSuffix(filename, ".log"),
		strings.HasSuffix(filename, ".csv"),
		strings.HasSuffix(filename, ".json"),
		strings.HasSuffix(filename, ".yaml"),
		strings.HasSuffix(filename, ".yml"):
		return true
	default:
		return false
	}
}

func fetchAttachmentText(url string, maxBytes int) (string, error) {
	resp, err := attachmentHTTPClient.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	limit := int64(maxBytes)
	if limit <= 0 {
		limit = config.DefaultAttachmentTextMaxBytes
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, limit))
	if err != nil {
		return "", err
	}

	return string(body), nil
}

func getAttachmentTextMaxBytes() int {
	attachmentTextMaxBytesOnce.Do(func() {
		raw := getenvDefault(
			config.EnvDiscordAttachmentTextMaxBytes,
			strconv.Itoa(config.DefaultAttachmentTextMaxBytes),
		)
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			log.Printf(
				"invalid %s=%q; using default=%d",
				config.EnvDiscordAttachmentTextMaxBytes,
				raw,
				config.DefaultAttachmentTextMaxBytes,
			)
			n = config.DefaultAttachmentTextMaxBytes
		}
		attachmentTextMaxBytes = n
	})
	return attachmentTextMaxBytes
}
