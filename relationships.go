// Package main derives message relationship metadata for persistence.
package main

import (
	"strings"

	"github.com/bwmarrin/discordgo"
)

type messageRelationship struct {
	referencedMessageID string
	referencedChannelID string
	referencedGuildID   string
	threadID            string
	threadParentID      string
}

func deriveMessageRelationship(
	s *discordgo.Session,
	m *discordgo.Message,
	fallbackThreadParentID string,
) messageRelationship {
	rel := messageRelationship{}
	if m == nil {
		return rel
	}

	if m.MessageReference != nil {
		rel.referencedMessageID = strings.TrimSpace(m.MessageReference.MessageID)
		rel.referencedChannelID = strings.TrimSpace(m.MessageReference.ChannelID)
		rel.referencedGuildID = strings.TrimSpace(m.MessageReference.GuildID)
	}

	threadID, threadParentID := deriveThreadContext(s, m, fallbackThreadParentID)
	rel.threadID = threadID
	rel.threadParentID = threadParentID

	return rel
}

func deriveThreadContext(
	s *discordgo.Session,
	m *discordgo.Message,
	fallbackThreadParentID string,
) (threadID, threadParentID string) {
	if m == nil {
		return "", ""
	}
	channelID := strings.TrimSpace(m.ChannelID)
	fallbackThreadParentID = strings.TrimSpace(fallbackThreadParentID)

	if fallbackThreadParentID != "" && channelID != "" {
		return channelID, fallbackThreadParentID
	}

	if s != nil && s.State != nil && channelID != "" {
		if ch, err := s.State.Channel(channelID); err == nil && ch != nil && ch.IsThread() {
			return channelID, strings.TrimSpace(ch.ParentID)
		}
	}

	if m.Thread != nil {
		threadID = strings.TrimSpace(m.Thread.ID)
		threadParentID = strings.TrimSpace(m.Thread.ParentID)
		if threadID == "" {
			threadID = channelID
		}
		if threadID != "" && (m.Thread.IsThread() || threadParentID != "") {
			return threadID, threadParentID
		}
	}

	return "", ""
}
