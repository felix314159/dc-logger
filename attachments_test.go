package main

import (
	"strings"
	"testing"
)

func TestSummarizeAttachmentLogContent_NoTruncation(t *testing.T) {
	got := summarizeAttachmentLogContent([]string{"alpha", "beta"})
	want := "alpha\n\nbeta"
	if got != want {
		t.Fatalf("unexpected summary: got %q want %q", got, want)
	}
}

func TestSummarizeAttachmentLogContent_TruncatesAndMarks(t *testing.T) {
	input := strings.Repeat("x", attachmentLogMaxChars+50)
	got := summarizeAttachmentLogContent([]string{input})
	if !strings.Contains(got, "...[truncated in log; full attachment text stored in DB]") {
		t.Fatalf("expected truncation marker in %q", got)
	}
	if len(got) <= attachmentLogMaxChars {
		t.Fatalf("expected output longer than cap due to suffix, got len=%d cap=%d", len(got), attachmentLogMaxChars)
	}
	if !strings.HasPrefix(got, strings.Repeat("x", attachmentLogMaxChars)) {
		t.Fatalf("expected output to start with capped prefix")
	}
}
