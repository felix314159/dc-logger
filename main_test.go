package main

import (
	"io"
	"testing"
)

func TestParseMainConfigSkipSync(t *testing.T) {
	cfg, err := parseMainConfig([]string{"--skip-sync"}, io.Discard)
	if err != nil {
		t.Fatalf("parseMainConfig failed: %v", err)
	}
	if !cfg.skipSync {
		t.Fatal("expected --skip-sync to enable skipSync")
	}
}

func TestParseMainConfigDefault(t *testing.T) {
	cfg, err := parseMainConfig(nil, io.Discard)
	if err != nil {
		t.Fatalf("parseMainConfig failed: %v", err)
	}
	if cfg.skipSync {
		t.Fatal("expected skipSync to default to false")
	}
}
