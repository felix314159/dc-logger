// Package main wires startup configuration, storage initialization, and bot launch.
package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"example.org/dc-logger/internal/config"
)

type mainConfig struct {
	skipSync bool
}

func main() {
	log.Println("Engaging surveillance dystopia..")

	cfg, err := parseMainConfig(os.Args[1:], os.Stderr)
	if err != nil {
		log.Fatal(err)
	}

	token := os.Getenv(config.EnvDiscordBotToken)
	if token == "" {
		log.Fatalf("%s is not set", config.EnvDiscordBotToken)
	}

	if err := runBotWithOptions(token, config.DefaultDatabaseDir, botOptions{
		skipSync: cfg.skipSync,
	}); err != nil {
		log.Fatalf("bot failed: %v", err)
	}
}

func parseMainConfig(args []string, output io.Writer) (mainConfig, error) {
	var cfg mainConfig
	fs := flag.NewFlagSet("dc-logger", flag.ContinueOnError)
	fs.SetOutput(output)
	fs.BoolVar(&cfg.skipSync, "skip-sync", false, "skip startup backfill sync and start live logging immediately")

	if err := fs.Parse(args); err != nil {
		return mainConfig{}, err
	}
	if fs.NArg() > 0 {
		return mainConfig{}, fmt.Errorf("unexpected argument: %s", fs.Arg(0))
	}
	return cfg, nil
}
