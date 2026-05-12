// Package main wires startup configuration, storage initialization, and bot launch.
package main

import (
	"log"
	"os"

	"example.org/dc-logger/internal/config"
)

func main() {
	log.Println("Engaging surveillance dystopia..")

	token := os.Getenv(config.EnvDiscordBotToken)
	if token == "" {
		log.Fatalf("%s is not set", config.EnvDiscordBotToken)
	}

	dbPath := getenvDefault(config.EnvDiscordLogDB, config.DefaultLogDBPath)
	if err := runBot(token, dbPath); err != nil {
		log.Fatalf("bot failed: %v", err)
	}
}
