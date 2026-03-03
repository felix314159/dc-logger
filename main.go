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

	db, err := openAndInitDB(dbPath)
	if err != nil {
		log.Fatalf("db init failed: %v", err)
	}
	defer db.Close()

	stmts, err := prepareStatements(db)
	if err != nil {
		log.Fatalf("prepare statements failed: %v", err)
	}
	defer closePreparedStatements(stmts)

	if err := runBot(token, dbPath, db, stmts); err != nil {
		log.Fatalf("bot failed: %v", err)
	}
}
