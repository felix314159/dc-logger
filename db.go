// Package main defines SQLite schema initialization and prepared statement lifecycle helpers.
package main

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	_ "modernc.org/sqlite"
)

type preparedStatements struct {
	insertMsg                       *sql.Stmt
	upsertAttachment                *sql.Stmt
	markMessageEdited               *sql.Stmt
	markMessageDeleted              *sql.Stmt
	markAttachmentsDeletedByMessage *sql.Stmt
	insertEvent                     *sql.Stmt
	insertMessageUpdatedEventDedup  *sql.Stmt
	upsertState                     *sql.Stmt
	upsertChannel                   *sql.Stmt
	markChannelDeleted              *sql.Stmt
	upsertUser                      *sql.Stmt
	upsertIDNameMapping             *sql.Stmt
	upsertPingEvent                 *sql.Stmt
	upsertRole                      *sql.Stmt
	markRoleDeleted                 *sql.Stmt
	upsertMemberRole                *sql.Stmt
	deleteMemberRolesByUser         *sql.Stmt
	upsertRolePingEvent             *sql.Stmt
	upsertGuildMember               *sql.Stmt
	deleteGuildMember               *sql.Stmt
	upsertArchivedDiscoveryState    *sql.Stmt
}

func prepareStatements(db *sql.DB) (*preparedStatements, error) {
	stmts := &preparedStatements{}
	cleanup := func() {
		closePreparedStatements(stmts)
	}
	prepare := func(dst **sql.Stmt, query, name string) error {
		stmt, err := db.Prepare(query)
		if err != nil {
			return fmt.Errorf("prepare %s: %w", name, err)
		}
		*dst = stmt
		return nil
	}

	if err := prepare(&stmts.insertMsg, insertMessageQuery, "insert"); err != nil {
		return nil, err
	}
	if err := prepare(&stmts.upsertState, upsertChannelStateQuery, "state upsert"); err != nil {
		cleanup()
		return nil, err
	}
	if err := prepare(&stmts.upsertAttachment, upsertAttachmentQuery, "attachment upsert"); err != nil {
		cleanup()
		return nil, err
	}
	if err := prepare(&stmts.upsertChannel, upsertChannelQuery, "channel upsert"); err != nil {
		cleanup()
		return nil, err
	}
	if err := prepare(&stmts.markChannelDeleted, markChannelDeletedQuery, "channel deletion mark"); err != nil {
		cleanup()
		return nil, err
	}
	if err := prepare(&stmts.upsertUser, upsertUserQuery, "user upsert"); err != nil {
		cleanup()
		return nil, err
	}
	if err := prepare(&stmts.upsertIDNameMapping, upsertIDNameMappingQuery, "id-name mapping upsert"); err != nil {
		cleanup()
		return nil, err
	}
	if err := prepare(&stmts.upsertRole, upsertRoleQuery, "role upsert"); err != nil {
		cleanup()
		return nil, err
	}
	if err := prepare(&stmts.markRoleDeleted, markRoleDeletedQuery, "role deletion mark"); err != nil {
		cleanup()
		return nil, err
	}
	if err := prepare(&stmts.upsertMemberRole, upsertMemberRoleQuery, "member-role upsert"); err != nil {
		cleanup()
		return nil, err
	}
	if err := prepare(&stmts.deleteMemberRolesByUser, deleteMemberRolesByUserQuery, "member-roles by user delete"); err != nil {
		cleanup()
		return nil, err
	}
	if err := prepare(&stmts.upsertGuildMember, upsertGuildMemberQuery, "guild-member upsert"); err != nil {
		cleanup()
		return nil, err
	}
	if err := prepare(&stmts.deleteGuildMember, deleteGuildMemberQuery, "guild-member delete"); err != nil {
		cleanup()
		return nil, err
	}
	if err := prepare(&stmts.upsertPingEvent, upsertPingEventQuery, "ping event upsert"); err != nil {
		cleanup()
		return nil, err
	}
	if err := prepare(&stmts.upsertRolePingEvent, upsertRolePingEventQuery, "role ping event upsert"); err != nil {
		cleanup()
		return nil, err
	}
	if err := prepare(&stmts.markMessageEdited, markMessageEditedQuery, "message edit mark"); err != nil {
		cleanup()
		return nil, err
	}
	if err := prepare(&stmts.markMessageDeleted, markMessageDeletedQuery, "message deletion mark"); err != nil {
		cleanup()
		return nil, err
	}
	if err := prepare(&stmts.markAttachmentsDeletedByMessage, markAttachmentsDeletedByMessageQuery, "attachment deletion mark"); err != nil {
		cleanup()
		return nil, err
	}
	if err := prepare(&stmts.insertEvent, insertLifecycleEventQuery, "lifecycle event insert"); err != nil {
		cleanup()
		return nil, err
	}
	if err := prepare(
		&stmts.insertMessageUpdatedEventDedup,
		insertMessageUpdatedLifecycleEventDedupQuery,
		"message_updated lifecycle event dedup insert",
	); err != nil {
		cleanup()
		return nil, err
	}
	if err := prepare(&stmts.upsertArchivedDiscoveryState, upsertArchivedDiscoveryStateQuery, "archived discovery state upsert"); err != nil {
		cleanup()
		return nil, err
	}

	return stmts, nil
}

func closePreparedStatements(stmts *preparedStatements) {
	if stmts == nil {
		return
	}
	closeStmt := func(name string, stmt *sql.Stmt) {
		if stmt == nil {
			return
		}
		if err := stmt.Close(); err != nil {
			log.Printf("close %s statement failed: %v", name, err)
		}
	}

	closeStmt("insert message", stmts.insertMsg)
	closeStmt("upsert attachment", stmts.upsertAttachment)
	closeStmt("mark message edited", stmts.markMessageEdited)
	closeStmt("mark message deleted", stmts.markMessageDeleted)
	closeStmt("mark attachments deleted", stmts.markAttachmentsDeletedByMessage)
	closeStmt("insert lifecycle event", stmts.insertEvent)
	closeStmt("insert message_updated lifecycle event dedup", stmts.insertMessageUpdatedEventDedup)
	closeStmt("upsert state", stmts.upsertState)
	closeStmt("upsert channel", stmts.upsertChannel)
	closeStmt("mark channel deleted", stmts.markChannelDeleted)
	closeStmt("upsert user", stmts.upsertUser)
	closeStmt("upsert id-name mapping", stmts.upsertIDNameMapping)
	closeStmt("upsert role", stmts.upsertRole)
	closeStmt("mark role deleted", stmts.markRoleDeleted)
	closeStmt("upsert member-role", stmts.upsertMemberRole)
	closeStmt("delete member-roles by user", stmts.deleteMemberRolesByUser)
	closeStmt("upsert guild-member", stmts.upsertGuildMember)
	closeStmt("delete guild-member", stmts.deleteGuildMember)
	closeStmt("upsert ping event", stmts.upsertPingEvent)
	closeStmt("upsert role ping event", stmts.upsertRolePingEvent)
	closeStmt("upsert archived discovery state", stmts.upsertArchivedDiscoveryState)
}

func openAndInitDB(path string) (*sql.DB, error) {
	if err := ensureDatabaseParentDir(path); err != nil {
		return nil, err
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}

	for _, p := range sqlitePragmas {
		if _, err := db.Exec(p); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("pragma failed (%s): %w", p, err)
		}
	}

	if _, err := db.Exec(sqliteSchemaQuery); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("schema: %w", err)
	}

	for _, m := range sqliteSchemaMigrations {
		if _, err := db.Exec(m); err != nil && !strings.Contains(err.Error(), "duplicate column name") {
			_ = db.Close()
			return nil, fmt.Errorf("migration failed (%s): %w", m, err)
		}
	}

	for _, idx := range sqlitePostMigrationIndexes {
		if _, err := db.Exec(idx); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("post-migration index failed (%s): %w", idx, err)
		}
	}

	ftsEnabled := true
	for _, stmt := range sqliteFTSSetup {
		if _, err := db.Exec(stmt); err != nil {
			if isFTSUnavailableError(err) {
				log.Printf("sqlite FTS5 is unavailable; search will use LIKE fallback only: %v", err)
				ftsEnabled = false
				break
			}
			_ = db.Close()
			return nil, fmt.Errorf("fts setup failed: %w", err)
		}
	}
	if ftsEnabled {
		if err := runFTSStartupMaintenance(db); err != nil {
			_ = db.Close()
			return nil, err
		}
	}

	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := ensureDatabaseFilePresent(path); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func isFTSUnavailableError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "fts5") || strings.Contains(msg, "no such module")
}

func runFTSStartupMaintenance(db *sql.DB) error {
	if db == nil {
		return nil
	}

	hasFTSRows, err := queryExists(db, hasAnyMessageSearchFTSRowsQuery)
	if err != nil {
		return fmt.Errorf("fts maintenance precheck failed (fts rows): %w", err)
	}

	if !hasFTSRows {
		hasMessages, err := queryExists(db, hasAnyLiveMessagesQuery)
		if err != nil {
			return fmt.Errorf("fts maintenance precheck failed (messages): %w", err)
		}
		hasAttachments, err := queryExists(db, hasAnyLiveAttachmentsQuery)
		if err != nil {
			return fmt.Errorf("fts maintenance precheck failed (attachments): %w", err)
		}
		if hasMessages || hasAttachments {
			log.Println("fts startup maintenance: bootstrap from existing messages/attachments")
			for _, stmt := range sqliteFTSBootstrap {
				if _, err := db.Exec(stmt); err != nil {
					return fmt.Errorf("fts bootstrap failed: %w", err)
				}
			}
		}
	}

	for _, stmt := range sqliteFTSPrune {
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("fts prune failed: %w", err)
		}
	}
	return nil
}

func queryExists(db *sql.DB, query string) (bool, error) {
	if db == nil {
		return false, nil
	}
	var exists int
	if err := db.QueryRow(query).Scan(&exists); err != nil {
		return false, err
	}
	return exists != 0, nil
}
