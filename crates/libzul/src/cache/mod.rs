use crate::zulip::models::{
    ZulipDirectConversation, ZulipDmParticipant, ZulipMessage, ZulipMessageType, ZulipStream,
    ZulipTopic,
};
use anyhow::{Context, Result};
use chrono::{DateTime, TimeZone, Utc};
use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

const MIGRATION_001: &str = r#"
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS app_metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS zulip_streams (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    description TEXT,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS zulip_topics (
    stream_id INTEGER NOT NULL,
    stream_name TEXT NOT NULL,
    name TEXT NOT NULL,
    message_count INTEGER NOT NULL DEFAULT 0,
    last_message_at INTEGER,
    PRIMARY KEY (stream_id, name)
);

CREATE TABLE IF NOT EXISTS zulip_messages (
    id INTEGER PRIMARY KEY,
    message_type TEXT NOT NULL DEFAULT 'stream',
    stream_id INTEGER,
    stream_name TEXT NOT NULL,
    topic TEXT NOT NULL,
    dm_conversation_key TEXT,
    dm_participants_json TEXT NOT NULL DEFAULT '[]',
    sender_id INTEGER,
    sender_name TEXT NOT NULL,
    sender_email TEXT,
    timestamp INTEGER NOT NULL,
    rendered_html TEXT NOT NULL,
    plain_text TEXT NOT NULL,
    raw_json TEXT NOT NULL,
    permalink TEXT,
    fetched_at INTEGER NOT NULL
);

CREATE VIRTUAL TABLE IF NOT EXISTS zulip_messages_fts USING fts5(
    plain_text,
    sender_name,
    stream_name,
    topic,
    content='zulip_messages',
    content_rowid='id'
);

CREATE TRIGGER IF NOT EXISTS zulip_messages_ai AFTER INSERT ON zulip_messages BEGIN
    INSERT INTO zulip_messages_fts(rowid, plain_text, sender_name, stream_name, topic)
    VALUES (new.id, new.plain_text, new.sender_name, new.stream_name, new.topic);
END;

CREATE TRIGGER IF NOT EXISTS zulip_messages_ad AFTER DELETE ON zulip_messages BEGIN
    INSERT INTO zulip_messages_fts(zulip_messages_fts, rowid, plain_text, sender_name, stream_name, topic)
    VALUES('delete', old.id, old.plain_text, old.sender_name, old.stream_name, old.topic);
END;

CREATE TRIGGER IF NOT EXISTS zulip_messages_au AFTER UPDATE ON zulip_messages BEGIN
    INSERT INTO zulip_messages_fts(zulip_messages_fts, rowid, plain_text, sender_name, stream_name, topic)
    VALUES('delete', old.id, old.plain_text, old.sender_name, old.stream_name, old.topic);
    INSERT INTO zulip_messages_fts(rowid, plain_text, sender_name, stream_name, topic)
    VALUES (new.id, new.plain_text, new.sender_name, new.stream_name, new.topic);
END;

CREATE TABLE IF NOT EXISTS sync_state (
    scope TEXT PRIMARY KEY,
    synced_at INTEGER NOT NULL,
    status TEXT NOT NULL,
    detail TEXT
);

CREATE TABLE IF NOT EXISTS drafts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stream_name TEXT NOT NULL,
    topic TEXT NOT NULL,
    content TEXT NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS read_state (
    message_id INTEGER PRIMARY KEY,
    read_at INTEGER
);

CREATE TABLE IF NOT EXISTS audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    action TEXT NOT NULL,
    stream_name TEXT,
    topic TEXT,
    content_hash TEXT,
    zulip_message_id INTEGER,
    dry_run INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL
);
"#;

/// Recorded synchronization status for a cache scope.
#[derive(Debug, Clone, Serialize)]
pub struct SyncRecord {
    /// Sync scope name.
    pub scope: String,
    /// Time the status was recorded.
    pub synced_at: DateTime<Utc>,
    /// Short status string.
    pub status: String,
    /// Optional detail text.
    pub detail: Option<String>,
}

/// Aggregate message count for a stream.
#[derive(Debug, Clone, Serialize)]
pub struct StreamActivity {
    /// Stream name.
    pub stream_name: String,
    /// Number of cached messages in the stream.
    pub message_count: i64,
}

/// SQLite-backed cache for Zulip streams, topics, messages, and send audit records.
pub struct Cache {
    conn: Connection,
}

impl Cache {
    /// Open a cache database at `path`, creating parent directories and applying migrations.
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
        }
        let conn = Connection::open(path).with_context(|| format!("opening {}", path.display()))?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        let cache = Self { conn };
        cache.migrate()?;
        Ok(cache)
    }

    #[cfg(test)]
    /// Open an in-memory cache for unit tests.
    pub fn in_memory() -> Result<Self> {
        let cache = Self {
            conn: Connection::open_in_memory()?,
        };
        cache.migrate()?;
        Ok(cache)
    }

    fn migrate(&self) -> Result<()> {
        self.conn.execute_batch(MIGRATION_001)?;
        self.ensure_column(
            "zulip_messages",
            "message_type",
            "ALTER TABLE zulip_messages ADD COLUMN message_type TEXT NOT NULL DEFAULT 'stream'",
        )?;
        self.ensure_column(
            "zulip_messages",
            "dm_conversation_key",
            "ALTER TABLE zulip_messages ADD COLUMN dm_conversation_key TEXT",
        )?;
        self.ensure_column(
            "zulip_messages",
            "dm_participants_json",
            "ALTER TABLE zulip_messages ADD COLUMN dm_participants_json TEXT NOT NULL DEFAULT '[]'",
        )?;
        self.conn.execute(
            "INSERT OR REPLACE INTO app_metadata (key, value) VALUES ('schema_version', '2')",
            [],
        )?;
        Ok(())
    }

    fn ensure_column(&self, table: &str, column: &str, alter_sql: &str) -> Result<()> {
        let mut stmt = self.conn.prepare(&format!("PRAGMA table_info({table})"))?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
        let columns = rows.collect::<rusqlite::Result<Vec<_>>>()?;
        if !columns.iter().any(|name| name == column) {
            self.conn.execute(alter_sql, [])?;
        }
        Ok(())
    }

    /// Insert or update cached stream metadata.
    pub fn upsert_streams(&self, streams: &[ZulipStream]) -> Result<()> {
        let now = Utc::now().timestamp();
        for stream in streams {
            self.conn.execute(
                "INSERT INTO zulip_streams (id, name, description, updated_at)
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(id) DO UPDATE SET
                    name=excluded.name,
                    description=excluded.description,
                    updated_at=excluded.updated_at",
                params![stream.id, stream.name, stream.description, now],
            )?;
        }
        Ok(())
    }

    /// Insert or update cached messages and rebuild topic summaries.
    pub fn upsert_messages(&self, messages: &[ZulipMessage]) -> Result<()> {
        let fetched_at = Utc::now().timestamp();
        for message in messages {
            self.conn.execute(
                "INSERT INTO zulip_messages (
                    id, message_type, stream_id, stream_name, topic, dm_conversation_key,
                    dm_participants_json, sender_id, sender_name, sender_email, timestamp,
                    rendered_html, plain_text, raw_json, permalink, fetched_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)
                 ON CONFLICT(id) DO UPDATE SET
                    message_type=excluded.message_type,
                    stream_id=excluded.stream_id,
                    stream_name=excluded.stream_name,
                    topic=excluded.topic,
                    dm_conversation_key=excluded.dm_conversation_key,
                    dm_participants_json=excluded.dm_participants_json,
                    sender_id=excluded.sender_id,
                    sender_name=excluded.sender_name,
                    sender_email=excluded.sender_email,
                    timestamp=excluded.timestamp,
                    rendered_html=excluded.rendered_html,
                    plain_text=excluded.plain_text,
                    raw_json=excluded.raw_json,
                    permalink=excluded.permalink,
                    fetched_at=excluded.fetched_at",
                params![
                    message.id,
                    message.message_type.as_str(),
                    message.stream_id,
                    message.stream_name,
                    message.topic,
                    &message.dm_conversation_key,
                    serde_json::to_string(&message.dm_participants)?,
                    message.sender_id,
                    message.sender_name,
                    message.sender_email,
                    message.timestamp.timestamp(),
                    message.rendered_html,
                    message.plain_text,
                    serde_json::to_string(&message.raw_json)?,
                    message.permalink,
                    fetched_at
                ],
            )?;
        }
        self.rebuild_topics()?;
        Ok(())
    }

    fn rebuild_topics(&self) -> Result<()> {
        self.conn.execute("DELETE FROM zulip_topics", [])?;
        self.conn.execute(
            "INSERT INTO zulip_topics (stream_id, stream_name, name, message_count, last_message_at)
             SELECT COALESCE(stream_id, 0), stream_name, topic, COUNT(*), MAX(timestamp)
             FROM zulip_messages
             WHERE message_type = 'stream'
             GROUP BY COALESCE(stream_id, 0), stream_name, topic",
            [],
        )?;
        Ok(())
    }

    /// List cached streams sorted by name.
    pub fn streams(&self) -> Result<Vec<ZulipStream>> {
        let mut stmt = self
            .conn
            .prepare("SELECT id, name, description FROM zulip_streams ORDER BY lower(name)")?;
        let rows = stmt.query_map([], |row| {
            Ok(ZulipStream {
                id: row.get(0)?,
                name: row.get(1)?,
                description: row.get(2)?,
            })
        })?;
        let streams = rows.collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(streams)
    }

    /// List cached topics for a stream.
    pub fn topics(&self, stream: &str) -> Result<Vec<ZulipTopic>> {
        let mut stmt = self.conn.prepare(
            "SELECT stream_id, stream_name, name, message_count, last_message_at
             FROM zulip_topics
             WHERE stream_name = ?1
             ORDER BY last_message_at DESC, lower(name)",
        )?;
        let rows = stmt.query_map(params![stream], topic_from_row)?;
        let topics = rows.collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(topics)
    }

    /// List cached messages for one stream/topic pair.
    pub fn messages_for_topic(&self, stream: &str, topic: &str) -> Result<Vec<ZulipMessage>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, stream_id, stream_name, topic, sender_id, sender_name, sender_email,
                    timestamp, rendered_html, plain_text, raw_json, permalink, message_type,
                    dm_conversation_key, dm_participants_json
             FROM zulip_messages
             WHERE stream_name = ?1 AND topic = ?2
                AND message_type = 'stream'
             ORDER BY timestamp ASC, id ASC",
        )?;
        let rows = stmt.query_map(params![stream, topic], message_from_row)?;
        let messages = rows.collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(messages)
    }

    /// List cached messages newer than `hours` ago.
    pub fn recent_messages(&self, hours: i64) -> Result<Vec<ZulipMessage>> {
        let cutoff = (Utc::now() - chrono::Duration::hours(hours)).timestamp();
        let mut stmt = self.conn.prepare(
            "SELECT id, stream_id, stream_name, topic, sender_id, sender_name, sender_email,
                    timestamp, rendered_html, plain_text, raw_json, permalink, message_type,
                    dm_conversation_key, dm_participants_json
             FROM zulip_messages
             WHERE timestamp >= ?1
             ORDER BY timestamp DESC, id DESC",
        )?;
        let rows = stmt.query_map(params![cutoff], message_from_row)?;
        let messages = rows.collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(messages)
    }

    /// Return the highest cached Zulip message ID.
    pub fn max_message_id(&self) -> Result<Option<i64>> {
        self.conn
            .query_row("SELECT MAX(id) FROM zulip_messages", [], |row| row.get(0))
            .map_err(Into::into)
    }

    /// Return cached authored message IDs ordered by least-recently refreshed.
    pub fn authored_message_ids_for_refresh(
        &self,
        sender_email: &str,
        limit: usize,
    ) -> Result<Vec<i64>> {
        let mut stmt = self.conn.prepare(
            "SELECT id
             FROM zulip_messages
             WHERE sender_email = ?1
             ORDER BY fetched_at ASC, timestamp DESC, id DESC
             LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![sender_email, limit as i64], |row| row.get(0))?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Mark cached message IDs as refreshed without changing their content.
    pub fn mark_messages_refreshed(&self, message_ids: &[i64]) -> Result<()> {
        let fetched_at = Utc::now().timestamp();
        for message_id in message_ids {
            self.conn.execute(
                "UPDATE zulip_messages SET fetched_at = ?1 WHERE id = ?2",
                params![fetched_at, message_id],
            )?;
        }
        Ok(())
    }

    /// Search cached messages with SQLite FTS.
    pub fn search_messages(&self, query: &str) -> Result<Vec<ZulipMessage>> {
        let mut stmt = self.conn.prepare(
            "SELECT m.id, m.stream_id, m.stream_name, m.topic, m.sender_id, m.sender_name,
                    m.sender_email, m.timestamp, m.rendered_html, m.plain_text, m.raw_json,
                    m.permalink, m.message_type, m.dm_conversation_key, m.dm_participants_json
             FROM zulip_messages_fts f
             JOIN zulip_messages m ON m.id = f.rowid
             WHERE zulip_messages_fts MATCH ?1
             ORDER BY bm25(zulip_messages_fts), m.timestamp DESC
             LIMIT 100",
        )?;
        let result = stmt
            .query_map(params![query], message_from_row)?
            .collect::<rusqlite::Result<Vec<_>>>();
        match result {
            Ok(messages) => Ok(messages),
            Err(_) => self.search_messages_like(query),
        }
    }

    /// Return cached message counts grouped by stream.
    pub fn stream_activity(&self) -> Result<Vec<StreamActivity>> {
        let mut stmt = self
            .conn
            .prepare("SELECT stream_name, COUNT(*) FROM zulip_messages WHERE message_type = 'stream' GROUP BY stream_name")?;
        let rows = stmt.query_map([], |row| {
            Ok(StreamActivity {
                stream_name: row.get(0)?,
                message_count: row.get(1)?,
            })
        })?;
        let activity = rows.collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(activity)
    }

    /// Return unread cached message counts grouped by stream.
    pub fn unread_stream_activity(&self) -> Result<Vec<StreamActivity>> {
        let mut stmt = self.conn.prepare(
            "SELECT m.stream_name, COUNT(*)
             FROM zulip_messages m
             LEFT JOIN read_state r ON r.message_id = m.id AND r.read_at IS NOT NULL
             WHERE m.message_type = 'stream' AND r.message_id IS NULL
             GROUP BY m.stream_name",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(StreamActivity {
                stream_name: row.get(0)?,
                message_count: row.get(1)?,
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Return the number of cached direct messages.
    pub fn direct_message_count(&self) -> Result<i64> {
        self.conn
            .query_row(
                "SELECT COUNT(*) FROM zulip_messages WHERE message_type = 'direct'",
                [],
                |row| row.get(0),
            )
            .map_err(Into::into)
    }

    /// Return the number of unread cached direct messages.
    pub fn unread_direct_message_count(&self) -> Result<i64> {
        self.conn
            .query_row(
                "SELECT COUNT(*)
                 FROM zulip_messages m
                 LEFT JOIN read_state r ON r.message_id = m.id AND r.read_at IS NOT NULL
                 WHERE m.message_type = 'direct' AND r.message_id IS NULL",
                [],
                |row| row.get(0),
            )
            .map_err(Into::into)
    }

    /// Return unread message counts keyed by topic name for one stream.
    pub fn unread_topic_counts(&self, stream: &str) -> Result<BTreeMap<String, i64>> {
        let mut stmt = self.conn.prepare(
            "SELECT m.topic, COUNT(*)
             FROM zulip_messages m
             LEFT JOIN read_state r ON r.message_id = m.id AND r.read_at IS NOT NULL
             WHERE m.stream_name = ?1
                AND m.message_type = 'stream'
                AND r.message_id IS NULL
             GROUP BY m.topic",
        )?;
        let rows = stmt.query_map(params![stream], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?;
        Ok(rows.collect::<rusqlite::Result<BTreeMap<_, _>>>()?)
    }

    /// Return unread message counts keyed by direct-conversation key.
    pub fn unread_direct_conversation_counts(&self) -> Result<BTreeMap<String, i64>> {
        let mut stmt = self.conn.prepare(
            "SELECT m.dm_conversation_key, COUNT(*)
             FROM zulip_messages m
             LEFT JOIN read_state r ON r.message_id = m.id AND r.read_at IS NOT NULL
             WHERE m.message_type = 'direct'
                AND m.dm_conversation_key IS NOT NULL
                AND r.message_id IS NULL
             GROUP BY m.dm_conversation_key",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?;
        Ok(rows.collect::<rusqlite::Result<BTreeMap<_, _>>>()?)
    }

    /// Mark cached message IDs as read.
    pub fn mark_messages_read(&self, message_ids: &[i64]) -> Result<()> {
        let read_at = Utc::now().timestamp();
        for message_id in message_ids {
            self.conn.execute(
                "INSERT INTO read_state (message_id, read_at)
                 VALUES (?1, ?2)
                 ON CONFLICT(message_id) DO UPDATE SET read_at = excluded.read_at",
                params![message_id, read_at],
            )?;
        }
        Ok(())
    }

    /// Return the subset of message IDs that have been marked read.
    pub fn read_message_ids(&self, message_ids: &[i64]) -> Result<BTreeSet<i64>> {
        let mut read_ids = BTreeSet::new();
        for message_id in message_ids {
            let is_read = self
                .conn
                .query_row(
                    "SELECT 1 FROM read_state WHERE message_id = ?1 AND read_at IS NOT NULL",
                    params![message_id],
                    |_| Ok(()),
                )
                .optional()?
                .is_some();
            if is_read {
                read_ids.insert(*message_id);
            }
        }
        Ok(read_ids)
    }

    /// List cached direct-message conversations.
    pub fn direct_conversations(&self) -> Result<Vec<ZulipDirectConversation>> {
        let mut stmt = self.conn.prepare(
            "SELECT dm_conversation_key, MAX(dm_participants_json), COUNT(*), MAX(timestamp)
             FROM zulip_messages
             WHERE message_type = 'direct' AND dm_conversation_key IS NOT NULL
             GROUP BY dm_conversation_key
             ORDER BY MAX(timestamp) DESC, lower(MAX(topic))",
        )?;
        let rows = stmt.query_map([], direct_conversation_from_row)?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// List cached messages for a direct-message conversation.
    pub fn messages_for_direct_conversation(&self, key: &str) -> Result<Vec<ZulipMessage>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, stream_id, stream_name, topic, sender_id, sender_name, sender_email,
                    timestamp, rendered_html, plain_text, raw_json, permalink, message_type,
                    dm_conversation_key, dm_participants_json
             FROM zulip_messages
             WHERE message_type = 'direct' AND dm_conversation_key = ?1
             ORDER BY timestamp ASC, id ASC",
        )?;
        let rows = stmt.query_map(params![key], message_from_row)?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    fn search_messages_like(&self, query: &str) -> Result<Vec<ZulipMessage>> {
        let like = format!("%{}%", query);
        let mut stmt = self.conn.prepare(
            "SELECT id, stream_id, stream_name, topic, sender_id, sender_name, sender_email,
                    timestamp, rendered_html, plain_text, raw_json, permalink, message_type,
                    dm_conversation_key, dm_participants_json
             FROM zulip_messages
             WHERE plain_text LIKE ?1 OR sender_name LIKE ?1 OR stream_name LIKE ?1 OR topic LIKE ?1
             ORDER BY timestamp DESC
             LIMIT 100",
        )?;
        let rows = stmt.query_map(params![like], message_from_row)?;
        let messages = rows.collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(messages)
    }

    /// Record synchronization status for a scope.
    pub fn set_sync_state(&self, scope: &str, status: &str, detail: Option<&str>) -> Result<()> {
        self.conn.execute(
            "INSERT INTO sync_state (scope, synced_at, status, detail)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(scope) DO UPDATE SET
                synced_at=excluded.synced_at,
                status=excluded.status,
                detail=excluded.detail",
            params![scope, Utc::now().timestamp(), status, detail],
        )?;
        Ok(())
    }

    /// Return synchronization status for a scope.
    pub fn sync_state(&self, scope: &str) -> Result<Option<SyncRecord>> {
        self.conn
            .query_row(
                "SELECT scope, synced_at, status, detail FROM sync_state WHERE scope = ?1",
                params![scope],
                |row| {
                    Ok(SyncRecord {
                        scope: row.get(0)?,
                        synced_at: ts(row.get(1)?),
                        status: row.get(2)?,
                        detail: row.get(3)?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }

    /// Return whether the send audit log already contains a content hash.
    pub fn has_sent_hash(&self, hash: &str) -> Result<bool> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM audit_log WHERE action = 'send_stream_message' AND content_hash = ?1 AND dry_run = 0",
            params![hash],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    /// Record an attempted send and return its duplicate-protection hash.
    pub fn record_send_attempt(&self, stream: &str, topic: &str, content: &str) -> Result<String> {
        self.record_send(stream, topic, content, None, false)
    }

    /// Attach a Zulip message ID to a previously recorded send attempt.
    pub fn complete_send_attempt(&self, hash: &str, zulip_message_id: i64) -> Result<()> {
        self.conn.execute(
            "UPDATE audit_log
             SET zulip_message_id = ?1
             WHERE id = (
                SELECT id FROM audit_log
                WHERE action = 'send_stream_message' AND content_hash = ?2 AND dry_run = 0
                ORDER BY created_at DESC, id DESC
                LIMIT 1
             )",
            params![zulip_message_id, hash],
        )?;
        Ok(())
    }

    /// Record a send or dry-run entry and return its duplicate-protection hash.
    pub fn record_send(
        &self,
        stream: &str,
        topic: &str,
        content: &str,
        zulip_message_id: Option<i64>,
        dry_run: bool,
    ) -> Result<String> {
        let hash = send_hash(stream, topic, content);
        self.conn.execute(
            "INSERT INTO audit_log (action, stream_name, topic, content_hash, zulip_message_id, dry_run, created_at)
             VALUES ('send_stream_message', ?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                stream,
                topic,
                hash,
                zulip_message_id,
                if dry_run { 1 } else { 0 },
                Utc::now().timestamp()
            ],
        )?;
        Ok(hash)
    }
}

/// Build the duplicate-protection hash for a stream/topic/content tuple.
pub fn send_hash(stream: &str, topic: &str, content: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(stream.as_bytes());
    hasher.update([0]);
    hasher.update(topic.as_bytes());
    hasher.update([0]);
    hasher.update(content.as_bytes());
    hex_lower(&hasher.finalize())
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        value.push(HEX[(byte >> 4) as usize] as char);
        value.push(HEX[(byte & 0x0f) as usize] as char);
    }
    value
}

fn topic_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ZulipTopic> {
    let last_message_at: Option<i64> = row.get(4)?;
    Ok(ZulipTopic {
        stream_id: row.get(0)?,
        stream_name: row.get(1)?,
        name: row.get(2)?,
        message_count: row.get(3)?,
        last_message_at: last_message_at.map(ts),
    })
}

fn message_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ZulipMessage> {
    let raw_json: String = row.get(10)?;
    let message_type = ZulipMessageType::from_cache(row.get::<_, String>(12)?.as_str());
    let dm_participants_json: String = row.get(14)?;
    Ok(ZulipMessage {
        id: row.get(0)?,
        message_type,
        stream_id: row.get(1)?,
        stream_name: row.get(2)?,
        topic: row.get(3)?,
        dm_conversation_key: row.get(13)?,
        dm_participants: serde_json::from_str(&dm_participants_json).unwrap_or_default(),
        sender_id: row.get(4)?,
        sender_name: row.get(5)?,
        sender_email: row.get(6)?,
        timestamp: ts(row.get(7)?),
        rendered_html: row.get(8)?,
        plain_text: row.get(9)?,
        raw_json: serde_json::from_str(&raw_json).unwrap_or(serde_json::Value::Null),
        permalink: row.get(11)?,
    })
}

fn direct_conversation_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<ZulipDirectConversation> {
    let participants_json: String = row.get(1)?;
    let last_message_at: Option<i64> = row.get(3)?;
    Ok(ZulipDirectConversation {
        key: row.get(0)?,
        participants: serde_json::from_str::<Vec<ZulipDmParticipant>>(&participants_json)
            .unwrap_or_default(),
        message_count: row.get(2)?,
        last_message_at: last_message_at.map(ts),
    })
}

fn ts(value: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(value, 0)
        .single()
        .unwrap_or_else(Utc::now)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn migrations_and_repositories_work() {
        let cache = Cache::in_memory().unwrap();
        cache
            .upsert_streams(&[ZulipStream {
                id: 1,
                name: "core team".to_string(),
                description: Some("Core".to_string()),
            }])
            .unwrap();
        cache
            .upsert_messages(&[ZulipMessage {
                id: 9,
                message_type: ZulipMessageType::Stream,
                stream_id: Some(1),
                stream_name: "core team".to_string(),
                topic: "Release".to_string(),
                dm_conversation_key: None,
                dm_participants: Vec::new(),
                sender_id: Some(2),
                sender_name: "Ada".to_string(),
                sender_email: Some("ada@example.com".to_string()),
                timestamp: Utc::now(),
                rendered_html: "<p>installer bug?</p>".to_string(),
                plain_text: "installer bug?".to_string(),
                permalink: Some("https://example/9".to_string()),
                raw_json: json!({"id":9}),
            }])
            .unwrap();

        assert_eq!(cache.streams().unwrap().len(), 1);
        assert_eq!(cache.topics("core team").unwrap()[0].name, "Release");
        assert_eq!(cache.search_messages("installer").unwrap()[0].id, 9);
        assert_eq!(cache.max_message_id().unwrap(), Some(9));
    }

    #[test]
    fn authored_refresh_queue_rotates_by_fetched_at() {
        let cache = Cache::in_memory().unwrap();
        cache
            .upsert_messages(&[
                test_message(1, "ada@example.com"),
                test_message(2, "ada@example.com"),
                test_message(3, "other@example.com"),
            ])
            .unwrap();
        cache
            .conn
            .execute("UPDATE zulip_messages SET fetched_at = 100", [])
            .unwrap();

        assert_eq!(
            cache
                .authored_message_ids_for_refresh("ada@example.com", 10)
                .unwrap(),
            [2, 1]
        );

        cache.mark_messages_refreshed(&[2]).unwrap();

        assert_eq!(
            cache
                .authored_message_ids_for_refresh("ada@example.com", 10)
                .unwrap(),
            [1, 2]
        );
    }

    #[test]
    fn duplicate_hash_is_stable() {
        assert_eq!(send_hash("s", "t", "m"), send_hash("s", "t", "m"));
        assert_ne!(send_hash("s", "t", "m"), send_hash("s", "t", "m2"));
    }

    #[test]
    fn direct_conversations_group_and_fetch_messages() {
        let cache = Cache::in_memory().unwrap();
        let participants = vec![ZulipDmParticipant {
            id: Some(7),
            name: "Ada".to_string(),
            email: Some("ada@example.com".to_string()),
        }];
        cache
            .upsert_messages(&[
                direct_message(1, "id:7", participants.clone()),
                direct_message(2, "id:7", participants.clone()),
            ])
            .unwrap();

        let conversations = cache.direct_conversations().unwrap();
        assert_eq!(conversations.len(), 1);
        assert_eq!(conversations[0].key, "id:7");
        assert_eq!(conversations[0].message_count, 2);
        assert_eq!(cache.direct_message_count().unwrap(), 2);
        assert_eq!(
            cache
                .messages_for_direct_conversation("id:7")
                .unwrap()
                .iter()
                .map(|message| message.id)
                .collect::<Vec<_>>(),
            [1, 2]
        );
    }

    #[test]
    fn unread_counts_exclude_read_messages() {
        let cache = Cache::in_memory().unwrap();
        let participants = vec![ZulipDmParticipant {
            id: Some(7),
            name: "Ada".to_string(),
            email: Some("ada@example.com".to_string()),
        }];
        cache
            .upsert_messages(&[
                test_message(1, "ada@example.com"),
                test_message(2, "ada@example.com"),
                direct_message(10, "id:7", participants.clone()),
            ])
            .unwrap();

        assert_eq!(cache.unread_stream_activity().unwrap()[0].message_count, 2);
        assert_eq!(
            cache
                .unread_topic_counts("core team")
                .unwrap()
                .get("Release"),
            Some(&2)
        );
        assert_eq!(cache.unread_direct_message_count().unwrap(), 1);
        assert_eq!(
            cache
                .unread_direct_conversation_counts()
                .unwrap()
                .get("id:7"),
            Some(&1)
        );

        cache.mark_messages_read(&[1, 10]).unwrap();

        assert_eq!(cache.unread_stream_activity().unwrap()[0].message_count, 1);
        assert_eq!(
            cache
                .unread_topic_counts("core team")
                .unwrap()
                .get("Release"),
            Some(&1)
        );
        assert_eq!(cache.unread_direct_message_count().unwrap(), 0);
        assert!(cache
            .unread_direct_conversation_counts()
            .unwrap()
            .get("id:7")
            .is_none());
        assert_eq!(cache.read_message_ids(&[1, 2, 10]).unwrap(), [1, 10].into());
    }

    #[test]
    fn read_state_persists_after_reopening_cache() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("cache.sqlite");
        {
            let cache = Cache::open(&path).unwrap();
            cache
                .upsert_messages(&[
                    test_message(1, "ada@example.com"),
                    test_message(2, "ada@example.com"),
                ])
                .unwrap();
            cache.mark_messages_read(&[1]).unwrap();
        }

        let cache = Cache::open(&path).unwrap();

        assert_eq!(cache.read_message_ids(&[1, 2]).unwrap(), [1].into());
        assert_eq!(
            cache
                .unread_topic_counts("core team")
                .unwrap()
                .get("Release"),
            Some(&1)
        );
    }

    fn test_message(id: i64, sender_email: &str) -> ZulipMessage {
        ZulipMessage {
            id,
            message_type: ZulipMessageType::Stream,
            stream_id: Some(1),
            stream_name: "core team".to_string(),
            topic: "Release".to_string(),
            dm_conversation_key: None,
            dm_participants: Vec::new(),
            sender_id: Some(2),
            sender_name: "Ada".to_string(),
            sender_email: Some(sender_email.to_string()),
            timestamp: Utc.timestamp_opt(1_700_000_000 + id, 0).unwrap(),
            rendered_html: format!("<p>message {id}</p>"),
            plain_text: format!("message {id}"),
            permalink: Some(format!("https://example/{id}")),
            raw_json: json!({"id": id}),
        }
    }

    fn direct_message(id: i64, key: &str, participants: Vec<ZulipDmParticipant>) -> ZulipMessage {
        ZulipMessage {
            id,
            message_type: ZulipMessageType::Direct,
            stream_id: None,
            stream_name: "Direct messages".to_string(),
            topic: participants
                .iter()
                .map(|participant| participant.name.as_str())
                .collect::<Vec<_>>()
                .join(", "),
            dm_conversation_key: Some(key.to_string()),
            dm_participants: participants,
            sender_id: Some(2),
            sender_name: "Ada".to_string(),
            sender_email: Some("ada@example.com".to_string()),
            timestamp: Utc.timestamp_opt(1_700_000_000 + id, 0).unwrap(),
            rendered_html: format!("<p>dm {id}</p>"),
            plain_text: format!("dm {id}"),
            permalink: Some(format!("https://example/{id}")),
            raw_json: json!({"id": id}),
        }
    }
}
