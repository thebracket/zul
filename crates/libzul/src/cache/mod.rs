use crate::zulip::models::{ZulipMessage, ZulipStream, ZulipTopic};
use anyhow::{Context, Result};
use chrono::{DateTime, TimeZone, Utc};
use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;
use sha2::{Digest, Sha256};
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
    stream_id INTEGER,
    stream_name TEXT NOT NULL,
    topic TEXT NOT NULL,
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
        self.conn.execute(
            "INSERT OR REPLACE INTO app_metadata (key, value) VALUES ('schema_version', '1')",
            [],
        )?;
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
                    id, stream_id, stream_name, topic, sender_id, sender_name, sender_email,
                    timestamp, rendered_html, plain_text, raw_json, permalink, fetched_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
                 ON CONFLICT(id) DO UPDATE SET
                    stream_id=excluded.stream_id,
                    stream_name=excluded.stream_name,
                    topic=excluded.topic,
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
                    message.stream_id,
                    message.stream_name,
                    message.topic,
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
                    timestamp, rendered_html, plain_text, raw_json, permalink
             FROM zulip_messages
             WHERE stream_name = ?1 AND topic = ?2
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
                    timestamp, rendered_html, plain_text, raw_json, permalink
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

    /// Search cached messages with SQLite FTS.
    pub fn search_messages(&self, query: &str) -> Result<Vec<ZulipMessage>> {
        let mut stmt = self.conn.prepare(
            "SELECT m.id, m.stream_id, m.stream_name, m.topic, m.sender_id, m.sender_name,
                    m.sender_email, m.timestamp, m.rendered_html, m.plain_text, m.raw_json,
                    m.permalink
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
            .prepare("SELECT stream_name, COUNT(*) FROM zulip_messages GROUP BY stream_name")?;
        let rows = stmt.query_map([], |row| {
            Ok(StreamActivity {
                stream_name: row.get(0)?,
                message_count: row.get(1)?,
            })
        })?;
        let activity = rows.collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(activity)
    }

    fn search_messages_like(&self, query: &str) -> Result<Vec<ZulipMessage>> {
        let like = format!("%{}%", query);
        let mut stmt = self.conn.prepare(
            "SELECT id, stream_id, stream_name, topic, sender_id, sender_name, sender_email,
                    timestamp, rendered_html, plain_text, raw_json, permalink
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
    Ok(ZulipMessage {
        id: row.get(0)?,
        stream_id: row.get(1)?,
        stream_name: row.get(2)?,
        topic: row.get(3)?,
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
                stream_id: Some(1),
                stream_name: "core team".to_string(),
                topic: "Release".to_string(),
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
    fn duplicate_hash_is_stable() {
        assert_eq!(send_hash("s", "t", "m"), send_hash("s", "t", "m"));
        assert_ne!(send_hash("s", "t", "m"), send_hash("s", "t", "m2"));
    }
}
