//! The SQLite store: one row per indexed conversation, plus a queryable table of
//! outstanding work items.
//!
//! Two tables rather than one blob because the two access patterns genuinely
//! differ. A conversation's turns and file list are only ever read whole, for one
//! conversation, so they are JSON columns. Outstanding items are read ACROSS
//! conversations, filtered and ordered — "what is still open in this project this
//! week" is the dashboard's central question — so they are rows.
//!
//! Indexing is a full replace inside one transaction: the digest row is upserted
//! and the conversation's items are deleted and re-inserted. A chat that lost a
//! to-do (the agent finished it) must not keep the stale row, and a merge would
//! have to guess which side won.
//!
//! At rest (see [`crate::seal_keys`]): `title`, `folder_path`, `headline`,
//! `summary`, `turns_json`, `files_json` and `work_item.content` are sealed per
//! value (`encb:v1:`, store `mission-control.db`), each bound to its
//! conversation id (and the item's ordinal). The folder filter compares a
//! blind token (`folder_path_bi`, exact match as before). Counts, timestamps,
//! statuses and agent ids stay plaintext: every dashboard aggregate runs on
//! them in SQL. Legacy plaintext is sealed when the store opens, then freed
//! pages are scrubbed; every connection runs `secure_delete = ON`.

use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension, Row};
use ryu_crypto::content_sqlite::{
    enable_secure_delete, finish_store, migrate_columns, run_pending_vacuum, ColumnPlan, KeyKind,
};
use ryu_crypto::{BlindIndex, ContentContext};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::seal_keys::{sqlite_store_seal, PluginKeys, StoreSeal};

use crate::model::{
    ConversationDetail, ConversationDigest, DayBucket, FileTouch, HotFile, IndexRequest,
    IndexStateRow, OpenItemRow, Totals, TurnRecord, WorkItem,
};

pub const DB_FILE_NAME: &str = "mission-control.db";

/// The registered content-store name (part of every key derivation; never rename).
pub const STORE: &str = "mission-control.db";
/// This app's canonical plugin id (the sealing key's scope).
pub const PLUGIN_ID: &str = "@ryu/mission-control";

/// The digest columns sealed per value, all bound to `conversation_id`.
const DIGEST_SEALED: [&str; 6] = [
    "title",
    "folder_path",
    "headline",
    "summary",
    "turns_json",
    "files_json",
];

fn digest_ctx(column: &str, id: &str) -> ContentContext {
    ContentContext::for_id(format!("conversation_digest.{column}"), id)
}

fn item_ctx(id: &str, ordinal: i64) -> ContentContext {
    ContentContext::new("work_item.content")
        .with("conversation_id", id)
        .with("ordinal", ordinal.to_string())
}

/// The digest columns, for the open-time migration (`work_item` has a
/// composite key and is migrated by [`Store::migrate_items`]).
pub fn column_plans() -> Result<Vec<ColumnPlan>> {
    DIGEST_SEALED
        .iter()
        .map(|column| {
            ColumnPlan::text(
                "conversation_digest",
                column,
                "conversation_id",
                KeyKind::Text,
            )
        })
        .collect()
}

/// One connection behind a mutex. The workload is a handful of writes per chat
/// and a few reads per dashboard open — a pool would be ceremony, and SQLite
/// serialises writers anyway.
#[derive(Clone)]
pub struct Store {
    conn: Arc<Mutex<Connection>>,
    seal: StoreSeal,
    folder_bi: Option<BlindIndex>,
    unreadable: Arc<AtomicU64>,
}

impl Store {
    /// Open the store, resolving this process's sealing key with the store's
    /// directory as the plugin data dir (see [`crate::seal_keys`]).
    pub fn open(path: impl AsRef<Path>) -> Result<Store> {
        let path = path.as_ref();
        let dir = path.parent().unwrap_or_else(|| Path::new("."));
        let keys = PluginKeys::resolve(PLUGIN_ID, dir)?;
        Self::open_with_keys(path, &keys)
    }

    /// [`Self::open`] with explicit keys.
    pub fn open_with_keys(path: impl AsRef<Path>, keys: &PluginKeys) -> Result<Store> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).ok();
        }
        let conn = Connection::open(path).with_context(|| format!("opening {}", path.display()))?;
        Self::from_connection(conn, keys, Some(path))
    }

    /// In-memory store under a random key, for tests.
    pub fn open_memory() -> Result<Store> {
        use base64::Engine as _;
        use rand::RngCore as _;
        let mut root = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut root);
        let cipher = ryu_crypto::PluginCipher::from_subkey_b64(
            PLUGIN_ID,
            &base64::engine::general_purpose::STANDARD.encode(root),
        )?;
        let keys = PluginKeys::sealing(cipher, crate::seal_keys::KeySource::Standalone);
        Self::from_connection(Connection::open_in_memory()?, &keys, None)
    }

    fn from_connection(
        mut conn: Connection,
        keys: &PluginKeys,
        path: Option<&Path>,
    ) -> Result<Store> {
        enable_secure_delete(&conn)?;
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA foreign_keys=ON;
             CREATE TABLE IF NOT EXISTS conversation_digest (
                 conversation_id   TEXT PRIMARY KEY,
                 title             TEXT,
                 folder_path       TEXT,
                 agent_id          TEXT,
                 source_updated_at INTEGER NOT NULL,
                 indexed_at        INTEGER NOT NULL,
                 turns             INTEGER NOT NULL DEFAULT 0,
                 writes            INTEGER NOT NULL DEFAULT 0,
                 commands          INTEGER NOT NULL DEFAULT 0,
                 failures          INTEGER NOT NULL DEFAULT 0,
                 tool_calls        INTEGER NOT NULL DEFAULT 0,
                 files_touched     INTEGER NOT NULL DEFAULT 0,
                 headline          TEXT,
                 turns_json        TEXT NOT NULL DEFAULT '[]',
                 files_json        TEXT NOT NULL DEFAULT '[]',
                 summary           TEXT,
                 summarized_at     INTEGER,
                 source_version    TEXT
             );
             CREATE INDEX IF NOT EXISTS idx_digest_updated
                 ON conversation_digest(source_updated_at DESC);
             CREATE INDEX IF NOT EXISTS idx_digest_folder
                 ON conversation_digest(folder_path);
             CREATE TABLE IF NOT EXISTS work_item (
                 conversation_id TEXT NOT NULL
                     REFERENCES conversation_digest(conversation_id) ON DELETE CASCADE,
                 ordinal         INTEGER NOT NULL,
                 content         TEXT NOT NULL,
                 status          TEXT NOT NULL,
                 PRIMARY KEY (conversation_id, ordinal)
             );
             CREATE INDEX IF NOT EXISTS idx_item_status ON work_item(status);",
        )
        .context("creating the mission-control schema")?;
        // Blind token of `folder_path` for the dashboard's exact folder filter
        // (the plaintext index on the sealed column goes away).
        if let Err(err) = conn.execute(
            "ALTER TABLE conversation_digest ADD COLUMN folder_path_bi TEXT",
            [],
        ) {
            if !err.to_string().contains("duplicate column name") {
                return Err(err).context("adding conversation_digest.folder_path_bi");
            }
        }
        conn.execute_batch(
            "DROP INDEX IF EXISTS idx_digest_folder;
             CREATE INDEX IF NOT EXISTS idx_digest_folder_bi
                 ON conversation_digest(folder_path_bi);",
        )
        .context("indexing conversation_digest.folder_path_bi")?;
        // Stores created before source versions existed. Their rows keep a NULL
        // version, which no scope entry ever matches: they stay unreadable until
        // the desktop re-indexes them under the conversation's current version.
        let has_version: bool = conn
            .prepare("SELECT 1 FROM pragma_table_info('conversation_digest') WHERE name = 'source_version'")?
            .exists([])?;
        if !has_version {
            conn.execute(
                "ALTER TABLE conversation_digest ADD COLUMN source_version TEXT",
                [],
            )
            .context("adding the source_version column")?;
        }
        let seal = sqlite_store_seal(keys, &conn, STORE)?;
        let folder_bi = if seal.is_sealing() {
            keys.blind_index(STORE, "folder_path")
        } else {
            None
        };
        let store = Store {
            conn: Arc::new(Mutex::new(Connection::open_in_memory()?)),
            seal,
            folder_bi,
            unreadable: Arc::new(AtomicU64::new(0)),
        };
        if matches!(store.seal, StoreSeal::Compat) {
            // Compat mode filters on `folder_path_bi` holding the folder itself;
            // rows indexed before this release have NULL there.
            conn.execute(
                "UPDATE conversation_digest SET folder_path_bi = folder_path
                 WHERE folder_path_bi IS NULL AND folder_path IS NOT NULL",
                [],
            )
            .context("backfilling the compat-mode folder filter")?;
        }
        if let StoreSeal::Sealing(cipher) = &store.seal {
            if let Some(path) = path {
                if let Err(error) = run_pending_vacuum(&conn, path, STORE) {
                    tracing::warn!("mission-control.db: deferred compaction failed: {error:#}");
                }
            }
            let backfilled = store.backfill_folder_tokens(&conn)?;
            let report = migrate_columns(&mut conn, cipher, &column_plans()?)
                .context("sealing legacy mission-control.db rows")?;
            let items = store.migrate_items(&conn)?;
            let unreadable = report.unreadable + items.1;
            if unreadable > 0 {
                tracing::warn!(
                    count = unreadable,
                    "mission-control.db: values that do not open were left untouched"
                );
            }
            if report.sealed + items.0 + backfilled > 0 {
                if let Some(path) = path {
                    if let Err(error) = finish_store(&conn, path, STORE) {
                        tracing::warn!(
                            "mission-control.db: scrubbing freed pages failed: {error:#}"
                        );
                    }
                }
            }
        }
        *store.conn.lock().expect("mission-control store poisoned") = conn;
        Ok(store)
    }

    /// The sealing health item: `{state, detail?, unreadable_rows}`.
    pub fn seal_health(&self) -> serde_json::Value {
        let mut health = self.seal.health();
        health["unreadable_rows"] = self.unreadable.load(Ordering::Relaxed).into();
        health
    }

    /// The token the folder filter compares (`None` without a folder filter).
    fn folder_token(&self, folder: Option<&str>) -> Option<String> {
        folder.map(|folder| match &self.folder_bi {
            Some(index) => index.token(folder),
            None => folder.to_owned(),
        })
    }

    /// Fill `folder_path_bi` for rows that have a folder and no token yet: rows
    /// written before sealing (NULL) or in compat mode (which stores the folder
    /// itself there, so the one filter query serves both modes). Returns the
    /// rows updated.
    fn backfill_folder_tokens(&self, conn: &Connection) -> Result<u64> {
        let Some(index) = &self.folder_bi else {
            return Ok(0);
        };
        let rows: Vec<(String, String)> = {
            let mut stmt = conn.prepare(
                "SELECT conversation_id, folder_path FROM conversation_digest
                 WHERE folder_path IS NOT NULL
                   AND (folder_path_bi IS NULL OR substr(folder_path_bi, 1, 4) <> 'bi1:')",
            )?;
            let rows = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?;
            rows.collect::<rusqlite::Result<_>>()?
        };
        let mut updated = 0;
        for (id, stored) in rows {
            let Ok(folder) = self
                .seal
                .open_text(&stored, &digest_ctx("folder_path", &id))
            else {
                continue;
            };
            updated += conn.execute(
                "UPDATE conversation_digest SET folder_path_bi = ?2
                 WHERE conversation_id = ?1 AND folder_path IS ?3",
                params![id, index.token(&folder), stored],
            )? as u64;
        }
        Ok(updated)
    }

    /// Seal legacy `work_item.content` values (composite key, so not a
    /// `ColumnPlan`): open each value, seal only legacy plaintext, compare-and-swap
    /// on the stored value. Returns `(sealed, unreadable)`; a second run seals
    /// nothing.
    fn migrate_items(&self, conn: &Connection) -> Result<(u64, u64)> {
        let rows: Vec<(String, i64, String)> = {
            let mut stmt =
                conn.prepare("SELECT conversation_id, ordinal, content FROM work_item")?;
            let rows = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?;
            rows.collect::<rusqlite::Result<_>>()?
        };
        let (mut sealed, mut unreadable) = (0, 0);
        for (id, ordinal, stored) in rows {
            let ctx = item_ctx(&id, ordinal);
            match self.seal.open_text_detailed(&stored, &ctx) {
                Ok((value, true)) => {
                    let new = self.seal.seal_text(&value, &ctx)?;
                    sealed += conn.execute(
                        "UPDATE work_item SET content = ?3
                         WHERE conversation_id = ?1 AND ordinal = ?2 AND content IS ?4",
                        params![id, ordinal, new, stored],
                    )? as u64;
                }
                Ok((_, false)) => {}
                Err(_) => unreadable += 1,
            }
        }
        Ok((sealed, unreadable))
    }

    fn skip_unreadable(&self, id: &str, error: &anyhow::Error) {
        let total = self.unreadable.fetch_add(1, Ordering::Relaxed) + 1;
        tracing::warn!(conversation = %id, total, "skipping a row that does not open: {error:#}");
    }

    /// Open the sealed fields of a digest read by [`read_digest`].
    fn open_digest(&self, mut digest: ConversationDigest) -> Result<ConversationDigest> {
        let id = digest.conversation_id.clone();
        let open = |column: &str, value: Option<String>| -> Result<Option<String>> {
            self.seal
                .open_opt(value.as_deref(), &digest_ctx(column, &id))
        };
        digest.title = open("title", digest.title.take())?;
        digest.folder_path = open("folder_path", digest.folder_path.take())?;
        digest.headline = open("headline", digest.headline.take())?;
        digest.summary = open("summary", digest.summary.take())?;
        Ok(digest)
    }

    /// Open listed digests, skipping (and counting) any that do not open.
    fn open_digests(&self, rows: Vec<ConversationDigest>) -> Vec<ConversationDigest> {
        rows.into_iter()
            .filter_map(|row| {
                let id = row.conversation_id.clone();
                self.open_digest(row)
                    .map_err(|error| self.skip_unreadable(&id, &error))
                    .ok()
            })
            .collect()
    }

    /// A non-sensitive reachability probe for the unauthenticated health route.
    pub fn ping(&self) -> Result<()> {
        let guard = self.conn.lock().expect("mission-control store poisoned");
        guard.query_row("SELECT 1", [], |_| Ok(()))?;
        Ok(())
    }

    /// Replace everything stored for one conversation at `version` (the source
    /// version Core stamped for it). Returns the stored digest.
    ///
    /// Replacing a row stored under a DIFFERENT version (a deleted conversation
    /// whose id was re-created) also drops that row's narrative summary, so
    /// nothing of the old conversation survives the overwrite.
    pub fn index(
        &self,
        id: &str,
        version: &str,
        req: &IndexRequest,
        now_ms: i64,
    ) -> Result<ConversationDigest> {
        self.seal.ensure_writable()?;
        let seal =
            |column: &str, value: Option<&str>| self.seal.seal_opt(value, &digest_ctx(column, id));
        let turns_json = self.seal.seal_text(
            &serde_json::to_string(&req.turns)?,
            &digest_ctx("turns_json", id),
        )?;
        let files_json = self.seal.seal_text(
            &serde_json::to_string(&req.files)?,
            &digest_ctx("files_json", id),
        )?;
        let headline = seal("headline", req.turns.last().map(|t| t.headline.as_str()))?;
        let title = seal("title", req.title.as_deref())?;
        let folder_path = seal("folder_path", req.folder_path.as_deref())?;
        let folder_path_bi = self.folder_token(req.folder_path.as_deref());
        let items = req
            .open_items
            .iter()
            .chain(req.done_items.iter())
            .enumerate()
            .map(|(ordinal, item)| {
                let ordinal = ordinal as i64;
                let content = self.seal.seal_text(&item.content, &item_ctx(id, ordinal))?;
                Ok((ordinal, content, item.status.clone()))
            })
            .collect::<Result<Vec<_>>>()?;

        let mut guard = self.conn.lock().expect("mission-control store poisoned");
        let tx = guard.transaction()?;
        tx.execute(
            "INSERT INTO conversation_digest (
                 conversation_id, title, folder_path, agent_id, source_updated_at,
                 indexed_at, turns, writes, commands, failures, tool_calls,
                 files_touched, headline, turns_json, files_json, folder_path_bi,
                 source_version
             ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17)
             ON CONFLICT(conversation_id) DO UPDATE SET
                 summary=CASE WHEN conversation_digest.source_version IS excluded.source_version
                              THEN conversation_digest.summary END,
                 summarized_at=CASE WHEN conversation_digest.source_version IS excluded.source_version
                                    THEN conversation_digest.summarized_at END,
                 source_version=excluded.source_version,
                 title=excluded.title,
                 folder_path=excluded.folder_path,
                 agent_id=excluded.agent_id,
                 source_updated_at=excluded.source_updated_at,
                 indexed_at=excluded.indexed_at,
                 turns=excluded.turns,
                 writes=excluded.writes,
                 commands=excluded.commands,
                 failures=excluded.failures,
                 tool_calls=excluded.tool_calls,
                 files_touched=excluded.files_touched,
                 headline=excluded.headline,
                 turns_json=excluded.turns_json,
                 files_json=excluded.files_json,
                 folder_path_bi=excluded.folder_path_bi",
            params![
                id,
                title,
                folder_path,
                req.agent_id,
                req.source_updated_at,
                now_ms,
                req.totals.turns,
                req.totals.writes,
                req.totals.commands,
                req.totals.failures,
                req.totals.tool_calls,
                req.totals.files_touched,
                headline,
                turns_json,
                files_json,
                folder_path_bi,
                version,
            ],
        )?;

        // A re-index never merges: the newest snapshot of the plan is the whole
        // truth, so the old rows go before the new ones land.
        tx.execute(
            "DELETE FROM work_item WHERE conversation_id = ?1",
            params![id],
        )?;
        {
            let mut stmt = tx.prepare(
                "INSERT INTO work_item (conversation_id, ordinal, content, status)
                 VALUES (?1,?2,?3,?4)",
            )?;
            for (ordinal, content, status) in &items {
                stmt.execute(params![id, ordinal, content, status])?;
            }
        }
        tx.commit()?;
        drop(guard);

        self.digest(id, version)?
            .context("the digest vanished immediately after being written")
    }

    /// The digest stored for `id` at exactly `version`. A row indexed under any
    /// other version belongs to a different (deleted) conversation: `None`.
    pub fn digest(&self, id: &str, version: &str) -> Result<Option<ConversationDigest>> {
        let guard = self.conn.lock().expect("mission-control store poisoned");
        let row = guard
            .query_row(
                &format!("{DIGEST_SELECT} WHERE d.conversation_id = ?1 AND d.source_version = ?2"),
                params![id, version],
                read_digest,
            )
            .optional()?;
        drop(guard);
        row.map(|row| self.open_digest(row)).transpose()
    }

    pub fn detail(&self, id: &str, version: &str) -> Result<Option<ConversationDetail>> {
        let Some(digest) = self.digest(id, version)? else {
            return Ok(None);
        };
        let guard = self.conn.lock().expect("mission-control store poisoned");
        let (turns_json, files_json): (String, String) = guard.query_row(
            "SELECT turns_json, files_json FROM conversation_digest WHERE conversation_id = ?1",
            params![id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        let mut open_items = Vec::new();
        let mut done_items = Vec::new();
        {
            let mut stmt = guard.prepare(
                "SELECT ordinal, content, status FROM work_item
                 WHERE conversation_id = ?1 ORDER BY ordinal",
            )?;
            let rows = stmt.query_map(params![id], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                ))
            })?;
            for item in rows {
                let (ordinal, content, status) = item?;
                let item = WorkItem {
                    content: self.seal.open_text(&content, &item_ctx(id, ordinal))?,
                    status,
                };
                if item.status == "completed" {
                    done_items.push(item);
                } else {
                    open_items.push(item);
                }
            }
        }
        drop(guard);
        let turns_json = self
            .seal
            .open_text(&turns_json, &digest_ctx("turns_json", id))?;
        let files_json = self
            .seal
            .open_text(&files_json, &digest_ctx("files_json", id))?;
        Ok(Some(ConversationDetail {
            digest,
            // A malformed blob is a bug in a past write, not a reason to 500 a
            // dashboard: serve the row with an empty list and keep the totals.
            turns: serde_json::from_str::<Vec<TurnRecord>>(&turns_json).unwrap_or_default(),
            files: serde_json::from_str::<Vec<FileTouch>>(&files_json).unwrap_or_default(),
            open_items,
            done_items,
        }))
    }

    pub fn delete(&self, id: &str) -> Result<bool> {
        let guard = self.conn.lock().expect("mission-control store poisoned");
        let n = guard.execute(
            "DELETE FROM conversation_digest WHERE conversation_id = ?1",
            params![id],
        )?;
        Ok(n > 0)
    }

    pub fn set_summary(&self, id: &str, version: &str, summary: &str, now_ms: i64) -> Result<bool> {
        let summary = self.seal.seal_text(summary, &digest_ctx("summary", id))?;
        let guard = self.conn.lock().expect("mission-control store poisoned");
        let n = guard.execute(
            "UPDATE conversation_digest SET summary = ?3, summarized_at = ?4
             WHERE conversation_id = ?1 AND source_version = ?2",
            params![id, version, summary, now_ms],
        )?;
        Ok(n > 0)
    }

    /// `authorized_ids_json` is Core's scope: a JSON array of `<version>:<id>`.
    pub fn index_state(&self, authorized_ids_json: &str) -> Result<Vec<IndexStateRow>> {
        let guard = self.conn.lock().expect("mission-control store poisoned");
        let mut stmt = guard.prepare(
            "SELECT conversation_id, source_updated_at, summary IS NOT NULL
             FROM conversation_digest
             WHERE (source_version || ':' || conversation_id) IN (SELECT value FROM json_each(?1))",
        )?;
        let rows = stmt.query_map(params![authorized_ids_json], |r| {
            Ok(IndexStateRow {
                conversation_id: r.get(0)?,
                source_updated_at: r.get(1)?,
                summarized: r.get::<_, i64>(2)? != 0,
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Conversations matching the window, newest first.
    pub fn conversations(&self, filter: &Filter) -> Result<Vec<ConversationDigest>> {
        let guard = self.conn.lock().expect("mission-control store poisoned");
        let sql = format!(
            "{DIGEST_SELECT} {} ORDER BY d.source_updated_at DESC LIMIT ?4",
            filter.where_clause()
        );
        let mut stmt = guard.prepare(&sql)?;
        let folder = self.folder_token(filter.folder_path.as_deref());
        let rows = stmt.query_map(
            params![
                filter.since_ms,
                folder,
                filter.authorized_ids_json,
                filter.limit
            ],
            read_digest,
        )?;
        let rows = rows.collect::<rusqlite::Result<Vec<_>>>()?;
        drop(stmt);
        drop(guard);
        Ok(self.open_digests(rows))
    }

    /// Per-local-day activity over the window, oldest first so a chart reads
    /// left-to-right without the client re-sorting.
    pub fn days(&self, filter: &Filter) -> Result<Vec<DayBucket>> {
        let guard = self.conn.lock().expect("mission-control store poisoned");
        let sql = format!(
            "SELECT date(d.source_updated_at / 1000, 'unixepoch', 'localtime') AS day,
                    COUNT(*), COALESCE(SUM(d.turns),0), COALESCE(SUM(d.writes),0),
                    COALESCE(SUM(d.failures),0)
             FROM conversation_digest d {}
             GROUP BY day ORDER BY day ASC",
            filter.where_clause()
        );
        let mut stmt = guard.prepare(&sql)?;
        let folder = self.folder_token(filter.folder_path.as_deref());
        let rows = stmt.query_map(
            params![filter.since_ms, folder, filter.authorized_ids_json],
            |r| {
                Ok(DayBucket {
                    date: r.get(0)?,
                    conversations: r.get(1)?,
                    turns: r.get(2)?,
                    writes: r.get(3)?,
                    failures: r.get(4)?,
                })
            },
        )?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Outstanding work across the window. Ordered by how recently its chat moved:
    /// a to-do from this morning is more actionable than one from last month.
    pub fn open_items(&self, filter: &Filter) -> Result<Vec<OpenItemRow>> {
        let guard = self.conn.lock().expect("mission-control store poisoned");
        let sql = format!(
            "SELECT i.conversation_id, d.title, d.folder_path, i.content, i.status,
                    d.source_updated_at, i.ordinal
             FROM work_item i
             JOIN conversation_digest d ON d.conversation_id = i.conversation_id
             {} AND i.status <> 'completed'
             ORDER BY d.source_updated_at DESC, i.ordinal ASC
             LIMIT ?4",
            filter.where_clause()
        );
        let mut stmt = guard.prepare(&sql)?;
        let folder = self.folder_token(filter.folder_path.as_deref());
        let rows = stmt.query_map(
            params![
                filter.since_ms,
                folder,
                filter.authorized_ids_json,
                filter.item_limit
            ],
            |r| {
                Ok((
                    OpenItemRow {
                        conversation_id: r.get(0)?,
                        conversation_title: r.get(1)?,
                        folder_path: r.get(2)?,
                        content: r.get(3)?,
                        status: r.get(4)?,
                        source_updated_at: r.get(5)?,
                    },
                    r.get::<_, i64>(6)?,
                ))
            },
        )?;
        let rows = rows.collect::<rusqlite::Result<Vec<_>>>()?;
        drop(stmt);
        drop(guard);
        let mut out = Vec::with_capacity(rows.len());
        for (mut row, ordinal) in rows {
            let id = row.conversation_id.clone();
            let opened = (|| -> Result<()> {
                row.conversation_title = self
                    .seal
                    .open_opt(row.conversation_title.as_deref(), &digest_ctx("title", &id))?;
                row.folder_path = self
                    .seal
                    .open_opt(row.folder_path.as_deref(), &digest_ctx("folder_path", &id))?;
                row.content = self.seal.open_text(&row.content, &item_ctx(&id, ordinal))?;
                Ok(())
            })();
            match opened {
                Ok(()) => out.push(row),
                Err(error) => self.skip_unreadable(&id, &error),
            }
        }
        Ok(out)
    }
}

/// The window every dashboard query shares. `since_ms` and `folder_path` are
/// bound as parameters and NULL-checked in SQL rather than concatenated, so an
/// absent filter is one code path instead of a second query string. The folder
/// is compared through its blind token (`folder_path_bi`): the store computes
/// the token from `folder_path` before binding it.
#[derive(Debug, Clone)]
pub struct Filter {
    pub since_ms: Option<i64>,
    pub folder_path: Option<String>,
    /// JSON array of `<version>:<id>` entries minted by Core from the caller's
    /// current conversation ACL. A row matches only under the version it was
    /// indexed at. Empty by default so any caller that forgets to provide scope
    /// fails closed.
    pub authorized_ids_json: String,
    pub limit: i64,
    pub item_limit: i64,
}

impl Default for Filter {
    fn default() -> Self {
        Filter {
            since_ms: None,
            folder_path: None,
            authorized_ids_json: "[]".to_owned(),
            limit: 100,
            item_limit: 200,
        }
    }
}

impl Filter {
    /// `?1` = since_ms, `?2` = folder_path, `?3` = authorized ids. Always emits
    /// a WHERE so callers can append `AND …` without knowing whether a filter was set.
    fn where_clause(&self) -> &'static str {
        "WHERE (?1 IS NULL OR d.source_updated_at >= ?1)
           AND (?2 IS NULL OR d.folder_path_bi = ?2)
           AND (d.source_version || ':' || d.conversation_id) IN (SELECT value FROM json_each(?3))"
    }
}

const DIGEST_SELECT: &str = "SELECT d.conversation_id, d.title, d.folder_path, d.agent_id,
        d.source_updated_at, d.indexed_at, d.turns, d.writes, d.commands, d.failures,
        d.tool_calls, d.files_touched, d.headline, d.summary, d.summarized_at,
        (SELECT COUNT(*) FROM work_item w
          WHERE w.conversation_id = d.conversation_id AND w.status <> 'completed'),
        (SELECT COUNT(*) FROM work_item w
          WHERE w.conversation_id = d.conversation_id AND w.status = 'completed')
   FROM conversation_digest d";

fn read_digest(r: &Row<'_>) -> rusqlite::Result<ConversationDigest> {
    Ok(ConversationDigest {
        conversation_id: r.get(0)?,
        title: r.get(1)?,
        folder_path: r.get(2)?,
        agent_id: r.get(3)?,
        source_updated_at: r.get(4)?,
        indexed_at: r.get(5)?,
        totals: Totals {
            turns: r.get(6)?,
            writes: r.get(7)?,
            commands: r.get(8)?,
            failures: r.get(9)?,
            tool_calls: r.get(10)?,
            files_touched: r.get(11)?,
        },
        headline: r.get(12)?,
        summary: r.get(13)?,
        summarized_at: r.get(14)?,
        open_count: r.get(15)?,
        done_count: r.get(16)?,
    })
}

/// Roll the per-conversation file lists up into "who touched this path, and how
/// many different chats did". Done in Rust, not SQL: the paths live in a JSON
/// blob because per-conversation they are only read whole, and normalising them
/// into a table to serve one dashboard section would cost a write per path on
/// every re-index.
pub fn hot_files(details: &[ConversationDetail], limit: usize) -> Vec<HotFile> {
    use std::collections::HashMap;
    struct Acc {
        kind: String,
        touches: i64,
        conversations: i64,
    }
    // A write is a more interesting fact about a path than a read, so it wins
    // when different chats did different things to the same file.
    fn rank(kind: &str) -> i64 {
        match kind {
            "create" => 2,
            "edit" => 1,
            _ => 0,
        }
    }
    let mut acc: HashMap<String, Acc> = HashMap::new();
    for detail in details {
        for touch in &detail.files {
            let entry = acc.entry(touch.path.clone()).or_insert(Acc {
                kind: touch.kind.clone(),
                touches: 0,
                conversations: 0,
            });
            entry.touches += touch.count.max(1);
            entry.conversations += 1;
            if rank(&touch.kind) > rank(&entry.kind) {
                entry.kind = touch.kind.clone();
            }
        }
    }
    let mut out: Vec<HotFile> = acc
        .into_iter()
        .map(|(path, a)| HotFile {
            path,
            kind: a.kind,
            touches: a.touches,
            conversations: a.conversations,
        })
        .collect();
    // Total order (conversations, touches, path) so the same data always renders
    // in the same order — a list that reshuffles between refreshes reads as churn.
    out.sort_by(|a, b| {
        b.conversations
            .cmp(&a.conversations)
            .then(b.touches.cmp(&a.touches))
            .then(a.path.cmp(&b.path))
    });
    out.truncate(limit);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Totals;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn req(updated: i64, open: &[&str], done: &[&str]) -> IndexRequest {
        IndexRequest {
            title: Some("Ship auth".into()),
            folder_path: Some("/repo".into()),
            agent_id: None,
            source_updated_at: updated,
            totals: Totals {
                turns: 2,
                writes: 1,
                commands: 3,
                failures: 1,
                tool_calls: 7,
                files_touched: 1,
            },
            turns: vec![TurnRecord {
                index: 1,
                headline: "Changed auth.ts".into(),
                request: "add auth".into(),
                rationale: "the expiry check is off by one".into(),
                outcome: "done".into(),
                status: "ok".into(),
                files: vec![],
            }],
            files: vec![FileTouch {
                path: "/repo/auth.ts".into(),
                kind: "edit".into(),
                count: 2,
            }],
            open_items: open
                .iter()
                .map(|c| WorkItem {
                    content: (*c).into(),
                    status: "pending".into(),
                })
                .collect(),
            done_items: done
                .iter()
                .map(|c| WorkItem {
                    content: (*c).into(),
                    status: "completed".into(),
                })
                .collect(),
        }
    }

    /// The source version every test row is indexed under.
    const V: &str = "0123456789ab";

    /// Core's scope encoding for `ids`, all at version [`V`].
    fn scope_json(ids: &[&str]) -> String {
        let entries: Vec<String> = ids.iter().map(|id| format!("{V}:{id}")).collect();
        serde_json::to_string(&entries).unwrap()
    }

    fn scoped_filter(ids: &[&str]) -> Filter {
        Filter {
            authorized_ids_json: scope_json(ids),
            ..Default::default()
        }
    }

    #[test]
    fn indexing_stores_totals_and_the_latest_headline() {
        let store = Store::open_memory().unwrap();
        let digest = store
            .index("c1", V, &req(1000, &["ship it"], &[]), 5)
            .unwrap();
        assert_eq!(digest.totals.turns, 2);
        assert_eq!(digest.headline.as_deref(), Some("Changed auth.ts"));
        assert_eq!(digest.open_count, 1);
        assert_eq!(digest.done_count, 0);
        assert_eq!(digest.indexed_at, 5);
    }

    #[test]
    fn re_indexing_replaces_items_rather_than_accumulating_them() {
        let store = Store::open_memory().unwrap();
        store
            .index("c1", V, &req(1000, &["a", "b"], &[]), 1)
            .unwrap();
        let digest = store.index("c1", V, &req(2000, &["b"], &["a"]), 2).unwrap();
        assert_eq!(digest.open_count, 1);
        assert_eq!(digest.done_count, 1);
        let detail = store.detail("c1", V).unwrap().unwrap();
        assert_eq!(detail.open_items.len(), 1);
        assert_eq!(detail.open_items[0].content, "b");
    }

    #[test]
    fn a_summary_survives_but_re_indexing_keeps_it() {
        let store = Store::open_memory().unwrap();
        store.index("c1", V, &req(1000, &[], &[]), 1).unwrap();
        assert!(store.set_summary("c1", V, "shipped auth", 9).unwrap());
        store.index("c1", V, &req(2000, &[], &[]), 2).unwrap();
        let digest = store.digest("c1", V).unwrap().unwrap();
        assert_eq!(digest.summary.as_deref(), Some("shipped auth"));
        assert_eq!(digest.summarized_at, Some(9));
    }

    #[test]
    fn the_window_filters_by_time_and_folder() {
        let store = Store::open_memory().unwrap();
        store.index("old", V, &req(1_000, &[], &[]), 1).unwrap();
        store.index("new", V, &req(9_000, &[], &[]), 1).unwrap();
        let mut other = req(9_000, &[], &[]);
        other.folder_path = Some("/elsewhere".into());
        store.index("other", V, &other, 1).unwrap();

        let recent = store
            .conversations(&Filter {
                since_ms: Some(5_000),
                ..scoped_filter(&["old", "new", "other"])
            })
            .unwrap();
        assert_eq!(recent.len(), 2);

        let scoped = store
            .conversations(&Filter {
                since_ms: Some(5_000),
                folder_path: Some("/repo".into()),
                ..scoped_filter(&["old", "new", "other"])
            })
            .unwrap();
        assert_eq!(scoped.len(), 1);
        assert_eq!(scoped[0].conversation_id, "new");
    }

    #[test]
    fn open_items_exclude_completed_work_and_carry_their_chat() {
        let store = Store::open_memory().unwrap();
        store
            .index("c1", V, &req(1000, &["write the docs"], &["ship it"]), 1)
            .unwrap();
        let items = store.open_items(&scoped_filter(&["c1"])).unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].content, "write the docs");
        assert_eq!(items[0].conversation_title.as_deref(), Some("Ship auth"));
    }

    #[test]
    fn deleting_a_conversation_takes_its_items_with_it() {
        let store = Store::open_memory().unwrap();
        store.index("c1", V, &req(1000, &["x"], &[]), 1).unwrap();
        assert!(store.delete("c1").unwrap());
        assert!(store.digest("c1", V).unwrap().is_none());
        assert!(store
            .open_items(&scoped_filter(&["c1"]))
            .unwrap()
            .is_empty());
        assert!(!store.delete("c1").unwrap());
    }

    #[test]
    fn index_state_reports_staleness_keys() {
        let store = Store::open_memory().unwrap();
        store.index("c1", V, &req(1234, &[], &[]), 1).unwrap();
        let state = store.index_state(&scope_json(&["c1"])).unwrap();
        assert_eq!(state.len(), 1);
        assert_eq!(state[0].source_updated_at, 1234);
        assert!(!state[0].summarized);
    }

    #[test]
    fn hot_files_count_distinct_conversations_and_prefer_the_stronger_touch() {
        let store = Store::open_memory().unwrap();
        let mut a = req(1000, &[], &[]);
        a.files = vec![FileTouch {
            path: "/repo/auth.ts".into(),
            kind: "read".into(),
            count: 1,
        }];
        let mut b = req(2000, &[], &[]);
        b.files = vec![
            FileTouch {
                path: "/repo/auth.ts".into(),
                kind: "edit".into(),
                count: 3,
            },
            FileTouch {
                path: "/repo/solo.ts".into(),
                kind: "create".into(),
                count: 9,
            },
        ];
        store.index("a", V, &a, 1).unwrap();
        store.index("b", V, &b, 1).unwrap();

        let details: Vec<_> = ["a", "b"]
            .iter()
            .map(|id| store.detail(id, V).unwrap().unwrap())
            .collect();
        let hot = hot_files(&details, 10);
        assert_eq!(hot[0].path, "/repo/auth.ts");
        assert_eq!(hot[0].conversations, 2);
        assert_eq!(hot[0].touches, 4);
        assert_eq!(hot[0].kind, "edit");
        assert_eq!(hot[1].path, "/repo/solo.ts");
    }

    #[test]
    fn days_bucket_by_local_date() {
        let store = Store::open_memory().unwrap();
        // Two chats on the same instant land in one bucket; a much later one does not.
        store
            .index("a", V, &req(1_700_000_000_000, &[], &[]), 1)
            .unwrap();
        store
            .index("b", V, &req(1_700_000_000_000, &[], &[]), 1)
            .unwrap();
        store
            .index("c", V, &req(1_800_000_000_000, &[], &[]), 1)
            .unwrap();
        let days = store.days(&scoped_filter(&["a", "b", "c"])).unwrap();
        assert_eq!(days.len(), 2);
        assert_eq!(days[0].conversations, 2);
        assert_eq!(days[0].turns, 4);
        // Oldest first, so a chart reads left to right.
        assert!(days[0].date < days[1].date);
    }

    #[test]
    fn file_store_survives_reopen_and_replacement_stays_atomic() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "ryu-mission-control-store-{}-{nonce}",
            std::process::id()
        ));
        let path = dir.join(DB_FILE_NAME);

        {
            let store = Store::open(&path).expect("open new store");
            store
                .index("durable", V, &req(1_000, &["first"], &[]), 10)
                .expect("initial index");
            assert!(store
                .set_summary("durable", V, "retained narrative", 11)
                .expect("set summary"));
        }

        {
            let reopened = Store::open(&path).expect("reopen store");
            let detail = reopened
                .detail("durable", V)
                .expect("read reopened store")
                .expect("durable row");
            assert_eq!(detail.digest.summary.as_deref(), Some("retained narrative"));
            assert_eq!(detail.open_items[0].content, "first");

            reopened
                .index("durable", V, &req(2_000, &["replacement"], &[]), 12)
                .expect("replace after reopen");
            let replaced = reopened.detail("durable", V).unwrap().unwrap();
            assert_eq!(replaced.open_items.len(), 1);
            assert_eq!(replaced.open_items[0].content, "replacement");
            assert_eq!(
                replaced.digest.summary.as_deref(),
                Some("retained narrative")
            );
        }

        std::fs::remove_dir_all(dir).expect("remove isolated test store");
    }

    #[test]
    fn concurrent_distinct_conversation_writes_are_not_lost() {
        let store = Store::open_memory().unwrap();
        let workers = (0..8)
            .map(|index| {
                let store = store.clone();
                std::thread::spawn(move || {
                    let id = format!("chat-{index}");
                    store
                        .index(&id, V, &req(index, &["pending"], &[]), index)
                        .expect("concurrent index");
                })
            })
            .collect::<Vec<_>>();
        for worker in workers {
            worker.join().expect("writer thread");
        }

        let ids = (0..8)
            .map(|index| format!("chat-{index}"))
            .collect::<Vec<_>>();
        let refs = ids.iter().map(String::as_str).collect::<Vec<_>>();
        let filter = scoped_filter(&refs);
        let rows = store.conversations(&filter).unwrap();
        assert_eq!(rows.len(), 8);
        assert_eq!(store.open_items(&filter).unwrap().len(), 8);
    }

    #[test]
    fn aggregate_queries_are_limited_to_the_authorized_conversation_set() {
        let store = Store::open_memory().unwrap();
        store
            .index("mine", V, &req(2_000, &["visible"], &[]), 1)
            .unwrap();
        store
            .index("theirs", V, &req(3_000, &["secret"], &[]), 1)
            .unwrap();

        let filter = scoped_filter(&["mine"]);
        let conversations = store.conversations(&filter).unwrap();
        assert_eq!(conversations.len(), 1);
        assert_eq!(conversations[0].conversation_id, "mine");
        assert_eq!(store.days(&filter).unwrap()[0].conversations, 1);
        assert_eq!(store.open_items(&filter).unwrap()[0].content, "visible");

        let state = store.index_state(&scope_json(&["mine"])).unwrap();
        assert_eq!(state.len(), 1);
        assert_eq!(state[0].conversation_id, "mine");
    }

    /// A conversation deleted in Core and re-created under the same id has a new
    /// source version. The old digest must be unreadable under it, by id or in any
    /// aggregate, and a re-index under the new version must not inherit the old
    /// narrative summary.
    #[test]
    fn a_re_created_conversation_never_reads_the_old_digest() {
        const NEW: &str = "ba9876543210";
        let store = Store::open_memory().unwrap();
        store
            .index("x", V, &req(1_000, &["alice's secret plan"], &[]), 1)
            .unwrap();
        assert!(store
            .set_summary("x", V, "alice's private narrative", 2)
            .unwrap());

        assert!(store.digest("x", NEW).unwrap().is_none());
        assert!(store.detail("x", NEW).unwrap().is_none());
        assert!(!store.set_summary("x", NEW, "overwrite", 3).unwrap());
        let new_scope = serde_json::to_string(&[format!("{NEW}:x")]).unwrap();
        let filter = Filter {
            authorized_ids_json: new_scope.clone(),
            ..Default::default()
        };
        assert!(store.conversations(&filter).unwrap().is_empty());
        assert!(store.open_items(&filter).unwrap().is_empty());
        assert!(store.days(&filter).unwrap().is_empty());
        assert!(store.index_state(&new_scope).unwrap().is_empty());

        // The new owner indexes their own conversation: the old row, items and
        // summary are replaced, not merged.
        store
            .index("x", NEW, &req(2_000, &["bob's plan"], &[]), 4)
            .unwrap();
        let detail = store.detail("x", NEW).unwrap().unwrap();
        assert_eq!(detail.digest.summary, None);
        assert_eq!(detail.digest.summarized_at, None);
        assert_eq!(detail.open_items.len(), 1);
        assert_eq!(detail.open_items[0].content, "bob's plan");
        assert!(store.detail("x", V).unwrap().is_none());
    }

    /// Rows written before source versions existed have none, and match no scope.
    #[test]
    fn a_legacy_unversioned_row_is_unreadable_until_re_indexed() {
        let store = Store::open_memory().unwrap();
        store
            .index("legacy", V, &req(1_000, &["old"], &[]), 1)
            .unwrap();
        store
            .conn
            .lock()
            .unwrap()
            .execute("UPDATE conversation_digest SET source_version = NULL", [])
            .unwrap();
        assert!(store.detail("legacy", V).unwrap().is_none());
        assert!(store
            .index_state(&scope_json(&["legacy"]))
            .unwrap()
            .is_empty());
        assert!(store
            .conversations(&scoped_filter(&["legacy"]))
            .unwrap()
            .is_empty());
    }

    // ── at rest ──────────────────────────────────────────────────────────────

    const SECRET: &str = "zebra-mission-marker";

    fn keys(n: u8) -> PluginKeys {
        use base64::Engine as _;
        let subkey = base64::engine::general_purpose::STANDARD.encode([n; 32]);
        PluginKeys::sealing(
            ryu_crypto::PluginCipher::from_subkey_b64(PLUGIN_ID, &subkey).unwrap(),
            crate::seal_keys::KeySource::Delivered,
        )
    }

    fn db_path() -> std::path::PathBuf {
        use rand::RngCore as _;
        let mut nonce = [0u8; 8];
        rand::thread_rng().fill_bytes(&mut nonce);
        let name: String = nonce.iter().map(|b| format!("{b:02x}")).collect();
        let dir = std::env::temp_dir().join(format!("ryu-mc-seal-{name}"));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join(DB_FILE_NAME)
    }

    fn disk_has(path: &Path, needle: &str) -> bool {
        let mut bytes = std::fs::read(path).unwrap_or_default();
        let mut wal = path.as_os_str().to_owned();
        wal.push("-wal");
        bytes.extend(std::fs::read(std::path::PathBuf::from(wal)).unwrap_or_default());
        bytes.windows(needle.len()).any(|w| w == needle.as_bytes())
    }

    fn secret_req() -> IndexRequest {
        let mut r = req(1000, &[&format!("todo {SECRET}")], &[]);
        r.title = Some(format!("title {SECRET}"));
        r.folder_path = Some(format!("/repo/{SECRET}"));
        r.turns[0].headline = format!("headline {SECRET}");
        r.turns[0].request = format!("request {SECRET}");
        r.files[0].path = format!("/repo/{SECRET}/a.ts");
        r
    }

    fn assert_sealed(store: &Store, n: u8) {
        let conn = store.conn.lock().unwrap();
        let cipher = keys(n).content_cipher(STORE).unwrap();
        ryu_crypto::content_sqlite::assert_all_open(&conn, &cipher, &column_plans().unwrap())
            .unwrap();
        let mut stmt = conn
            .prepare("SELECT conversation_id, ordinal, content FROM work_item")
            .unwrap();
        let rows = stmt
            .query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, String>(2)?,
                ))
            })
            .unwrap();
        for row in rows {
            let (id, ordinal, content) = row.unwrap();
            let opened = cipher.open_text(&content, &item_ctx(&id, ordinal)).unwrap();
            assert!(
                !opened.needs_reseal,
                "work_item {id}/{ordinal} is not sealed"
            );
        }
    }

    #[test]
    fn content_is_sealed_on_disk_and_the_folder_filter_still_matches() {
        let path = db_path();
        let store = Store::open_with_keys(&path, &keys(1)).unwrap();
        store.index("c1", V, &secret_req(), 1).unwrap();
        store
            .set_summary("c1", V, &format!("summary {SECRET}"), 2)
            .unwrap();
        let detail = store.detail("c1", V).unwrap().unwrap();
        assert_eq!(
            detail.digest.title.as_deref(),
            Some(format!("title {SECRET}").as_str())
        );
        assert_eq!(detail.turns[0].request, format!("request {SECRET}"));
        assert_eq!(detail.open_items[0].content, format!("todo {SECRET}"));
        let scoped = Filter {
            authorized_ids_json: scope_json(&["c1", "c2", "legacy"]),
            folder_path: Some(format!("/repo/{SECRET}")),
            ..Default::default()
        };
        assert_eq!(store.conversations(&scoped).unwrap().len(), 1);
        assert_eq!(store.open_items(&scoped).unwrap().len(), 1);
        assert_eq!(store.days(&scoped).unwrap().len(), 1);
        let other = Filter {
            authorized_ids_json: scope_json(&["c1", "c2", "legacy"]),
            folder_path: Some("/repo".into()),
            ..Default::default()
        };
        assert!(store.conversations(&other).unwrap().is_empty());
        assert!(!disk_has(&path, SECRET));
        assert_sealed(&store, 1);
    }

    #[test]
    fn legacy_plaintext_is_migrated_once_and_still_reads() {
        let path = db_path();
        {
            // A pre-sealing store, written in compat mode (plaintext, folder in the
            // token column), then a pre-phase-2 row with no token at all.
            let store = Store::open_with_keys(&path, &PluginKeys::compat()).unwrap();
            assert_eq!(store.seal_health()["state"], "compat");
            store.index("c1", V, &secret_req(), 1).unwrap();
            store.set_summary("c1", V, SECRET, 2).unwrap();
            store.index("c2", V, &secret_req(), 1).unwrap();
            store
                .conn
                .lock()
                .unwrap()
                .execute("UPDATE conversation_digest SET folder_path_bi = NULL WHERE conversation_id = 'c2'", [])
                .unwrap();
            assert!(disk_has(&path, SECRET));
        }
        let store = Store::open_with_keys(&path, &keys(2)).unwrap();
        assert_sealed(&store, 2);
        assert!(!disk_has(&path, SECRET));
        let scoped = Filter {
            authorized_ids_json: scope_json(&["c1", "c2", "legacy"]),
            folder_path: Some(format!("/repo/{SECRET}")),
            ..Default::default()
        };
        assert_eq!(store.conversations(&scoped).unwrap().len(), 2);
        assert_eq!(
            store.digest("c1", V).unwrap().unwrap().summary.as_deref(),
            Some(SECRET)
        );
        let snapshot = |store: &Store| -> Vec<String> {
            let conn = store.conn.lock().unwrap();
            let mut stmt = conn
                .prepare("SELECT title || turns_json || folder_path_bi FROM conversation_digest UNION ALL SELECT content FROM work_item")
                .unwrap();
            let rows = stmt.query_map([], |r| r.get(0)).unwrap();
            rows.map(Result::unwrap).collect()
        };
        let first = snapshot(&store);
        drop(store);
        let store = Store::open_with_keys(&path, &keys(2)).unwrap();
        assert_eq!(snapshot(&store), first, "a second open must change nothing");
    }

    #[test]
    fn compat_mode_folder_filter_finds_rows_indexed_before_the_token_column() {
        let path = db_path();
        {
            let store = Store::open_with_keys(&path, &PluginKeys::compat()).unwrap();
            store.index("legacy", V, &secret_req(), 1).unwrap();
            // A row indexed before this release: no folder token at all.
            store
                .conn
                .lock()
                .unwrap()
                .execute("UPDATE conversation_digest SET folder_path_bi = NULL", [])
                .unwrap();
        }
        // Reopened under an older Core that delivers no key.
        let store = Store::open_with_keys(&path, &PluginKeys::compat()).unwrap();
        assert_eq!(store.seal_health()["state"], "compat");
        let scoped = Filter {
            authorized_ids_json: scope_json(&["c1", "c2", "legacy"]),
            folder_path: Some(format!("/repo/{SECRET}")),
            ..Default::default()
        };
        assert_eq!(store.conversations(&scoped).unwrap().len(), 1);
        assert_eq!(store.open_items(&scoped).unwrap().len(), 1);
        assert_eq!(store.days(&scoped).unwrap().len(), 1);
        drop(store);
        // A later key still seals it and the filter keeps matching.
        let sealed = Store::open_with_keys(&path, &keys(3)).unwrap();
        assert_eq!(sealed.conversations(&scoped).unwrap().len(), 1);
        assert!(!disk_has(&path, SECRET));
    }

    #[test]
    fn a_value_copied_to_another_row_does_not_open_and_lists_skip_it() {
        let store = Store::open_memory().unwrap();
        store.index("a", V, &req(1000, &["x"], &[]), 1).unwrap();
        store.index("b", V, &req(2000, &["y"], &[]), 1).unwrap();
        store
            .conn
            .lock()
            .unwrap()
            .execute(
                "UPDATE conversation_digest SET title = (SELECT title FROM conversation_digest WHERE conversation_id = 'a') WHERE conversation_id = 'b'",
                [],
            )
            .unwrap();
        assert!(store.digest("b", V).is_err());
        assert_eq!(
            store
                .conversations(&scoped_filter(&["a", "b"]))
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            store.open_items(&scoped_filter(&["a", "b"])).unwrap().len(),
            1
        );
        assert_eq!(store.seal_health()["unreadable_rows"], 2);
    }

    #[test]
    fn another_key_or_no_key_refuses_writes_and_keeps_data() {
        let path = db_path();
        Store::open_with_keys(&path, &keys(4))
            .unwrap()
            .index("c1", V, &secret_req(), 1)
            .unwrap();
        let wrong = Store::open_with_keys(&path, &keys(5)).unwrap();
        assert_eq!(wrong.seal_health()["state"], "unhealthy");
        assert!(wrong.index("c2", V, &secret_req(), 1).is_err());
        assert!(wrong.set_summary("c1", V, "x", 1).is_err());
        drop(wrong);
        let none = Store::open_with_keys(&path, &PluginKeys::compat()).unwrap();
        assert_eq!(none.seal_health()["state"], "unhealthy");
        assert!(none.index("c2", V, &secret_req(), 1).is_err());
        drop(none);
        let back = Store::open_with_keys(&path, &keys(4)).unwrap();
        assert_eq!(
            back.digest("c1", V).unwrap().unwrap().title.as_deref(),
            Some(format!("title {SECRET}").as_str())
        );
    }
}
