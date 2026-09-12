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

use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension, Row};
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::model::{
    ConversationDetail, ConversationDigest, DayBucket, FileTouch, HotFile, IndexRequest,
    IndexStateRow, OpenItemRow, Totals, TurnRecord, WorkItem,
};

pub const DB_FILE_NAME: &str = "mission-control.db";

/// One connection behind a mutex. The workload is a handful of writes per chat
/// and a few reads per dashboard open — a pool would be ceremony, and SQLite
/// serialises writers anyway.
#[derive(Clone)]
pub struct Store {
    conn: Arc<Mutex<Connection>>,
}

impl Store {
    pub fn open(path: impl AsRef<Path>) -> Result<Store> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).ok();
        }
        let conn = Connection::open(path).with_context(|| format!("opening {}", path.display()))?;
        Self::from_connection(conn)
    }

    /// In-memory store, for tests.
    pub fn open_memory() -> Result<Store> {
        Self::from_connection(Connection::open_in_memory()?)
    }

    fn from_connection(conn: Connection) -> Result<Store> {
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
                 summarized_at     INTEGER
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
        Ok(Store {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Replace everything stored for one conversation. Returns the stored digest.
    pub fn index(&self, id: &str, req: &IndexRequest, now_ms: i64) -> Result<ConversationDigest> {
        let turns_json = serde_json::to_string(&req.turns)?;
        let files_json = serde_json::to_string(&req.files)?;
        let headline = req.turns.last().map(|t| t.headline.clone());

        let mut guard = self.conn.lock().expect("mission-control store poisoned");
        let tx = guard.transaction()?;
        tx.execute(
            "INSERT INTO conversation_digest (
                 conversation_id, title, folder_path, agent_id, source_updated_at,
                 indexed_at, turns, writes, commands, failures, tool_calls,
                 files_touched, headline, turns_json, files_json
             ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15)
             ON CONFLICT(conversation_id) DO UPDATE SET
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
                 files_json=excluded.files_json",
            params![
                id,
                req.title,
                req.folder_path,
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
            ],
        )?;

        // A re-index never merges: the newest snapshot of the plan is the whole
        // truth, so the old rows go before the new ones land.
        tx.execute(
            "DELETE FROM work_item WHERE conversation_id = ?1",
            params![id],
        )?;
        let mut ordinal = 0i64;
        {
            let mut stmt = tx.prepare(
                "INSERT INTO work_item (conversation_id, ordinal, content, status)
                 VALUES (?1,?2,?3,?4)",
            )?;
            for item in req.open_items.iter().chain(req.done_items.iter()) {
                stmt.execute(params![id, ordinal, item.content, item.status])?;
                ordinal += 1;
            }
        }
        tx.commit()?;
        drop(guard);

        self.digest(id)?
            .context("the digest vanished immediately after being written")
    }

    pub fn digest(&self, id: &str) -> Result<Option<ConversationDigest>> {
        let guard = self.conn.lock().expect("mission-control store poisoned");
        let row = guard
            .query_row(
                &format!("{DIGEST_SELECT} WHERE d.conversation_id = ?1"),
                params![id],
                read_digest,
            )
            .optional()?;
        Ok(row)
    }

    pub fn detail(&self, id: &str) -> Result<Option<ConversationDetail>> {
        let Some(digest) = self.digest(id)? else {
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
                "SELECT content, status FROM work_item
                 WHERE conversation_id = ?1 ORDER BY ordinal",
            )?;
            let rows = stmt.query_map(params![id], |r| {
                Ok(WorkItem {
                    content: r.get(0)?,
                    status: r.get(1)?,
                })
            })?;
            for item in rows {
                let item = item?;
                if item.status == "completed" {
                    done_items.push(item);
                } else {
                    open_items.push(item);
                }
            }
        }
        drop(guard);
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

    pub fn set_summary(&self, id: &str, summary: &str, now_ms: i64) -> Result<bool> {
        let guard = self.conn.lock().expect("mission-control store poisoned");
        let n = guard.execute(
            "UPDATE conversation_digest SET summary = ?2, summarized_at = ?3
             WHERE conversation_id = ?1",
            params![id, summary, now_ms],
        )?;
        Ok(n > 0)
    }

    pub fn index_state(&self) -> Result<Vec<IndexStateRow>> {
        let guard = self.conn.lock().expect("mission-control store poisoned");
        let mut stmt = guard.prepare(
            "SELECT conversation_id, source_updated_at, summary IS NOT NULL
             FROM conversation_digest",
        )?;
        let rows = stmt.query_map([], |r| {
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
            "{DIGEST_SELECT} {} ORDER BY d.source_updated_at DESC LIMIT ?3",
            filter.where_clause()
        );
        let mut stmt = guard.prepare(&sql)?;
        let rows = stmt.query_map(
            params![filter.since_ms, filter.folder_path, filter.limit],
            read_digest,
        )?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
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
        let rows = stmt.query_map(params![filter.since_ms, filter.folder_path], |r| {
            Ok(DayBucket {
                date: r.get(0)?,
                conversations: r.get(1)?,
                turns: r.get(2)?,
                writes: r.get(3)?,
                failures: r.get(4)?,
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Outstanding work across the window. Ordered by how recently its chat moved:
    /// a to-do from this morning is more actionable than one from last month.
    pub fn open_items(&self, filter: &Filter) -> Result<Vec<OpenItemRow>> {
        let guard = self.conn.lock().expect("mission-control store poisoned");
        let sql = format!(
            "SELECT i.conversation_id, d.title, d.folder_path, i.content, i.status,
                    d.source_updated_at
             FROM work_item i
             JOIN conversation_digest d ON d.conversation_id = i.conversation_id
             {} AND i.status <> 'completed'
             ORDER BY d.source_updated_at DESC, i.ordinal ASC
             LIMIT ?3",
            filter.where_clause()
        );
        let mut stmt = guard.prepare(&sql)?;
        let rows = stmt.query_map(
            params![filter.since_ms, filter.folder_path, filter.item_limit],
            |r| {
                Ok(OpenItemRow {
                    conversation_id: r.get(0)?,
                    conversation_title: r.get(1)?,
                    folder_path: r.get(2)?,
                    content: r.get(3)?,
                    status: r.get(4)?,
                    source_updated_at: r.get(5)?,
                })
            },
        )?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }
}

/// The window every dashboard query shares. `since_ms` and `folder_path` are
/// bound as parameters and NULL-checked in SQL rather than concatenated, so an
/// absent filter is one code path instead of a second query string.
#[derive(Debug, Clone)]
pub struct Filter {
    pub since_ms: Option<i64>,
    pub folder_path: Option<String>,
    pub limit: i64,
    pub item_limit: i64,
}

impl Default for Filter {
    fn default() -> Self {
        Filter {
            since_ms: None,
            folder_path: None,
            limit: 100,
            item_limit: 200,
        }
    }
}

impl Filter {
    /// `?1` = since_ms, `?2` = folder_path. Always emits a WHERE so callers can
    /// append `AND …` without knowing whether a filter was set.
    fn where_clause(&self) -> &'static str {
        "WHERE (?1 IS NULL OR d.source_updated_at >= ?1)
           AND (?2 IS NULL OR d.folder_path = ?2)"
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

    #[test]
    fn indexing_stores_totals_and_the_latest_headline() {
        let store = Store::open_memory().unwrap();
        let digest = store.index("c1", &req(1000, &["ship it"], &[]), 5).unwrap();
        assert_eq!(digest.totals.turns, 2);
        assert_eq!(digest.headline.as_deref(), Some("Changed auth.ts"));
        assert_eq!(digest.open_count, 1);
        assert_eq!(digest.done_count, 0);
        assert_eq!(digest.indexed_at, 5);
    }

    #[test]
    fn re_indexing_replaces_items_rather_than_accumulating_them() {
        let store = Store::open_memory().unwrap();
        store.index("c1", &req(1000, &["a", "b"], &[]), 1).unwrap();
        let digest = store.index("c1", &req(2000, &["b"], &["a"]), 2).unwrap();
        assert_eq!(digest.open_count, 1);
        assert_eq!(digest.done_count, 1);
        let detail = store.detail("c1").unwrap().unwrap();
        assert_eq!(detail.open_items.len(), 1);
        assert_eq!(detail.open_items[0].content, "b");
    }

    #[test]
    fn a_summary_survives_but_re_indexing_keeps_it() {
        let store = Store::open_memory().unwrap();
        store.index("c1", &req(1000, &[], &[]), 1).unwrap();
        assert!(store.set_summary("c1", "shipped auth", 9).unwrap());
        store.index("c1", &req(2000, &[], &[]), 2).unwrap();
        let digest = store.digest("c1").unwrap().unwrap();
        assert_eq!(digest.summary.as_deref(), Some("shipped auth"));
        assert_eq!(digest.summarized_at, Some(9));
    }

    #[test]
    fn the_window_filters_by_time_and_folder() {
        let store = Store::open_memory().unwrap();
        store.index("old", &req(1_000, &[], &[]), 1).unwrap();
        store.index("new", &req(9_000, &[], &[]), 1).unwrap();
        let mut other = req(9_000, &[], &[]);
        other.folder_path = Some("/elsewhere".into());
        store.index("other", &other, 1).unwrap();

        let recent = store
            .conversations(&Filter {
                since_ms: Some(5_000),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(recent.len(), 2);

        let scoped = store
            .conversations(&Filter {
                since_ms: Some(5_000),
                folder_path: Some("/repo".into()),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(scoped.len(), 1);
        assert_eq!(scoped[0].conversation_id, "new");
    }

    #[test]
    fn open_items_exclude_completed_work_and_carry_their_chat() {
        let store = Store::open_memory().unwrap();
        store
            .index("c1", &req(1000, &["write the docs"], &["ship it"]), 1)
            .unwrap();
        let items = store.open_items(&Filter::default()).unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].content, "write the docs");
        assert_eq!(items[0].conversation_title.as_deref(), Some("Ship auth"));
    }

    #[test]
    fn deleting_a_conversation_takes_its_items_with_it() {
        let store = Store::open_memory().unwrap();
        store.index("c1", &req(1000, &["x"], &[]), 1).unwrap();
        assert!(store.delete("c1").unwrap());
        assert!(store.digest("c1").unwrap().is_none());
        assert!(store.open_items(&Filter::default()).unwrap().is_empty());
        assert!(!store.delete("c1").unwrap());
    }

    #[test]
    fn index_state_reports_staleness_keys() {
        let store = Store::open_memory().unwrap();
        store.index("c1", &req(1234, &[], &[]), 1).unwrap();
        let state = store.index_state().unwrap();
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
        store.index("a", &a, 1).unwrap();
        store.index("b", &b, 1).unwrap();

        let details: Vec<_> = ["a", "b"]
            .iter()
            .map(|id| store.detail(id).unwrap().unwrap())
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
            .index("a", &req(1_700_000_000_000, &[], &[]), 1)
            .unwrap();
        store
            .index("b", &req(1_700_000_000_000, &[], &[]), 1)
            .unwrap();
        store
            .index("c", &req(1_800_000_000_000, &[], &[]), 1)
            .unwrap();
        let days = store.days(&Filter::default()).unwrap();
        assert_eq!(days.len(), 2);
        assert_eq!(days[0].conversations, 2);
        assert_eq!(days[0].turns, 4);
        // Oldest first, so a chart reads left to right.
        assert!(days[0].date < days[1].date);
    }
}
