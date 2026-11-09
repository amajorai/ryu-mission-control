//! The wire shapes Mission Control stores and serves.
//!
//! The digest itself is computed in the DESKTOP, not here, by
//! `apps/desktop/src/lib/mission-control/turn-groups.ts`. That is not a layering
//! accident: a sidecar's callback surface into Core is `model/complete`, `rpc`
//! and `capability/:cap` (`apps/core/src/sidecar/ext_proxy.rs:797`) — there is no
//! conversation read, and the `parts` column is sealed at rest. So the one
//! process that can see a conversation's tool calls is the client that already
//! has them on screen, and this crate's job is to remember what that client
//! derived and to answer questions across many chats that no single chat can.
//!
//! One consequence worth stating: the same TypeScript function produces the
//! in-chat panel AND the rows indexed here, so a chat summarised in the panel and
//! the same chat on the project page cannot disagree.
//!
//! Field names are snake_case on the wire (Rust's default, matching `ryu-ugc`).

use serde::{Deserialize, Serialize};

/// How a conversation touched one path. Mirrors `MissionTouchKind` in the
/// desktop extractor; kept as a plain string because an older sidecar must not
/// reject a kind a newer desktop learned to report.
pub type TouchKind = String;

#[derive(Debug, Clone, Default, Serialize, Deserialize, utoipa::ToSchema)]
pub struct Totals {
    #[serde(default)]
    pub turns: i64,
    #[serde(default)]
    pub writes: i64,
    #[serde(default)]
    pub commands: i64,
    #[serde(default)]
    pub failures: i64,
    #[serde(default)]
    pub tool_calls: i64,
    #[serde(default)]
    pub files_touched: i64,
}

impl Totals {
    pub fn add(&mut self, other: &Totals) {
        self.turns += other.turns;
        self.writes += other.writes;
        self.commands += other.commands;
        self.failures += other.failures;
        self.tool_calls += other.tool_calls;
        self.files_touched += other.files_touched;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct FileTouch {
    pub path: String,
    pub kind: TouchKind,
    #[serde(default)]
    pub count: i64,
}

/// One turn as the panel renders it, minus the chip lists the project page has
/// no room for. `rationale` is the agent's own pre-tool prose — the "why" — and
/// is the field the whole feature exists to carry across days.
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct TurnRecord {
    #[serde(default)]
    pub index: i64,
    pub headline: String,
    #[serde(default)]
    pub request: String,
    #[serde(default)]
    pub rationale: String,
    #[serde(default)]
    pub outcome: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub files: Vec<FileTouch>,
}

/// An outstanding piece of work. Sourced from the conversation's last TodoWrite
/// snapshot, which is why it needs no model to be trustworthy.
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct WorkItem {
    pub content: String,
    #[serde(default)]
    pub status: String,
}

/// What the desktop PUTs for one conversation. Everything is a full replacement:
/// re-indexing a chat that grew by one turn rewrites the row, so there is no
/// merge semantics to get wrong and no way for a stale turn to survive an edit.
#[derive(Debug, Clone, Deserialize, utoipa::ToSchema)]
pub struct IndexRequest {
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub folder_path: Option<String>,
    #[serde(default)]
    pub agent_id: Option<String>,
    /// The conversation's `updated_at` from Core, in epoch milliseconds. The
    /// staleness key: the desktop skips re-indexing a chat whose stored value
    /// still matches, so a backfill over a hundred chats fetches almost none.
    #[serde(default)]
    pub source_updated_at: i64,
    #[serde(default)]
    pub totals: Totals,
    #[serde(default)]
    pub turns: Vec<TurnRecord>,
    #[serde(default)]
    pub files: Vec<FileTouch>,
    #[serde(default)]
    pub open_items: Vec<WorkItem>,
    #[serde(default)]
    pub done_items: Vec<WorkItem>,
}

/// A stored conversation, as the project page lists it.
#[derive(Debug, Clone, Serialize)]
pub struct ConversationDigest {
    pub conversation_id: String,
    pub title: Option<String>,
    pub folder_path: Option<String>,
    pub agent_id: Option<String>,
    pub source_updated_at: i64,
    pub indexed_at: i64,
    pub totals: Totals,
    /// The most recent turn's headline — the one-liner for the list row.
    pub headline: Option<String>,
    /// A narrative written by the node's side model, when one has been asked for.
    /// `None` means nobody asked; it never means the model said nothing.
    pub summary: Option<String>,
    pub summarized_at: Option<i64>,
    pub open_count: i64,
    pub done_count: i64,
}

/// A digest plus the detail the list view omits.
#[derive(Debug, Clone, Serialize)]
pub struct ConversationDetail {
    #[serde(flatten)]
    pub digest: ConversationDigest,
    pub turns: Vec<TurnRecord>,
    pub files: Vec<FileTouch>,
    pub open_items: Vec<WorkItem>,
    pub done_items: Vec<WorkItem>,
}

/// One local calendar day of activity, for the dashboard's per-day strip.
#[derive(Debug, Clone, Serialize)]
pub struct DayBucket {
    /// `YYYY-MM-DD` in the NODE's local timezone — "what did I do Tuesday" is a
    /// question about the user's Tuesday, not UTC's.
    pub date: String,
    pub conversations: i64,
    pub turns: i64,
    pub writes: i64,
    pub failures: i64,
}

/// An outstanding item with the chat it came from, so the dashboard can say
/// where to go and not merely what is undone.
#[derive(Debug, Clone, Serialize)]
pub struct OpenItemRow {
    pub conversation_id: String,
    pub conversation_title: Option<String>,
    pub folder_path: Option<String>,
    pub content: String,
    pub status: String,
    pub source_updated_at: i64,
}

/// A path with how many DISTINCT conversations touched it — the signal that says
/// "three different chats have been editing this file this week".
#[derive(Debug, Clone, Serialize)]
pub struct HotFile {
    pub path: String,
    pub kind: TouchKind,
    pub touches: i64,
    pub conversations: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct OverviewTotals {
    #[serde(flatten)]
    pub work: Totals,
    pub conversations: i64,
    pub open_items: i64,
}

/// The whole project page in one response. Deliberately one request: every
/// section is a different projection of the same filtered row set, and four
/// endpoints would let them disagree mid-refresh.
#[derive(Debug, Clone, Serialize)]
pub struct Overview {
    pub since_ms: Option<i64>,
    pub folder_path: Option<String>,
    pub totals: OverviewTotals,
    pub days: Vec<DayBucket>,
    pub conversations: Vec<ConversationDigest>,
    pub open_items: Vec<OpenItemRow>,
    pub files: Vec<HotFile>,
}

/// What the desktop needs to decide which chats to re-index: one row per stored
/// conversation, nothing else. Cheap enough to fetch on every dashboard open.
#[derive(Debug, Clone, Serialize)]
pub struct IndexStateRow {
    pub conversation_id: String,
    pub source_updated_at: i64,
    pub summarized: bool,
}
