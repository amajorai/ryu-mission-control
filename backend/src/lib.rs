//! `ryu-mission-control` — the project-level view over many chats.
//!
//! ## What this app is for
//!
//! A chat's transcript answers "what was said". The Mission Control panel in the
//! desktop answers "what did THIS chat do" — one card per turn, with the agent's
//! own rationale under each. This crate answers the question neither of those
//! can: **what has been happening across every chat, this week.** Recent
//! sessions and what each accomplished, per-day activity, the files several
//! chats keep returning to, and — the part that actually changes behaviour — the
//! to-dos left outstanding in chats nobody has reopened.
//!
//! ## Why the digest is computed in the client
//!
//! A manifest sidecar's callbacks into Core are exactly three:
//! `POST /api/host/model/complete`, `/api/host/rpc` and
//! `/api/host/capability/:cap` (`apps/core/src/sidecar/ext_proxy.rs:797`). None
//! of them reads a conversation, and the `parts` column those digests come from
//! is sealed at rest. So this process cannot see a chat, by design — and the one
//! place that already holds every message with its tool calls is the desktop.
//!
//! The desktop therefore derives the digest with
//! `apps/desktop/src/lib/mission-control/turn-groups.ts` and PUTs it here. That
//! function is the same one the in-chat panel renders from, so the panel and the
//! project page cannot disagree about a chat. Re-indexing is keyed on the
//! conversation's `updated_at`, which the client compares against
//! `GET /index-state`, so refreshing a dashboard over a hundred chats fetches
//! only the handful that moved.
//!
//! ## Degradation
//!
//! Everything except the narrative summary is an indexed fact. A node with no
//! model configured, or one where `hook:side-model` was never approved, gets a
//! fully working dashboard; only `POST /conversations/:id/summarize` reports
//! itself unavailable. That ordering is deliberate.
//!
//! ## Zero Core coupling
//!
//! This crate depends on no `apps/core` code. Core's only knowledge of it is the
//! single `include_str!` of its manifest in `BUILTIN_MANIFESTS`, and the desktop
//! reaches it through the manifest's `http.public_mount` over the generic
//! ext-proxy.

pub mod api;
pub mod host;
pub mod model;
pub mod store;
pub mod summary;

pub use api::{routes, Ctx};
pub use host::Host;
pub use store::{Filter, Store, DB_FILE_NAME};
