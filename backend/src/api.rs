//! The HTTP surface Core proxies as `/api/mission-control/*`.
//!
//! Paths here are RELATIVE to that mount; the binary nests them under it, so the
//! generic ext-proxy forwards unchanged and the desktop page reaches the sidecar
//! with no per-app Core coupling. Every path served here must also appear in the
//! manifest's `http.routes[]` — Core's ext-proxy 404s an undeclared path before
//! it ever reaches this process.
//!
//! Two families, split by whether a model is involved:
//!
//! * **Indexed** — `PUT /conversations/:id`, `GET /conversations/:id`,
//!   `GET /overview`, `GET /index-state`, `DELETE /conversations/:id`. Pure
//!   store reads and writes. These work on a node with no model configured, and
//!   they are everything the dashboard needs to be useful.
//! * **Model-backed** — `POST /conversations/:id/summarize`. Calls back into Core
//!   for a completion (see `host.rs`) and stores the narrative alongside the
//!   digest. Unavailable rather than silently empty when there is no host.

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::json;

use crate::host::Host;
use crate::model::{IndexRequest, Overview, OverviewTotals, Totals};
use crate::store::{hot_files, Filter, Store};
use crate::summary;

/// Hot-file rows served on the overview. A dashboard section, not a file tree.
const MAX_HOT_FILES: usize = 25;
/// Upper bound a caller's `limit` is clamped to, so one query cannot ask for the
/// whole store.
const MAX_LIMIT: i64 = 500;

pub struct Ctx {
    pub store: Store,
    /// `None` when the process was not spawned by Core: the indexed routes still
    /// work, and `/summarize` reports 503 instead of pretending.
    pub host: Option<Host>,
}

pub fn routes(ctx: Arc<Ctx>) -> Router {
    Router::new()
        .route("/overview", get(overview))
        .route("/index-state", get(index_state))
        .route("/conversations", get(conversations))
        .route(
            "/conversations/:id",
            put(index_conversation).get(get_conversation).delete(delete_conversation),
        )
        .route("/conversations/:id/summarize", post(summarize))
        .with_state(ctx)
}

// ── errors ───────────────────────────────────────────────────────────────────

struct ApiError(StatusCode, String);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({ "error": self.1 }))).into_response()
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(err: anyhow::Error) -> Self {
        // `{err:#}` keeps the context chain, which is what makes a store failure
        // diagnosable from the response instead of just "500".
        tracing::warn!("mission-control: {err:#}");
        ApiError(StatusCode::INTERNAL_SERVER_ERROR, format!("{err:#}"))
    }
}

fn not_found() -> ApiError {
    ApiError(
        StatusCode::NOT_FOUND,
        "no digest has been indexed for this conversation".to_owned(),
    )
}

type ApiResult<T> = Result<T, ApiError>;

// ── health ───────────────────────────────────────────────────────────────────

/// The un-gated loopback probe. Reports store reachability and nothing about the
/// user's work — Core hits this before auth, so it must leak nothing.
pub async fn health(store: Store) -> Response {
    match store.index_state() {
        Ok(rows) => Json(json!({
            "ok": true,
            "service": "mission-control",
            "indexed": rows.len(),
        }))
        .into_response(),
        Err(err) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "ok": false, "error": format!("{err:#}") })),
        )
            .into_response(),
    }
}

// ── window ───────────────────────────────────────────────────────────────────

/// The dashboard's window. `days` is the ergonomic form ("the last 7 days") and
/// `since_ms` the exact one; `days` wins when both are sent, because a UI that
/// offers a day picker should not have to also compute an instant.
#[derive(Debug, Deserialize)]
pub struct WindowQuery {
    #[serde(default)]
    days: Option<i64>,
    #[serde(default)]
    since_ms: Option<i64>,
    #[serde(default)]
    folder_path: Option<String>,
    #[serde(default)]
    limit: Option<i64>,
}

impl WindowQuery {
    fn to_filter(&self, now_ms: i64) -> Filter {
        let since_ms = match self.days {
            // A non-positive day count means "no window", not "the last zero
            // days" — clamping it to now would silently serve an empty page.
            Some(days) if days > 0 => Some(now_ms - days * 86_400_000),
            _ => self.since_ms,
        };
        let default = Filter::default();
        let limit = self
            .limit
            .filter(|n| *n > 0)
            .unwrap_or(default.limit)
            .min(MAX_LIMIT);
        Filter {
            since_ms,
            folder_path: self
                .folder_path
                .as_ref()
                .map(|f| f.trim().to_owned())
                .filter(|f| !f.is_empty()),
            limit,
            item_limit: default.item_limit,
        }
    }
}

// ── handlers ─────────────────────────────────────────────────────────────────

async fn index_conversation(
    State(ctx): State<Arc<Ctx>>,
    Path(id): Path<String>,
    Json(req): Json<IndexRequest>,
) -> ApiResult<Response> {
    let digest = ctx.store.index(&id, &req, now_ms())?;
    Ok(Json(digest).into_response())
}

async fn get_conversation(
    State(ctx): State<Arc<Ctx>>,
    Path(id): Path<String>,
) -> ApiResult<Response> {
    let detail = ctx.store.detail(&id)?.ok_or_else(not_found)?;
    Ok(Json(detail).into_response())
}

async fn delete_conversation(
    State(ctx): State<Arc<Ctx>>,
    Path(id): Path<String>,
) -> ApiResult<Response> {
    let removed = ctx.store.delete(&id)?;
    Ok(Json(json!({ "removed": removed })).into_response())
}

async fn conversations(
    State(ctx): State<Arc<Ctx>>,
    Query(window): Query<WindowQuery>,
) -> ApiResult<Response> {
    let rows = ctx.store.conversations(&window.to_filter(now_ms()))?;
    Ok(Json(json!({ "conversations": rows })).into_response())
}

async fn index_state(State(ctx): State<Arc<Ctx>>) -> ApiResult<Response> {
    Ok(Json(json!({ "conversations": ctx.store.index_state()? })).into_response())
}

async fn overview(
    State(ctx): State<Arc<Ctx>>,
    Query(window): Query<WindowQuery>,
) -> ApiResult<Response> {
    let filter = window.to_filter(now_ms());
    let conversations = ctx.store.conversations(&filter)?;
    let days = ctx.store.days(&filter)?;
    let open_items = ctx.store.open_items(&filter)?;

    let mut work = Totals::default();
    for row in &conversations {
        work.add(&row.totals);
    }

    // `files_touched` is a per-conversation count, so summing it double-counts a
    // path two chats both edited. The rolled-up list is the honest denominator.
    let details = conversations
        .iter()
        .filter_map(|row| ctx.store.detail(&row.conversation_id).ok().flatten())
        .collect::<Vec<_>>();
    let files = hot_files(&details, MAX_HOT_FILES);
    work.files_touched = files.len() as i64;

    Ok(Json(Overview {
        since_ms: filter.since_ms,
        folder_path: filter.folder_path.clone(),
        totals: OverviewTotals {
            work,
            conversations: conversations.len() as i64,
            open_items: open_items.len() as i64,
        },
        days,
        conversations,
        open_items,
        files,
    })
    .into_response())
}

async fn summarize(
    State(ctx): State<Arc<Ctx>>,
    Path(id): Path<String>,
) -> ApiResult<Response> {
    let Some(host) = ctx.host.as_ref() else {
        return Err(ApiError(
            StatusCode::SERVICE_UNAVAILABLE,
            "this node cannot reach a model: the sidecar was not started by Core".to_owned(),
        ));
    };
    let detail = ctx.store.detail(&id)?.ok_or_else(not_found)?;
    let prompt = summary::build_prompt(&detail);
    let text = host
        .complete(summary::SYSTEM, &prompt, Some("auto-title-model"))
        .await
        .map_err(|err| {
            // Not a 500: the store is fine and the digest is intact. The model
            // leg failed, and the caller can retry it alone.
            ApiError(StatusCode::BAD_GATEWAY, format!("{err:#}"))
        })?;
    let normalized = summary::normalize(&text);
    let stamped = now_ms();
    if !ctx.store.set_summary(&id, &normalized, stamped)? {
        return Err(not_found());
    }
    Ok(Json(json!({ "summary": normalized, "summarized_at": stamped })).into_response())
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

#[cfg(test)]
mod tests {
    use super::*;

    const DAY_MS: i64 = 86_400_000;

    fn query(days: Option<i64>, since_ms: Option<i64>, limit: Option<i64>) -> WindowQuery {
        WindowQuery {
            days,
            since_ms,
            folder_path: None,
            limit,
        }
    }

    #[test]
    fn days_wins_over_an_explicit_instant() {
        let filter = query(Some(7), Some(1), None).to_filter(10 * DAY_MS);
        assert_eq!(filter.since_ms, Some(3 * DAY_MS));
    }

    #[test]
    fn a_non_positive_day_count_means_no_window_rather_than_an_empty_page() {
        assert_eq!(query(Some(0), None, None).to_filter(DAY_MS).since_ms, None);
        assert_eq!(query(Some(-3), None, None).to_filter(DAY_MS).since_ms, None);
    }

    #[test]
    fn an_explicit_instant_is_used_when_no_day_count_is_given() {
        let filter = query(None, Some(42), None).to_filter(DAY_MS);
        assert_eq!(filter.since_ms, Some(42));
    }

    #[test]
    fn the_limit_is_clamped_and_a_junk_value_falls_back_to_the_default() {
        assert_eq!(query(None, None, Some(9_999)).to_filter(0).limit, MAX_LIMIT);
        assert_eq!(
            query(None, None, Some(0)).to_filter(0).limit,
            Filter::default().limit
        );
        assert_eq!(query(None, None, Some(25)).to_filter(0).limit, 25);
    }

    #[test]
    fn a_blank_folder_filter_is_dropped_rather_than_matching_nothing() {
        let window = WindowQuery {
            days: None,
            since_ms: None,
            folder_path: Some("   ".into()),
            limit: None,
        };
        assert_eq!(window.to_filter(0).folder_path, None);
    }
}
