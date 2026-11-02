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

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::json;

use crate::host::Host;
// `FileTouch`/`TurnRecord`/`WorkItem` are imported for the OpenAPI
// `components(schemas(...))` list alone — they are the transitive graph under
// `IndexRequest`, and utoipa needs the name in scope to register it.
use crate::model::{
    FileTouch, IndexRequest, Overview, OverviewTotals, Totals, TurnRecord, WorkItem,
};
use crate::store::{hot_files, Filter, Store};
use crate::summary;

/// Hot-file rows served on the overview. A dashboard section, not a file tree.
const MAX_HOT_FILES: usize = 25;
/// Upper bound a caller's `limit` is clamped to, so one query cannot ask for the
/// whole store.
const MAX_LIMIT: i64 = 500;
const CALLER_RESOURCE_IDS_HEADER: &str = "x-ryu-caller-resource-ids";

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
            put(index_conversation)
                .get(get_conversation)
                .delete(delete_conversation),
        )
        .route("/conversations/:id/summarize", post(summarize))
        .with_state(ctx)
}

/// The OpenAPI sub-document Core fetches from `GET /openapi.json` and lowers into one
/// LLM tool per operation.
///
/// Deriving tools from this document is the ONLY path an agent has into this app, so
/// an unannotated route is not "undocumented" — it is uncallable. Core also INTERSECTS
/// the operations against `sidecars[0].http.routes[]`, so an operation documented here
/// but absent from the manifest yields nothing.
///
/// Two of the manifest's declared routes are absent on purpose. `/health` is the
/// un-gated loopback probe served from `main` (at both `/health` and
/// `/api/mission-control/health`), and `/` is declared but served by nothing at all —
/// both are the HARMLESS direction of the intersection, since a declared route with no
/// operation simply yields no tool. See `every_served_route_appears_in_the_openapi_doc`.
pub fn openapi() -> utoipa::openapi::OpenApi {
    <MissionControlApiDoc as utoipa::OpenApi>::openapi()
}

/// The document itself.
///
/// `components(schemas(...))` is what makes `request_body = IndexRequest` resolve to a
/// real `#/components/schemas/IndexRequest`. Without the entry the operation still
/// carries a `$ref` whose target is missing, and Core derives a write tool with ZERO
/// visible arguments — discoverable and uncallable.
///
/// The rows after `IndexRequest` are the TRANSITIVE graph reachable from it
/// (`Totals`, `TurnRecord` → `FileTouch`, `WorkItem`); each needed its own `ToSchema`
/// derive for the build to pass.
#[derive(utoipa::OpenApi)]
#[openapi(
    paths(
        conversations,
        delete_conversation,
        get_conversation,
        index_conversation,
        index_state,
        overview,
        summarize,
    ),
    components(schemas(IndexRequest, Totals, TurnRecord, FileTouch, WorkItem))
)]
struct MissionControlApiDoc;

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

/// Core's `x-ryu-caller-resource-ids` scope: the conversations the verified
/// caller may read right now, each as `<version>:<id>`. The version is Core's
/// digest of the conversation row's immutable identity, so a conversation that
/// was deleted and re-created under the same id (by anyone) arrives with a
/// different version and never matches the digest indexed for the old one.
struct CallerScope {
    /// id -> the source version Core stamped for it.
    versions: HashMap<String, String>,
    /// The entries re-encoded as a JSON array, for the SQL `json_each` filters.
    encoded: String,
}

/// Length of Core's source version (lowercase hex).
const SOURCE_VERSION_LEN: usize = 12;

impl CallerScope {
    fn from_headers(headers: &HeaderMap) -> ApiResult<Self> {
        let invalid = || {
            ApiError(
                StatusCode::FORBIDDEN,
                "Core provided an invalid authorized conversation scope".to_owned(),
            )
        };
        let raw = headers
            .get(CALLER_RESOURCE_IDS_HEADER)
            .and_then(|value| value.to_str().ok())
            .ok_or_else(|| {
                ApiError(
                    StatusCode::FORBIDDEN,
                    "Core did not provide an authorized conversation scope".to_owned(),
                )
            })?;
        let entries: Vec<String> = serde_json::from_str(raw).map_err(|_| invalid())?;
        let mut versions = HashMap::with_capacity(entries.len());
        for entry in &entries {
            let (version, id) = entry.split_once(':').ok_or_else(invalid)?;
            let well_formed = version.len() == SOURCE_VERSION_LEN
                && version
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                && !id.is_empty();
            if !well_formed {
                return Err(invalid());
            }
            versions.insert(id.to_owned(), version.to_owned());
        }
        Ok(Self {
            versions,
            encoded: serde_json::to_string(&entries).expect("serializing strings cannot fail"),
        })
    }

    /// The source version the caller may read `id` at, or 404.
    fn authorize(&self, id: &str) -> ApiResult<&str> {
        // Do not reveal whether another caller has indexed this id.
        self.versions
            .get(id)
            .map(String::as_str)
            .ok_or_else(not_found)
    }
}

type ApiResult<T> = Result<T, ApiError>;

// ── health ───────────────────────────────────────────────────────────────────

/// The un-gated loopback probe. Reports store reachability and nothing about the
/// user's work — Core hits this before auth, so it must leak nothing.
pub async fn health(store: Store) -> Response {
    match store.ping() {
        Ok(()) => Json(json!({
            "ok": true,
            "service": "mission-control",
            "sealing": store.seal_health(),
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
    fn to_filter(&self, now_ms: i64, authorized_ids_json: String) -> Filter {
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
            authorized_ids_json,
            limit,
            item_limit: default.item_limit,
        }
    }
}

// ── handlers ─────────────────────────────────────────────────────────────────

/// `PUT /conversations/:id` — replace one conversation's digest wholesale.
#[utoipa::path(
    put,
    path = "/api/mission-control/conversations/{id}",
    tag = "Mission Control",
    summary = "Index one conversation: store its turns, file touches, and open work items. This is a FULL REPLACEMENT of the stored digest, not a merge — anything omitted from the body is dropped. Normally driven by the desktop, not by hand.",
    params(("id" = String, Path, description = "Conversation id, as Core knows it")),
    request_body = IndexRequest,
    responses((status = 200, description = "Indexed", body = serde_json::Value))
)]
async fn index_conversation(
    State(ctx): State<Arc<Ctx>>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(req): Json<IndexRequest>,
) -> ApiResult<Response> {
    let scope = CallerScope::from_headers(&headers)?;
    let version = scope.authorize(&id)?;
    let digest = ctx.store.index(&id, version, &req, now_ms())?;
    Ok(Json(digest).into_response())
}

/// `GET /conversations/:id` — the full stored digest of one conversation.
#[utoipa::path(
    get,
    path = "/api/mission-control/conversations/{id}",
    tag = "Mission Control",
    summary = "Read the full stored record of one conversation: every indexed turn with the agent's own stated reasoning, the files it touched, and its outstanding work items. Read-only.",
    params(("id" = String, Path, description = "Conversation id, from GET /api/mission-control/conversations")),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn get_conversation(
    State(ctx): State<Arc<Ctx>>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Response> {
    let scope = CallerScope::from_headers(&headers)?;
    let version = scope.authorize(&id)?;
    let detail = ctx.store.detail(&id, version)?.ok_or_else(not_found)?;
    Ok(Json(detail).into_response())
}

/// `DELETE /conversations/:id` — forget one conversation's digest.
#[utoipa::path(
    delete,
    path = "/api/mission-control/conversations/{id}",
    tag = "Mission Control",
    summary = "PERMANENTLY delete this app's stored digest of one conversation. Cannot be undone here, though re-indexing rebuilds it. The conversation itself, in Core, is not touched.",
    params(("id" = String, Path, description = "Conversation id")),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn delete_conversation(
    State(ctx): State<Arc<Ctx>>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Response> {
    // Any row stored for an id the caller may now manage is removed, including
    // one indexed for an earlier (deleted) conversation under the same id.
    CallerScope::from_headers(&headers)?.authorize(&id)?;
    let removed = ctx.store.delete(&id)?;
    Ok(Json(json!({ "removed": removed })).into_response())
}

/// `GET /conversations` — the indexed conversations in a window.
#[utoipa::path(
    get,
    path = "/api/mission-control/conversations",
    tag = "Mission Control",
    summary = "List indexed conversations, newest first, with their per-chat totals and headline. Read-only; this is where to start to find a conversation id.",
    params(
        ("days" = Option<i64>, Query, description = "Only conversations from the last N days. Wins over `since_ms` when both are sent; a value of zero or less means no window at all."),
        ("since_ms" = Option<i64>, Query, description = "Only conversations updated at or after this epoch-milliseconds instant. Use `days` unless you need an exact boundary."),
        ("folder_path" = Option<String>, Query, description = "Only conversations belonging to this project folder."),
        ("limit" = Option<i64>, Query, description = "Maximum rows to return. Clamped to the app's own ceiling.")
    ),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn conversations(
    State(ctx): State<Arc<Ctx>>,
    headers: HeaderMap,
    Query(window): Query<WindowQuery>,
) -> ApiResult<Response> {
    let scope = CallerScope::from_headers(&headers)?;
    let rows = ctx
        .store
        .conversations(&window.to_filter(now_ms(), scope.encoded))?;
    Ok(Json(json!({ "conversations": rows })).into_response())
}

/// `GET /index-state` — what is indexed and how fresh, for incremental backfill.
#[utoipa::path(
    get,
    path = "/api/mission-control/index-state",
    tag = "Mission Control",
    summary = "List which conversations are already indexed and how fresh each digest is, so a backfill can skip the ones that have not changed. Read-only.",
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn index_state(State(ctx): State<Arc<Ctx>>, headers: HeaderMap) -> ApiResult<Response> {
    let scope = CallerScope::from_headers(&headers)?;
    Ok(Json(json!({ "conversations": ctx.store.index_state(&scope.encoded)? })).into_response())
}

/// `GET /overview` — the rolled-up dashboard for a window.
#[utoipa::path(
    get,
    path = "/api/mission-control/overview",
    tag = "Mission Control",
    summary = "Read the rolled-up picture of agent work over a window: totals, a per-day breakdown, the conversations involved, the outstanding work items, and the most-touched files. Read-only; the best single call for `what has been going on`.",
    params(
        ("days" = Option<i64>, Query, description = "Roll up the last N days. Wins over `since_ms` when both are sent; a value of zero or less means no window at all."),
        ("since_ms" = Option<i64>, Query, description = "Roll up everything at or after this epoch-milliseconds instant. Use `days` unless you need an exact boundary."),
        ("folder_path" = Option<String>, Query, description = "Restrict the roll-up to one project folder."),
        ("limit" = Option<i64>, Query, description = "Maximum conversations to fold in. Clamped to the app's own ceiling.")
    ),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn overview(
    State(ctx): State<Arc<Ctx>>,
    headers: HeaderMap,
    Query(window): Query<WindowQuery>,
) -> ApiResult<Response> {
    let scope = CallerScope::from_headers(&headers)?;
    let filter = window.to_filter(now_ms(), scope.encoded.clone());
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
        .filter_map(|row| {
            let version = scope.versions.get(&row.conversation_id)?;
            ctx.store
                .detail(&row.conversation_id, version)
                .ok()
                .flatten()
        })
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

/// `POST /conversations/:id/summarize` — ask the node's side model for a narrative.
// No `request_body`: the handler takes only `State` + `Path`. Declaring one would
// invent an argument a model then tries to fill.
#[utoipa::path(
    post,
    path = "/api/mission-control/conversations/{id}/summarize",
    tag = "Mission Control",
    summary = "Write a narrative summary of one indexed conversation and store it on the digest, REPLACING any summary already there. Calls a model, so it needs one configured and costs a completion.",
    params(("id" = String, Path, description = "Conversation id")),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
async fn summarize(
    State(ctx): State<Arc<Ctx>>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Response> {
    let scope = CallerScope::from_headers(&headers)?;
    let version = scope.authorize(&id)?;
    let detail = ctx.store.detail(&id, version)?.ok_or_else(not_found)?;
    let Some(host) = ctx.host.as_ref() else {
        return Err(ApiError(
            StatusCode::SERVICE_UNAVAILABLE,
            "this node cannot reach a model: the sidecar was not started by Core".to_owned(),
        ));
    };
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
    if !ctx.store.set_summary(&id, version, &normalized, stamped)? {
        return Err(not_found());
    }
    Ok(Json(json!({ "summary": normalized, "summarized_at": stamped })).into_response())
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

#[cfg(test)]
mod openapi_tests {

    #[test]
    fn multi_method_paths_keep_every_operation() {
        // utoipa keys `paths` by path STRING, so handlers annotated separately on the
        // same path must MERGE into one PathItem. If one overwrote another, the path key
        // would still exist and the write body would still resolve — the read tool would
        // silently never exist, which is exactly the failure this document prevents. The
        // route-coverage test above cannot see that, because it only checks the key.
        let wire = serde_json::to_value(super::openapi()).expect("the doc must serialize");
        for (path, methods) in [(
            "/api/mission-control/conversations/{id}",
            &["get", "put", "delete"][..],
        )] {
            let item = wire
                .pointer(&format!("/paths/{}", path.replace('/', "~1")))
                .unwrap_or_else(|| panic!("{path} has no PathItem"));
            for method in methods {
                assert!(
                    item.get(method).is_some(),
                    "{path} lost its {method} operation"
                );
            }
        }
    }
    /// This app's own manifest, read at compile time. The route contract lives there,
    /// so the invariants below compare the document against the real declaration rather
    /// than against a second list that could drift from it.
    fn manifest() -> serde_json::Value {
        serde_json::from_str(include_str!("../../manifest.json")).expect("valid JSON")
    }

    /// The manifest sidecar whose HTTP surface this router serves: the one declaring an
    /// `http.mount`. Selected BY mount rather than by index so a later mountless sidecar
    /// cannot silently redirect these assertions at the wrong process.
    fn mounted_sidecar() -> serde_json::Value {
        manifest()["sidecars"]
            .as_array()
            .expect("sidecars must be an array")
            .iter()
            .find(|s| s["http"]["mount"].is_string())
            .expect("one sidecar must declare an http.mount")
            .clone()
    }

    /// A manifest route (relative to the mount, `:param` form) rewritten into the form
    /// the OpenAPI document uses (absolute, `{param}` form). The two differ ON PURPOSE:
    /// the router registers relative paths because Core nests it, while the annotations
    /// carry the absolute EXTERNAL path. Normalise here; do not "align" either side.
    fn doc_path_for(mount: &str, route: &str) -> String {
        let joined = if route == "/" {
            mount.to_owned()
        } else {
            format!("{mount}{route}")
        };
        joined
            .split('/')
            .map(|seg| match seg.strip_prefix(':') {
                Some(name) => format!("{{{name}}}"),
                None => seg.to_owned(),
            })
            .collect::<Vec<_>>()
            .join("/")
    }

    #[test]
    fn openapi_doc_covers_the_served_routes() {
        assert!(!super::openapi().paths.paths.is_empty());
    }

    #[test]
    fn every_served_route_appears_in_the_openapi_doc() {
        // The direction that decides tool yield: Core keeps only the document operations
        // the manifest ALSO declares, so a declared route with no `#[utoipa::path]` is a
        // tool that silently never exists — nothing errors, the agent simply cannot call
        // it.
        //
        // Two DECLARED routes are skipped here because `routes()` does not serve them,
        // so there is nothing to annotate:
        //   `/health` — the un-gated loopback probe, registered in `main` at both
        //               `/health` and `/api/mission-control/health` so a probe arriving
        //               either way succeeds before auth. A health check is not a tool.
        //   `/`       — declared but served by NOTHING. Harmless (an undocumented
        //               declaration yields no tool, it does not break the proxy), and
        //               deliberately not "fixed" from this side: widening the router to
        //               match the manifest would invent a surface, and narrowing the
        //               manifest is an app-owner call, not a docs change.
        const DECLARED_BUT_NOT_SERVED_BY_THIS_ROUTER: [&str; 2] = ["/", "/health"];

        let sidecar = mounted_sidecar();
        let mount = sidecar["http"]["mount"].as_str().expect("an http.mount");
        let doc = super::openapi();
        for route in sidecar["http"]["routes"]
            .as_array()
            .expect("routes must be an array")
        {
            let path = route["path"].as_str().expect("a route path");
            if DECLARED_BUT_NOT_SERVED_BY_THIS_ROUTER.contains(&path) {
                continue;
            }
            let expected = doc_path_for(mount, path);
            assert!(
                doc.paths.paths.contains_key(&expected),
                "'{path}' is declared in manifest.json but the OpenAPI document has no \
                 '{expected}' operation — Core derives no tool for it"
            );
        }
    }

    #[test]
    fn the_index_body_is_typed_and_resolvable() {
        // An untyped body still yields an operation, so the tool is DISCOVERABLE with
        // zero visible arguments. Assert the `$ref` resolves the way Core's
        // `resolve_ref` will, and that the graph beneath it resolved too — a missing
        // `ToSchema` on `TurnRecord`/`FileTouch`/`WorkItem` would leave a body the model
        // cannot fill.
        let wire = serde_json::to_value(super::openapi()).expect("the doc must serialize");
        let schema = wire
            .pointer(
                "/paths/~1api~1mission-control~1conversations~1{id}/put/requestBody/content/application~1json/schema/$ref",
            )
            .and_then(serde_json::Value::as_str)
            .expect("PUT /conversations/{id} must declare a typed request body");
        assert_eq!(schema, "#/components/schemas/IndexRequest");
        for name in [
            "IndexRequest",
            "Totals",
            "TurnRecord",
            "FileTouch",
            "WorkItem",
        ] {
            assert!(
                wire.pointer(&format!("/components/schemas/{name}")).is_some(),
                "{name} is reachable from the IndexRequest body but missing from components.schemas"
            );
        }
    }

    #[test]
    fn the_window_queries_document_their_parameters() {
        // `overview` and `conversations` take `State` + `Query` only. Their arguments
        // come entirely from `params(...)`; without those the derived tools would be
        // callable but un-scopable, and every call would return the default window.
        let wire = serde_json::to_value(super::openapi()).expect("the doc must serialize");
        for path in [
            "/api/mission-control/overview",
            "/api/mission-control/conversations",
        ] {
            let params = wire
                .pointer(&format!(
                    "/paths/{}/get/parameters",
                    path.replace('/', "~1")
                ))
                .and_then(serde_json::Value::as_array)
                .unwrap_or_else(|| panic!("{path} must document its query parameters"));
            let names: Vec<&str> = params.iter().filter_map(|p| p["name"].as_str()).collect();
            for expected in ["days", "since_ms", "folder_path", "limit"] {
                assert!(
                    names.contains(&expected),
                    "{path} does not document its `{expected}` query parameter"
                );
            }
        }
    }

    #[test]
    fn summarize_declares_no_body() {
        // It takes only `State` + `Path`. A `request_body` here would invent an argument
        // the handler never reads.
        let wire = serde_json::to_value(super::openapi()).expect("the doc must serialize");
        let op = wire
            .pointer("/paths/~1api~1mission-control~1conversations~1{id}~1summarize/post")
            .expect("summarize must have a POST operation");
        assert!(
            op.get("requestBody").is_none(),
            "summarize takes no body but the document declares one"
        );
        assert!(
            op.get("parameters").is_some(),
            "summarize must still document its path id"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{to_bytes, Body};
    use axum::http::{Method, Request};
    use serde_json::{json, Value};
    use tower::ServiceExt;

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
        let filter = query(Some(7), Some(1), None).to_filter(10 * DAY_MS, "[]".to_owned());
        assert_eq!(filter.since_ms, Some(3 * DAY_MS));
    }

    #[test]
    fn a_non_positive_day_count_means_no_window_rather_than_an_empty_page() {
        assert_eq!(
            query(Some(0), None, None)
                .to_filter(DAY_MS, "[]".to_owned())
                .since_ms,
            None
        );
        assert_eq!(
            query(Some(-3), None, None)
                .to_filter(DAY_MS, "[]".to_owned())
                .since_ms,
            None
        );
    }

    #[test]
    fn an_explicit_instant_is_used_when_no_day_count_is_given() {
        let filter = query(None, Some(42), None).to_filter(DAY_MS, "[]".to_owned());
        assert_eq!(filter.since_ms, Some(42));
    }

    #[test]
    fn the_limit_is_clamped_and_a_junk_value_falls_back_to_the_default() {
        assert_eq!(
            query(None, None, Some(9_999))
                .to_filter(0, "[]".to_owned())
                .limit,
            MAX_LIMIT
        );
        assert_eq!(
            query(None, None, Some(0))
                .to_filter(0, "[]".to_owned())
                .limit,
            Filter::default().limit
        );
        assert_eq!(
            query(None, None, Some(25))
                .to_filter(0, "[]".to_owned())
                .limit,
            25
        );
    }

    #[test]
    fn a_blank_folder_filter_is_dropped_rather_than_matching_nothing() {
        let window = WindowQuery {
            days: None,
            since_ms: None,
            folder_path: Some("   ".into()),
            limit: None,
        };
        assert_eq!(window.to_filter(0, "[]".to_owned()).folder_path, None);
    }

    fn app() -> Router {
        routes(Arc::new(Ctx {
            store: Store::open_memory().expect("test store"),
            host: None,
        }))
    }

    async fn call(
        app: Router,
        method: Method,
        uri: &str,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        call_with_scope(app, method, uri, body, &["chat/one", "stable"]).await
    }

    /// The source version Core stamps for every test conversation.
    const V: &str = "0123456789ab";

    /// Core's scope header for `ids`, all at `version`.
    fn scope_header(ids: &[&str], version: &str) -> String {
        let entries: Vec<String> = ids.iter().map(|id| format!("{version}:{id}")).collect();
        serde_json::to_string(&entries).expect("scope JSON")
    }

    async fn call_with_scope(
        app: Router,
        method: Method,
        uri: &str,
        body: Option<Value>,
        scope: &[&str],
    ) -> (StatusCode, Value) {
        call_with_versioned_scope(app, method, uri, body, &scope_header(scope, V)).await
    }

    async fn call_with_versioned_scope(
        app: Router,
        method: Method,
        uri: &str,
        body: Option<Value>,
        scope_header: &str,
    ) -> (StatusCode, Value) {
        let mut request = Request::builder().method(method).uri(uri);
        request = request.header(CALLER_RESOURCE_IDS_HEADER, scope_header);
        if body.is_some() {
            request = request.header("content-type", "application/json");
        }
        let response = app
            .oneshot(
                request
                    .body(body.map_or_else(Body::empty, |value| Body::from(value.to_string())))
                    .expect("request"),
            )
            .await
            .expect("router response");
        let status = response.status();
        let bytes = to_bytes(response.into_body(), 1024 * 1024)
            .await
            .expect("bounded response body");
        let value = serde_json::from_slice(&bytes).expect("JSON response");
        (status, value)
    }

    fn digest(headline: &str, source_updated_at: i64, open_item: &str) -> Value {
        json!({
            "title": "Coverage sweep",
            "folder_path": "/workspace/ryu",
            "source_updated_at": source_updated_at,
            "totals": { "turns": 1, "writes": 1, "commands": 2, "tool_calls": 3, "files_touched": 1 },
            "turns": [{
                "index": 0,
                "headline": headline,
                "request": "verify the caller",
                "rationale": "exercise the production router",
                "outcome": "done",
                "status": "ok"
            }],
            "files": [{ "path": "/workspace/ryu/file.rs", "kind": "edit", "count": 1 }],
            "open_items": [{ "content": open_item, "status": "pending" }],
            "done_items": []
        })
    }

    #[tokio::test]
    async fn production_router_replaces_reads_rolls_up_and_deletes_a_digest() {
        let router = app();
        let (status, first) = call(
            router.clone(),
            Method::PUT,
            "/conversations/chat%2Fone",
            Some(digest("First pass", 100, "retry provider")),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(first["conversation_id"], "chat/one");
        assert_eq!(first["open_count"], 1);

        let (status, _) = call(
            router.clone(),
            Method::PUT,
            "/conversations/chat%2Fone",
            Some(digest("Retried exactly once", 200, "inspect evidence")),
        )
        .await;
        assert_eq!(status, StatusCode::OK);

        let (status, detail) = call(
            router.clone(),
            Method::GET,
            "/conversations/chat%2Fone",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(detail["headline"], "Retried exactly once");
        assert_eq!(detail["source_updated_at"], 200);
        assert_eq!(detail["open_items"][0]["content"], "inspect evidence");

        let (status, overview) = call(
            router.clone(),
            Method::GET,
            "/overview?folder_path=%2Fworkspace%2Fryu&since_ms=150",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(overview["totals"]["conversations"], 1);
        assert_eq!(overview["totals"]["open_items"], 1);
        assert_eq!(overview["files"][0]["path"], "/workspace/ryu/file.rs");

        let (status, removed) = call(
            router.clone(),
            Method::DELETE,
            "/conversations/chat%2Fone",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(removed["removed"], true);

        let (status, missing) = call(router, Method::GET, "/conversations/chat%2Fone", None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(
            missing["error"],
            "no digest has been indexed for this conversation"
        );
    }

    #[tokio::test]
    async fn production_router_refuses_invalid_input_and_unavailable_model_without_mutation() {
        let router = app();
        let (status, _) = call(
            router.clone(),
            Method::PUT,
            "/conversations/stable",
            Some(digest("Stable", 300, "keep me")),
        )
        .await;
        assert_eq!(status, StatusCode::OK);

        let invalid_response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::PUT)
                    .uri("/conversations/stable")
                    .header("content-type", "application/json")
                    .header(CALLER_RESOURCE_IDS_HEADER, scope_header(&["stable"], V))
                    .body(Body::from(r#"{"turns":"not-an-array"}"#))
                    .expect("request"),
            )
            .await
            .expect("router response");
        assert_eq!(invalid_response.status(), StatusCode::UNPROCESSABLE_ENTITY);

        let (status, unavailable) = call(
            router.clone(),
            Method::POST,
            "/conversations/stable/summarize",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(unavailable["error"]
            .as_str()
            .expect("error string")
            .contains("sidecar was not started by Core"));

        let (status, detail) = call(router, Method::GET, "/conversations/stable", None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(detail["headline"], "Stable");
        assert_eq!(detail["open_items"][0]["content"], "keep me");
        assert!(detail["summary"].is_null());
    }

    #[tokio::test]
    async fn every_route_uses_cores_current_conversation_scope() {
        let router = app();
        let (status, _) = call_with_scope(
            router.clone(),
            Method::PUT,
            "/conversations/mine",
            Some(digest("Mine", 100, "visible")),
            &["mine"],
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let (status, _) = call_with_scope(
            router.clone(),
            Method::PUT,
            "/conversations/theirs",
            Some(digest("Theirs", 200, "secret")),
            &["theirs"],
        )
        .await;
        assert_eq!(status, StatusCode::OK);

        let (status, list) = call_with_scope(
            router.clone(),
            Method::GET,
            "/conversations",
            None,
            &["mine"],
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(list["conversations"].as_array().unwrap().len(), 1);
        assert_eq!(list["conversations"][0]["conversation_id"], "mine");

        let (status, overview) =
            call_with_scope(router.clone(), Method::GET, "/overview", None, &["mine"]).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(overview["totals"]["conversations"], 1);
        assert_eq!(overview["open_items"][0]["content"], "visible");

        let (status, state) =
            call_with_scope(router.clone(), Method::GET, "/index-state", None, &["mine"]).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(state["conversations"].as_array().unwrap().len(), 1);
        assert_eq!(state["conversations"][0]["conversation_id"], "mine");

        for (method, uri) in [
            (Method::GET, "/conversations/theirs"),
            (Method::DELETE, "/conversations/theirs"),
            (Method::POST, "/conversations/theirs/summarize"),
        ] {
            let (status, _) = call_with_scope(router.clone(), method, uri, None, &["mine"]).await;
            assert_eq!(status, StatusCode::NOT_FOUND);
        }
        // An out-of-scope caller cannot overwrite the digest either.
        let (status, _) = call_with_scope(
            router.clone(),
            Method::PUT,
            "/conversations/theirs",
            Some(digest("Poisoned", 300, "poison")),
            &["mine"],
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        // The refused DELETE above removed nothing.
        let (status, theirs) = call_with_scope(
            router.clone(),
            Method::GET,
            "/conversations/theirs",
            None,
            &["theirs"],
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(theirs["headline"], "Theirs");

        let response = router
            .oneshot(
                Request::builder()
                    .uri("/conversations")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    /// Core stamps the caller's whole readable conversation set into one header (up
    /// to 256 KiB, several thousand ids). The sidecar's HTTP server must accept a
    /// header that large over a real socket, or a heavy user's dashboard would fail
    /// even though Core admitted the request.
    #[tokio::test]
    async fn a_core_sized_scope_header_is_accepted_over_http() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let port = listener.local_addr().expect("addr").port();
        let server = tokio::spawn(async move {
            let app = Router::new().nest("/api/mission-control", app());
            axum::serve(listener, app).await.expect("serve");
        });

        // `<12 hex>:<36-char id>` entries cost 52 bytes each in the array
        // ("…",), plus the brackets.
        let count = (256 * 1024 - 2) / 52;
        let ids: Vec<String> = (0..count)
            .map(|index| format!("{V}:{index:08}-0000-4000-8000-000000000000"))
            .collect();
        let encoded = serde_json::to_string(&ids).expect("ids JSON");
        assert!(encoded.len() > 250 * 1024 && encoded.len() <= 256 * 1024);

        let response = reqwest::Client::new()
            .get(format!(
                "http://127.0.0.1:{port}/api/mission-control/index-state"
            ))
            .header(CALLER_RESOURCE_IDS_HEADER, encoded)
            .send()
            .await
            .expect("request");
        assert_eq!(response.status().as_u16(), 200);
        server.abort();
    }

    /// The verifier's re-claim path: Alice indexes conversation `x`; Core deletes
    /// it; Bob creates a new conversation under the same id. Core now puts `x` in
    /// Bob's scope, but with the NEW row's source version, so no route serves him
    /// Alice's digest, and his own re-index starts clean (no inherited summary).
    #[tokio::test]
    async fn a_conversation_re_created_under_a_deleted_id_cannot_read_the_old_digest() {
        const RECREATED: &str = "ba9876543210";
        let store = Store::open_memory().expect("test store");
        let router = routes(Arc::new(Ctx {
            store: store.clone(),
            host: None,
        }));
        let alice_scope = scope_header(&["x"], V);
        let (status, _) = call_with_versioned_scope(
            router.clone(),
            Method::PUT,
            "/conversations/x",
            Some(digest("Alice's private work", 100, "alice secret")),
            &alice_scope,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(store.set_summary("x", V, "alice narrative", 101).unwrap());

        let bob_scope = scope_header(&["x"], RECREATED);
        for (method, uri) in [
            (Method::GET, "/conversations/x"),
            (Method::POST, "/conversations/x/summarize"),
        ] {
            let (status, _) =
                call_with_versioned_scope(router.clone(), method, uri, None, &bob_scope).await;
            assert_eq!(status, StatusCode::NOT_FOUND);
        }
        for uri in ["/conversations", "/index-state"] {
            let (status, body) =
                call_with_versioned_scope(router.clone(), Method::GET, uri, None, &bob_scope).await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(body["conversations"].as_array().unwrap().len(), 0, "{uri}");
        }
        let (status, overview) =
            call_with_versioned_scope(router.clone(), Method::GET, "/overview", None, &bob_scope)
                .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(overview["totals"]["conversations"], 0);
        assert!(!overview.to_string().contains("alice secret"));

        // Bob indexes his own conversation: it replaces the old row wholesale.
        let (status, _) = call_with_versioned_scope(
            router.clone(),
            Method::PUT,
            "/conversations/x",
            Some(digest("Bob's work", 200, "bob item")),
            &bob_scope,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let (status, detail) = call_with_versioned_scope(
            router.clone(),
            Method::GET,
            "/conversations/x",
            None,
            &bob_scope,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(detail["headline"], "Bob's work");
        assert!(
            detail["summary"].is_null(),
            "the old summary must not carry over"
        );
        assert!(!detail.to_string().contains("alice"));
    }

    /// A scope entry without Core's version (an older Core, or a forged shape) is
    /// refused rather than read as a bare id.
    #[tokio::test]
    async fn an_unversioned_scope_is_refused() {
        for raw in [
            r#"["x"]"#,
            r#"[":x"]"#,
            r#"["0123456789AB:x"]"#,
            r#"["0123456789ab:"]"#,
        ] {
            let (status, _) =
                call_with_versioned_scope(app(), Method::GET, "/conversations", None, raw).await;
            assert_eq!(status, StatusCode::FORBIDDEN, "{raw}");
        }
    }
}
