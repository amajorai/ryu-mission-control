//! `ryu-mission-control` — the standalone, out-of-process Mission Control sidecar.
//!
//! Runs the `ryu_mission_control` crate (the SQLite [`Store`] plus the
//! `/api/mission-control/*` surface) as a SEPARATE PROCESS that Core spawns,
//! health-checks and proxies to on loopback — exactly like `ryu-ugc` /
//! `ryu-reasoning`. The store and handlers live in the crate lib; this binary is
//! only the process shell around them, so the same crate still compiles into Core
//! in-process as a path dependency and no code is duplicated.
//!
//! [`ryu_mission_control::routes`] returns a state-baked `Router` whose paths are
//! RELATIVE to `/api/mission-control` (the manifest's `http.mount` /
//! `public_mount`). This binary nests it under that same prefix, so the generic
//! ext-proxy forwards `/api/mission-control/*` unchanged and the desktop page
//! reaches the sidecar with no per-app Core coupling at all.
//!
//! SECURITY: loopback-only bind (127.0.0.1) plus a shared-secret bearer gate
//! (`RYU_EXT_TOKEN`, injected by Core at spawn and presented on the health probe
//! and every proxied hop). EVERY `/api/mission-control/*` route is protected —
//! this store holds a digest of the user's work, including file paths and the
//! agent's reasoning, so there is no route that may answer an unauthenticated
//! caller. The gate is FAIL-CLOSED: with no token configured every protected
//! route rejects with 401.
//!
//! Health is the ONE un-gated surface (loopback probe, reports only reachability
//! and a count), so Core's pre-auth readiness check succeeds. It is registered at
//! BOTH `/health` — the manifest's `health_path`, which Core probes directly —
//! and `/api/mission-control/health`, the DECLARED proxy route, because
//! `proxy_for_plugin` forwards `<mount><sub_path>` verbatim and a proxied probe
//! therefore arrives with the prefix still on it. `routes()` deliberately
//! contains neither: health must answer before the gate, so it cannot live
//! inside the nest.
//!
//! Port: `RYU_MISSION_CONTROL_PORT` env, default `8010`. Data dir: resolved via
//! the inlined [`paths::ryu_dir`] (`RYU_DIR`-env-first, injected by Core at
//! spawn), so it opens the same `mission-control.db` the node uses.

mod paths;

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;

use axum::{
    extract::Request,
    http::{header::AUTHORIZATION, StatusCode},
    middleware::{from_fn, Next},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};

use ryu_mission_control::{api, routes, Ctx, Host, Store, DB_FILE_NAME};

/// Default loopback port for the Mission Control sidecar (overridable via
/// `RYU_MISSION_CONTROL_PORT`). NOT the next free number after 8006 reasoning —
/// 8007 is already claimed by `@ryu/tuition`, and 8008/8009/8011 were taken by
/// apps built concurrently with this one. 8010 is the gap left between them.
/// Kept identical in `manifest.json`; the two drifting apart means Core
/// health-checks and proxies to a port the process never bound.
const DEFAULT_PORT: u16 = 8010;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let port: u16 = std::env::var("RYU_MISSION_CONTROL_PORT")
        .ok()
        .and_then(|p| p.trim().parse().ok())
        .unwrap_or(DEFAULT_PORT);

    // Shared-secret bearer Core injects via the generic ext-proxy loader
    // (`RYU_EXT_TOKEN`) — the per-plugin minted secret it stamps on every proxied
    // hop and the health probe.
    let token = std::env::var("RYU_EXT_TOKEN")
        .ok()
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty());
    if token.is_some() {
        tracing::info!(
            "ryu-mission-control: protected /api/mission-control/* routes require the injected shared-secret bearer"
        );
    } else {
        tracing::warn!(
            "ryu-mission-control: no RYU_EXT_TOKEN set; protected /api/mission-control/* routes are FAIL-CLOSED (reject all). Core injects this token when it spawns the sidecar."
        );
    }

    let dir = paths::ryu_dir();
    let store = Store::open(dir.join(DB_FILE_NAME))?;

    // `None` when this process was not spawned by Core. Every indexed route still
    // works; only the narrative summary reports itself unavailable.
    let host = Host::from_env();
    if host.is_none() {
        tracing::warn!(
            "ryu-mission-control: no host callback env; POST /conversations/:id/summarize will report 503 (the dashboard's indexed data is unaffected)"
        );
    }

    let ctx = Arc::new(Ctx {
        store: store.clone(),
        host,
    });

    let gated_token = token.clone();
    // `/openapi.json` rides INSIDE the bearer gate but OUTSIDE the nest, at the server
    // ROOT: Core fetches `http://127.0.0.1:<port>/openapi.json` on this sidecar's first
    // Healthy edge and derives one LLM tool per operation, and root is the only address
    // it tries. Gated rather than sitting beside `/health` because the document
    // enumerates every route and body field this app accepts.
    let mission = Router::new()
        .nest("/api/mission-control", routes(ctx))
        .route("/openapi.json", get(|| async { Json(api::openapi()) }))
        .layer(from_fn(move |req: Request, next: Next| {
            let expected = gated_token.clone();
            async move { require_token(req, next, expected.as_deref()).await }
        }));

    // Health sits OUTSIDE the gated nest so the loopback probe succeeds before
    // auth, at both the paths a probe can arrive on (see the module docs). No axum
    // conflict: `routes()` registers neither.
    let probe_store = store.clone();
    let proxied_probe_store = store;
    let app = Router::new()
        .route(
            "/health",
            get(move || {
                let store = probe_store.clone();
                async move { api::health(store).await }
            }),
        )
        .route(
            "/api/mission-control/health",
            get(move || {
                let store = proxied_probe_store.clone();
                async move { api::health(store).await }
            }),
        )
        .merge(mission);

    // LOOPBACK ONLY (belt) plus shared-secret bearer (suspenders): Core is the
    // auth front and re-stamps the bearer on the proxied hop.
    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("ryu-mission-control sidecar listening on http://{addr}");

    axum::serve(listener, app).await?;
    Ok(())
}

/// Shared-secret bearer gate for the proxied surface. Core stays the auth front —
/// it runs `require_auth`, then re-stamps `Authorization: Bearer <RYU_EXT_TOKEN>`
/// on the loopback hop — so a request that did NOT come through Core (any other
/// local process on a shared host) is rejected with 401.
///
/// **Fail-closed:** `expected == None`/empty (no token configured) rejects every
/// request rather than falling open.
async fn require_token(req: Request, next: Next, expected: Option<&str>) -> Response {
    let provided = req
        .headers()
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    if bearer_ok(provided, expected) {
        next.run(req).await
    } else {
        (StatusCode::UNAUTHORIZED, "unauthorized").into_response()
    }
}

/// Pure bearer check (factored out so the auth decision is unit-testable without
/// an axum `Request`/`Next`). Returns `true` only when `expected` is a non-empty
/// token AND `provided` equals it, constant-time compared. A `None`/empty
/// `expected` is the fail-closed case, so always `false`.
fn bearer_ok(provided: Option<&str>, expected: Option<&str>) -> bool {
    ryu_sidecar_runtime::token_ok(provided, expected)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_configured_token_rejects_everything() {
        assert!(!bearer_ok(Some("anything"), None));
        assert!(!bearer_ok(Some("anything"), Some("")));
        assert!(!bearer_ok(None, None));
    }

    #[test]
    fn only_the_exact_token_is_accepted() {
        assert!(bearer_ok(Some("s3cret"), Some("s3cret")));
        assert!(!bearer_ok(Some("s3cre"), Some("s3cret")));
        assert!(!bearer_ok(Some("s3crett"), Some("s3cret")));
        assert!(!bearer_ok(None, Some("s3cret")));
    }

    #[test]
    fn ct_eq_is_length_sensitive_and_content_sensitive() {
        assert!(ryu_sidecar_runtime::constant_time_eq(b"abc", b"abc"));
        assert!(!ryu_sidecar_runtime::constant_time_eq(b"abc", b"abd"));
        assert!(!ryu_sidecar_runtime::constant_time_eq(b"abc", b"ab"));
    }
}
