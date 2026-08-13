//! The sidecar's one line back into Core.
//!
//! Narrating a week of work is a language task, so the summary route needs a
//! model. The sidecar holds no provider key and must not egress on its own;
//! instead it calls Core's generic sidecar callback:
//!
//! ```text
//! POST http://127.0.0.1:$RYU_CORE_PORT/api/host/model/complete
//!   authorization: Bearer $RYU_EXT_TOKEN
//!   x-ryu-plugin-id: $RYU_EXT_PLUGIN_ID
//!   { "prompt": …, "system": …, "model_pref_key": … }
//! ```
//!
//! Core authenticates the minted per-plugin token, intersects the manifest's
//! declared `host_api.grants` with the Gateway-*approved* grants, and only then
//! runs the completion through the same `host.sideModel` capability the turn-hook
//! sandbox uses. So the app inherits the node's provider routing, budget and
//! egress policy, and holds no credential of its own.
//!
//! Both halves of the grant matter: `hook:side-model` must appear in the
//! sidecar's `host_api.grants` **and** be approved for the plugin. Missing either
//! side is a 403, which surfaces here as a plain error rather than a silent empty
//! answer.
//!
//! Everything else in this app works with no host at all. The dashboard's
//! numbers, its per-day activity and its outstanding-work list are all indexed
//! facts; only the optional narrative needs this file. That ordering is
//! deliberate — a node with no model configured still gets a working Mission
//! Control.

use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use serde_json::{json, Value};

/// Env keys Core injects into every manifest sidecar at spawn.
const ENV_TOKEN: &str = "RYU_EXT_TOKEN";
const ENV_PLUGIN_ID: &str = "RYU_EXT_PLUGIN_ID";
const ENV_CORE_PORT: &str = "RYU_CORE_PORT";

/// A summary is a few hundred tokens over a bounded prompt; give it room but
/// never forever.
const TIMEOUT: Duration = Duration::from_secs(90);

#[derive(Clone)]
pub struct Host {
    base: String,
    plugin_id: String,
    token: String,
    http: reqwest::Client,
}

impl Host {
    /// Build from the injected environment. `None` when the process was not
    /// spawned by Core — every indexed route still works, and the summary route
    /// reports that it is unavailable rather than pretending.
    pub fn from_env() -> Option<Host> {
        let token = std::env::var(ENV_TOKEN).ok().filter(|s| !s.is_empty())?;
        let plugin_id = std::env::var(ENV_PLUGIN_ID).ok().filter(|s| !s.is_empty())?;
        let port = std::env::var(ENV_CORE_PORT)
            .ok()
            .and_then(|p| p.parse::<u16>().ok())?;
        Some(Host {
            base: format!("http://127.0.0.1:{port}"),
            plugin_id,
            token,
            http: reqwest::Client::builder().timeout(TIMEOUT).build().ok()?,
        })
    }

    /// One completion. `model_pref_key` names a settings key the user can point
    /// at a specific model, so a digest can run on something cheap without
    /// changing the chat's own model.
    pub async fn complete(
        &self,
        system: &str,
        prompt: &str,
        model_pref_key: Option<&str>,
    ) -> Result<String> {
        let mut args = json!({ "system": system, "prompt": prompt });
        if let Some(key) = model_pref_key {
            args["model_pref_key"] = Value::String(key.to_owned());
        }
        let resp = self
            .http
            .post(format!("{}/api/host/model/complete", self.base))
            .bearer_auth(&self.token)
            .header("x-ryu-plugin-id", &self.plugin_id)
            .json(&args)
            .send()
            .await
            .context("calling the host model callback")?;

        let status = resp.status();
        let body: Value = resp
            .json()
            .await
            .context("host model callback returned a non-JSON body")?;
        if !status.is_success() {
            let msg = body
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("host model callback failed");
            // 403 here almost always means the grant is declared but not approved.
            return Err(anyhow!("{msg} (HTTP {status})"));
        }
        let text = body.get("result").map(render_result).unwrap_or_default();
        if text.trim().is_empty() {
            return Err(anyhow!("the model returned an empty completion"));
        }
        Ok(text)
    }
}

/// The bridge returns either a bare string or a `{ text }`-shaped object
/// depending on the provider; accept both rather than depending on one provider.
fn render_result(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Object(map) => map
            .get("text")
            .or_else(|| map.get("content"))
            .or_else(|| map.get("output"))
            .map(render_result)
            .unwrap_or_default(),
        Value::Array(items) => items.iter().map(render_result).collect::<Vec<_>>().join(""),
        _ => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn result_shapes_all_render_to_text() {
        assert_eq!(render_result(&json!("hi")), "hi");
        assert_eq!(render_result(&json!({ "text": "hi" })), "hi");
        assert_eq!(render_result(&json!({ "content": "hi" })), "hi");
        assert_eq!(
            render_result(&json!([{ "text": "a" }, { "text": "b" }])),
            "ab"
        );
        assert_eq!(render_result(&json!(7)), "");
    }
}
