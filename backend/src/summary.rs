//! Turning an indexed conversation into a prompt, and the prompt's answer into a
//! stored narrative.
//!
//! The prompt is built from the digest, never from the transcript: this process
//! has no way to read a conversation (see `model.rs`), and it should not want
//! one. Feeding a model the turn headlines, the agent's own recorded rationale
//! and the outstanding items is both cheaper and more faithful than re-reading
//! the chat, because those fields were derived from the tool calls that actually
//! ran rather than from what the agent said in prose.
//!
//! The prompt is BOUNDED. A hundred-turn chat is truncated to the most recent
//! turns and each field is clipped, so the token cost of summarising a
//! conversation does not grow with the conversation. A digest that gets cut says
//! so in the prompt, so the model narrates "the recent work" rather than
//! confidently describing a week it was never shown.

use crate::model::ConversationDetail;

/// Turns beyond this are dropped from the prompt, newest kept.
const MAX_PROMPT_TURNS: usize = 24;
/// Per-field clip inside the prompt.
const MAX_FIELD_CHARS: usize = 320;
/// Outstanding items beyond this are counted rather than listed.
const MAX_PROMPT_ITEMS: usize = 20;
/// Guard against a model that answers with an essay.
pub const MAX_SUMMARY_CHARS: usize = 1200;

pub const SYSTEM: &str = "You summarise one AI coding session for a project dashboard. \
You are given a structured digest of what the session did: per-turn headlines derived from the \
tool calls that actually ran, the agent's own stated reasoning, and the to-do list it left behind. \
Write 2-4 plain sentences: what this session accomplished, and what is still outstanding. \
Be concrete — name files and outcomes from the digest. Do not invent work that is not in the \
digest, do not repeat the to-do list verbatim, and do not use headings, bullets or preamble. \
If the digest shows failures, say so.";

fn clip(text: &str, max: usize) -> String {
    let trimmed = text.trim();
    if trimmed.chars().count() <= max {
        return trimmed.to_owned();
    }
    let cut: String = trimmed.chars().take(max).collect();
    format!("{}…", cut.trim_end())
}

/// The user-side prompt for one conversation's digest.
pub fn build_prompt(detail: &ConversationDetail) -> String {
    let mut out = String::new();
    if let Some(title) = detail.digest.title.as_deref() {
        out.push_str(&format!("Session: {}\n", clip(title, MAX_FIELD_CHARS)));
    }
    if let Some(folder) = detail.digest.folder_path.as_deref() {
        out.push_str(&format!("Project: {folder}\n"));
    }
    let t = &detail.digest.totals;
    out.push_str(&format!(
        "Totals: {} turns, {} files written, {} commands run, {} tool errors.\n",
        t.turns, t.writes, t.commands, t.failures
    ));

    let skipped = detail.turns.len().saturating_sub(MAX_PROMPT_TURNS);
    if skipped > 0 {
        out.push_str(&format!(
            "\n(The {skipped} earliest turns are omitted; summarise the recent work below.)\n"
        ));
    }
    out.push_str("\nTurns:\n");
    for turn in detail.turns.iter().skip(skipped) {
        out.push_str(&format!("- [{}] {}", turn.status, clip(&turn.headline, MAX_FIELD_CHARS)));
        if !turn.request.is_empty() {
            out.push_str(&format!("\n  asked: {}", clip(&turn.request, MAX_FIELD_CHARS)));
        }
        if !turn.rationale.is_empty() {
            out.push_str(&format!(
                "\n  reasoning: {}",
                clip(&turn.rationale, MAX_FIELD_CHARS)
            ));
        }
        if !turn.outcome.is_empty() {
            out.push_str(&format!("\n  result: {}", clip(&turn.outcome, MAX_FIELD_CHARS)));
        }
        out.push('\n');
    }

    if !detail.files.is_empty() {
        out.push_str("\nFiles touched:\n");
        for file in detail.files.iter().take(MAX_PROMPT_ITEMS) {
            out.push_str(&format!("- {} ({})\n", file.path, file.kind));
        }
    }

    if detail.open_items.is_empty() {
        out.push_str("\nNothing was left outstanding.\n");
    } else {
        out.push_str("\nStill outstanding:\n");
        for item in detail.open_items.iter().take(MAX_PROMPT_ITEMS) {
            out.push_str(&format!("- {}\n", clip(&item.content, MAX_FIELD_CHARS)));
        }
        let extra = detail.open_items.len().saturating_sub(MAX_PROMPT_ITEMS);
        if extra > 0 {
            out.push_str(&format!("- (and {extra} more)\n"));
        }
    }
    out
}

/// Normalise a completion into the stored narrative: strip a leading label the
/// model may have added despite the system prompt, collapse it to one paragraph,
/// and clip it so one bad answer cannot bloat every dashboard read.
pub fn normalize(raw: &str) -> String {
    let mut text = raw.trim();
    for label in ["Summary:", "SUMMARY:", "summary:"] {
        if let Some(rest) = text.strip_prefix(label) {
            text = rest.trim_start();
        }
    }
    let collapsed = text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join(" ");
    clip(&collapsed, MAX_SUMMARY_CHARS)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ConversationDigest, FileTouch, Totals, TurnRecord, WorkItem};

    fn detail(turns: usize, open: usize) -> ConversationDetail {
        ConversationDetail {
            digest: ConversationDigest {
                conversation_id: "c1".into(),
                title: Some("Ship auth".into()),
                folder_path: Some("/repo".into()),
                agent_id: None,
                source_updated_at: 0,
                indexed_at: 0,
                totals: Totals {
                    turns: turns as i64,
                    writes: 2,
                    commands: 1,
                    failures: 1,
                    tool_calls: 9,
                    files_touched: 2,
                },
                headline: None,
                summary: None,
                summarized_at: None,
                open_count: open as i64,
                done_count: 0,
            },
            turns: (0..turns)
                .map(|i| TurnRecord {
                    index: i as i64 + 1,
                    headline: format!("Changed file{i}.ts"),
                    request: format!("do thing {i}"),
                    rationale: format!("because reason {i}"),
                    outcome: format!("done {i}"),
                    status: "ok".into(),
                    files: vec![],
                })
                .collect(),
            files: vec![FileTouch {
                path: "/repo/auth.ts".into(),
                kind: "edit".into(),
                count: 1,
            }],
            open_items: (0..open)
                .map(|i| WorkItem {
                    content: format!("todo {i}"),
                    status: "pending".into(),
                })
                .collect(),
            done_items: vec![],
        }
    }

    #[test]
    fn the_prompt_carries_totals_reasoning_and_outstanding_work() {
        let prompt = build_prompt(&detail(2, 1));
        assert!(prompt.contains("Session: Ship auth"));
        assert!(prompt.contains("2 turns, 2 files written, 1 commands run, 1 tool errors."));
        assert!(prompt.contains("reasoning: because reason 0"));
        assert!(prompt.contains("Still outstanding:"));
        assert!(prompt.contains("- todo 0"));
    }

    #[test]
    fn a_long_conversation_is_truncated_and_says_so() {
        let prompt = build_prompt(&detail(40, 0));
        assert!(prompt.contains("The 16 earliest turns are omitted"));
        // The newest turn survives; the oldest does not.
        assert!(prompt.contains("Changed file39.ts"));
        assert!(!prompt.contains("Changed file0.ts"));
    }

    #[test]
    fn an_empty_plan_is_stated_rather_than_left_out() {
        let prompt = build_prompt(&detail(1, 0));
        assert!(prompt.contains("Nothing was left outstanding."));
    }

    #[test]
    fn extra_outstanding_items_are_counted_not_listed() {
        let prompt = build_prompt(&detail(1, 25));
        assert!(prompt.contains("- todo 19"));
        assert!(!prompt.contains("- todo 20"));
        assert!(prompt.contains("(and 5 more)"));
    }

    #[test]
    fn normalize_strips_a_label_and_collapses_to_one_paragraph() {
        let out = normalize("Summary:\n\nShipped auth.\n\nTests pass.\n");
        assert_eq!(out, "Shipped auth. Tests pass.");
    }

    #[test]
    fn normalize_clips_an_essay() {
        let out = normalize(&"x".repeat(MAX_SUMMARY_CHARS * 2));
        assert_eq!(out.chars().count(), MAX_SUMMARY_CHARS + 1);
        assert!(out.ends_with('…'));
    }
}
