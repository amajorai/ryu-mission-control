# ryu-mission-control

Mission Control for Ryu — the project-level view over many chats: recent sessions and what each one accomplished, per-day activity, the files several chats keep returning to, and the to-dos left outstanding in threads nobody reopened. Digests are computed by the client, so every surface agrees.

> **The public home of `ryu-mission-control`.** Source, builds, and releases live here —
> binaries for every platform are attached to each release.
>
> This tree is generated from the Ryu monorepo, so commits pushed here
> directly are replaced on the next sync. **Pull requests are welcome** —
> open them here and they are ported into the monorepo, then flow back out.
> Ryu as a whole: https://github.com/amajorai/ryu

## Install

- Binary: `ryu-mission-control` from the [Ryu releases](https://github.com/amajorai/ryu/releases).
- Crate: `cargo install ryu-mission-control`.

## License

Apache-2.0 — see [LICENSE](./LICENSE).

---

# Mission Control

The project-level view over many chats.

A chat's transcript answers *what was said*. The **Mission Control panel** in the
chat's right dock answers *what this chat did* — one card per turn, with the
files it changed, the commands it ran, and the agent's own rationale under each.
This app answers the question neither of those can:

> **What has been happening across every chat, this week?**

Recent sessions and what each accomplished, per-day activity, the files several
chats keep returning to, and — the part that actually changes what you do next —
the to-dos left outstanding in threads nobody has reopened.

## The two halves

| half | where it lives | needs this app? |
| --- | --- | --- |
| In-chat turn digest | the desktop shell's `mission` dock panel | **no** |
| Cross-chat dashboard | this app's sidecar + the `/mission-control` page | yes |

The in-chat panel is shell infrastructure, not an app surface, and it works
whether or not Mission Control is installed. That is not a style choice: every
`contributes.dock_panels` kind is pinnable, so the shell hands it to the project
dock store and mounts it *outside* the chat, where the per-chat props a
conversation-local panel needs do not exist. See the note on
`isPinnableDockTabKind` in `apps/desktop/src/components/panels/dock-panels.ts`.

## Why the digest is computed in the client

A manifest sidecar's callbacks into Core are exactly three —
`POST /api/host/model/complete`, `/api/host/rpc`, `/api/host/capability/:cap`
(`apps/core/src/sidecar/ext_proxy.rs`). None of them reads a conversation, and
the `parts` column those digests come from is sealed at rest. So this process
cannot see a chat, by design.

The one place that *already* holds every message with its tool calls is the
desktop. It derives the digest with
`apps/desktop/src/lib/mission-control/turn-groups.ts` and `PUT`s it here. That is
the same function the in-chat panel renders from, so the panel and the project
page cannot disagree about a chat.

Re-indexing is keyed on the conversation's `updated_at`. The client reads
`GET /index-state`, compares, and only fetches the chats that actually moved — so
refreshing a dashboard over a hundred conversations costs a handful of requests.

## Degradation

Everything except the narrative summary is an indexed fact. A node with no model
configured, or one where `hook:side-model` was never approved, still gets a fully
working dashboard; only `POST /conversations/:id/summarize` reports itself
unavailable. Numbers before narration, deliberately.

## HTTP surface

Proxied by Core at `/api/mission-control/*`. Every route is behind the injected
shared-secret bearer except `/health`.

| method | path | what |
| --- | --- | --- |
| `GET` | `/health` | un-gated loopback probe; reachability and a count, no user data |
| `GET` | `/overview?days=&since_ms=&folder_path=&limit=` | the whole dashboard in one response |
| `GET` | `/index-state` | `{conversation_id, source_updated_at, summarized}` per stored chat |
| `GET` | `/conversations?days=&folder_path=&limit=` | digest rows only |
| `PUT` | `/conversations/:id` | index (full replace) one chat's digest |
| `GET` | `/conversations/:id` | the digest plus turns, files and work items |
| `DELETE` | `/conversations/:id` | forget a chat |
| `POST` | `/conversations/:id/summarize` | narrate the session via the node's side model |

`days` wins over `since_ms` when both are sent. A non-positive `days` means *no
window*, not *the last zero days*.

## Storage

`<RYU_DIR>/mission-control.db`. Two tables: `conversation_digest` (one row per
chat; turns and file lists as JSON, because they are only ever read whole) and
`work_item` (rows, because "what is still open across this project" is a
cross-conversation query). Indexing is a full replace in one transaction — a chat
that closed a to-do must not keep the stale row.

## Port

`8010`, overridable via `RYU_MISSION_CONTROL_PORT`. Core injects the
profile-shifted value at spawn (dev is `+1000`).
