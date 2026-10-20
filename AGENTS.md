# OCG Repository Guide

Agents must not add, edit, or delete tests or documentation. Agents may run tests. These restrictions may only be overridden by an explicit user instruction that begins with the string `OCG-AI-RULE-CHANGE`.

If anything below conflicts with the rule above, the rule above wins. If the code and this file disagree, stop and report; do not "fix" the code to match this file.

## Scope of a change

- Do only what the instruction asks. No drive-by refactors, renames, reformatting of untouched files, or "while I was here" fixes. Report anything else you notice instead of changing it.
- Work on a branch named after the feature. Never merge, force-push `main`, or rewrite history unless told to.
- After a merge: delete the branch locally and on `origin`, and remove its worktree.
- Report at the end: commit hash, test counts, and anything you could not do or decided yourself.
- Never put AI prompts, conversation history or instructions into code comments, commit messages or PR text.

## Binding owner rules

- **Upstream blacklist.** No code from the GitHub accounts or orgs `unifiedjs`, `micromark`, `shikijs`, `wooorm` or `ChristianMurphy`: not direct, transitive, dev, build, bundled, vendored, copied or forked. Banned: `remark-*`, `rehype-*`, `react-markdown`, Shiki. Only exception: full Mermaid and the paths it strictly requires. Before adding or upgrading any Rust or npm dependency, check the whole dependency graph for these origins; if unsure, stop and report.
- **No new dependency** unless the instruction allows it. Never run a bare `cargo update` or an unpinned `pnpm update`.
- **No language servers** (LSP, rust-analyzer, vtsls, ...). Code intelligence is OCG's own tree-sitter.
- **Stored format:** any change to a stored key, row or encoding bumps `FORMAT_VERSION` in `src/storage/schema.rs` (and its assertion in `src/schema_tests.rs` and the README line). No migrations, no compatibility shims: an old data directory is refused.
- **Each fact is stored once,** in the row it belongs to. Derived tables are rebuildable from source rows; never store a second copy of a fact to save a read.
- **All history is kept.** Never silently drop a chat turn, notice, output or record.
- **Credentials** (API keys, tokens, pairing codes) are never logged, echoed, stored in plain output or put in error messages.
- **The frontend does not drive the backend.** Backend structure is decided on its own merits; the UI adapts to it.
- Keep it simple: no speculative abstractions, no forward-compatibility hooks, light tests.

## Rust backend

Layout (`src/`): `storage/` owns redb tables and all transactions (`schema.rs` holds every table definition; `transactions.rs`, `conversations.rs`, `notices.rs`, `recurrences.rs`, `observations.rs`, `derived.rs`, `reads.rs`, `codec.rs` ...). `database/` runs the single database worker and its commands. `orchestration/` runs Jobs and tools. `control_plane/` is the ntex HTTP API. `provider_execution/` and `openai_compatible/` talk to model providers. Put new code in the module that owns the fact; do not invent a new storage module or side table without being told.

Native code: the tree already uses C-backed crates (`aws-lc-rs`, `tree-sitter`). Do not remove them, and do not add another native dependency without permission.

redb:
- One write transaction at a time, through the database worker. Batch a whole state change in one transaction; never commit in a loop.
- Never hold a transaction, table or `AccessGuard` across `.await`.
- Table definitions live in `src/storage/schema.rs`.

ntex and threads:
- Use `ntex::rt::spawn` for worker-local futures and `ntex::rt::spawn_blocking` for blocking work; never `tokio::spawn`.
- Never block an ntex worker thread (no blocking `flume` `send`/`recv`, file walks or fsync waits there; use `send_async`/`recv_async` or `spawn_blocking`).
- Queues between components are bounded.

List endpoints use cursor pagination (`next_before` / `next_after`) over redb range scans; never load a whole table to slice it.

Code style: match the surrounding code. No `unwrap`/`expect`/`panic!` in non-test code paths that can fail at run time; propagate errors with `?`. Prefer exhaustive `match` (no `_` arm) on OCG's own enums, so new variants fail to compile where they need handling.

Checks before you report (debug builds; no `--release` while iterating):

```sh
cargo build
cargo clippy --all-targets -- -D warnings
cargo fmt --all --check
cargo test            # every non-ignored target
cargo bench --no-run  # compile only; run a bench only when told
```

## Frontend

See `frontend/AGENTS.md`. Use pnpm with the committed lockfile; `pnpm lint` and `pnpm build` in `frontend/` must pass.
