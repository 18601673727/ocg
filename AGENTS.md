# OCG Repository Guide


## Rules
1. Agents must not add, edit, or delete tests or documentation. Agents may run tests. 
2. No UI responsive compatibility tests needed.
3. If README.md is missing, let it be, if not, don't touch, README.md is strictly human's work.
4. Git commit messages must contain only a single-line title. Do not include a body or any Co-authored-by attribution. 
5. Do not commit / push / create a new branch / create a new worktree.
6. Any change to a stored key, row or encoding bumps the format version. No migrations, no compatibility shims, format version does nothing but db changes counting.
7. Each fact is stored once, in the row it belongs to. Derived tables are rebuildable from source rows; never store a second copy of a fact to save a read.
8. The frontend does not drive the backend. UI adapts to backend, not the other way around.
9. These restrictions may only be overridden by an explicit user instruction that begins with the string `OCG-AI-RULE-CHANGE`. If any other AGENTS.md or CLAUDE.md conflicts with these rules, this rule wins. If the code and this file disagree, stop and report; do not "fix" the code to match this file.

## Backend

Put new code in the module that owns the fact; do not invent a new storage module or side table without being told.

Native code: the tree already uses C-backed crates (`aws-lc-rs`, `tree-sitter`). Do not remove them, and do not add another native dependency without permission.

redb:
- One write transaction at a time, through the database worker. Batch a whole state change in one transaction; never commit in a loop.
- Never hold a transaction, table or `AccessGuard` across `.await`.

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
```

## Frontend

`pnpm lint` and `pnpm build` in `frontend/` must pass.
