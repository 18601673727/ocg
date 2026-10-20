# OCG Repository Guide

Agents must not add, edit, or delete tests or documentation. Agents may run tests. These restrictions may only be overridden by an explicit user instruction that begins with the string `OCG-AI-RULE-CHANGE`.

## Read this before touch Rust

### Architecture & Core Design Philosophy
OCG consists of a pure-Rust agent engine and an `ntex` HTTP control plane backing a Next.js observability dashboard.
- **100% Pure-Rust Dependency Policy:** OCG strictly forbids C/C++ FFI or `-sys` dependencies (e.g., NO `rusqlite`, `libsqlite3-sys`, `openssl-sys`, or `zstd-sys`). Every dependency in `Cargo.toml` must compile cleanly without a C toolchain.
- **Transactional Over Background Sync:** Do NOT introduce background aggregation state machines or polling loops to synchronize state. State transitions happen atomically inside `redb` transactions, and projections are computed on-demand or within the unified storage modules.
- **Storage Consolidation (`redb` Schema v20+):** Storage logic is strictly consolidated into three core engines—**do not create fragmented storage modules or ad-hoc side tables**:
  1. `transactions.rs` — Atomic state transitions, job lifecycle writes, and multi-turn conversation mutations.
  2. `observations.rs` — Content-addressed telemetry, execution logs, and immutable event ingestion.
  3. `derived.rs` — On-demand state computation, index lookups, and read-side projections for the control plane.
- **Control Plane & UI Contract:** The `ntex` HTTP layer serves cursor-paginated surfaces (`next_before`, `next_after`) consumed by the Next.js dashboard (such as `jobs-surface.tsx`, `logs-surface.tsx`, and observation views).

### Build, Lint & Test Commands
- **Typecheck & Lint:** `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- **Format Check:** `cargo fmt --all --check`
- **Run Tests (Targeted):** `cargo nextest run -p <crate_name>` (fallback: `cargo test -p <crate_name>`)
- **Full Verification Gate:** Always pass `cargo fmt --all --check` and `cargo clippy` with zero warnings before finishing a task.
- **NEVER** run `--release` builds during normal iteration or testing; debug builds compile faster and preserve debug assertions.
- **NEVER** run a bare `cargo update`. Use `cargo update --precise <version>` only when explicitly instructed.
- **NEVER** introduce a crate into `Cargo.toml` without verifying its feature flags disable any default C/native bindings.

### Core Subsystem Guardrails

1. `redb` Storage & Schema Rules (`transactions.rs`, `observations.rs`, `derived.rs`)
- **Centralized Table Definitions:** Define all `redb::TableDefinition` and `MultimapTableDefinition` items as `const` in the schema definition layer:
  ```rust
  const OBSERVATIONS_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("observations");
  ```
- **Schema Versioning (v20+):** Any modification to key layout, composite keys, or value serialization MUST explicitly account for the `redb` schema version and belong inside `transactions.rs`, `observations.rs`, or `derived.rs`.
- **Content-Addressed Telemetry:** When writing to `observations.rs`, preserve deterministic content addressing (hash-keyed blobs/events) to prevent duplicate telemetry bloat across multi-turn agent runs.
- **Strict Transaction Lifetimes:**
  - **Write Path (`transactions.rs` / `observations.rs`):** `redb` permits only ONE active `WriteTransaction` at a time. Batch multi-key updates and index maintenance inside a single `WriteTransaction` rather than committing in a loop. Never block an `ntex` worker thread waiting on a contended write lock or `fsync`—route async writes through the bounded `flume` ingestion/writer pipeline or blocking pool.
  - **Read Path (`derived.rs`):** `ReadTransaction` is MVCC and non-blocking. Open the `ReadTransaction`, compute the derived view or slice the cursor range, copy/deserialize out of `AccessGuard`, and drop the transaction BEFORE any `.await` point.
- **NEVER Hold `redb` Guards Across `.await`:** `ReadTransaction`, `WriteTransaction`, `Table`, and `AccessGuard` must never cross an async suspension point.

2. `ntex` Control Plane & Cursor Pagination Rules
- **Single-Threaded Worker Model:** `ntex` runs a local executor per worker thread. Do NOT pollute `ntex` handlers, extractors, or service middlewares with unnecessary `+ Send + Sync + 'static` bounds.
- **Task Spawning:** ALWAYS use `ntex::rt::spawn` for worker-local futures. NEVER use `tokio::spawn` inside the `ntex` layer.
- **Cursor-Paginated Surfaces (`next_before` / `next_after`):**
  - All list endpoints backing Next.js surfaces (`logs`, `jobs`, `observations`) MUST use deterministic cursor pagination (`next_before`, `next_after`) backed by `redb` range scans (`table.range(...)`) in `derived.rs`.
  - **NEVER** implement offset/limit (`SKIP`/`OFFSET`) pagination or load entire tables into memory (`Vec`) before slicing.
  - Ensure cursor tokens are validated cleanly and return structured HTTP 400 errors via `ntex::web::WebResponseError` when malformed.

3. `flume` Concurrency & Backpressure Rules
- **No Unbounded Queues:** ALWAYS use `flume::bounded(capacity)` between `ntex` control-plane/agent workers and background `redb` committers or execution loops.
- **Async vs. Sync Context Separation (CRITICAL):**
  - Inside `async` functions (`ntex` handlers, async agent steps): ALWAYS use `sender.send_async(msg).await` and `receiver.recv_async().await`.
  - Inside dedicated synchronous OS threads: ALWAYS use `sender.send(msg)` and `receiver.recv()`.
  - **NEVER** call blocking `.send()` or `.recv()` on an `ntex` worker thread—it will stall all concurrent HTTP connections on that core.
- **Clean Shutdown:** Treat `flume::SendError` and `flume::RecvError::Disconnected` as explicit lifecycle termination signals rather than panicking.

### Rust Idioms & Code Hygiene
- **Zero-Panic Policy:** NEVER use `.unwrap()`, `.expect()`, `panic!()`, or `unreachable!()` in non-test code. Propagate errors with `?` and `thiserror`.
- **Exhaustive Matching:** PREFER exhaustive `match` statements without wildcard (`_`) arms over `matches!`, ensuring new enum variants (especially job states, observation types, and schema versions) trigger compiler errors everywhere they need handling.
- **Modern Control Flow:** PREFER `let` chains (`if let Some(x) = opt && x.is_valid()`) and `let ... else { return ...; }` guard clauses over deeply nested conditionals.
- **Lint Suppression:** PREFER `#[expect(clippy::...)]` over `#[allow(clippy::...)]` with a brief reason comment.
- **Imports:** Keep all `use` statements at the top of the module or `#[cfg(test)]` block—never inside functions.
- **Isolated Storage Tests:** In tests, ALWAYS initialize `redb::Database` with `redb::backends::InMemoryBackend::new()` or `tempfile::NamedTempFile` so `nextest` runs cleanly in parallel.
- **No Prompt Leakage:** NEVER reference AI prompts, conversation history, or refactoring instructions in code comments, commit messages, or PR descriptions.

## Read this before touch frontend
https://github.com/vercel/next.js/blob/canary/AGENTS.md

If any rule from the links above conflicts with the rule stated at the top, the rule stated at the top is the sole authority.