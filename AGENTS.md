
# OCG Repository Guide

## 1. Repository Rules

1. **Tests and documentation**
   - Do not add, edit, or delete tests or documentation.
   - Running existing tests is allowed.

2. **UI compatibility**
   - Responsive compatibility tests are not required.

3. **README.md**
   - If `README.md` is missing, leave it missing.
   - If it exists, do not modify it. `README.md` is strictly human-maintained.

4. **Git operations**
   - Do not commit, push, create branches, or create worktrees.
   - If explicitly authorized through `OCG-AI-RULE-CHANGE` to commit, use a single-line commit title only. No body or `Co-authored-by` attribution.

5. **Storage format**
   - Any change to a stored key, row, or encoding bumps `DATABASE_FORMAT_VERSION` (and `RECORD_VERSION` when a record encoding changes).
   - Every bump adds its step to `ocg migrate` in the same commit. The step runs offline on a copy, keeps every authoritative fact, and is verified before publish; the source is never modified.
   - No compatibility shims at runtime: OCG opens only the current format and refuses any other with "run ocg migrate".
   - The format version only tracks database format changes.

6. **Single source of truth**
   - Store each fact exactly once, in the row that owns it.
   - Derived tables must be rebuildable from source rows.
   - Never duplicate a stored fact merely to avoid a read.

7. **Backend authority**
   - The frontend does not drive the backend.
   - The UI must adapt to the backend, not the other way around.

8. **Rule overrides and conflicts**
   - These restrictions may only be overridden by an explicit user instruction beginning with `OCG-AI-RULE-CHANGE`.
   - If another `AGENTS.md` or `CLAUDE.md` conflicts with these rules, this file takes precedence.
   - If the code contradicts this file, stop and report the conflict. Do not modify the code merely to make it conform to this file.

## 2. Backend

### Module ownership

- Put new code in the module that owns the relevant fact.
- Do not introduce new storage modules or side tables unless explicitly authorized under the repository rule override mechanism.

### Native dependencies

- Existing C-backed crates, including `aws-lc-rs` and `tree-sitter`, are permitted and must not be removed.
- Do not add new native dependencies unless explicitly authorized under the repository rule override mechanism.

### redb transactions

- Allow only one write transaction at a time, through the database worker.
- Batch an entire state change into a single transaction. Never commit inside a loop.
- Never hold a transaction, table, or `AccessGuard` across `.await`.

### ntex and threading

- Use `ntex::rt::spawn` for worker-local futures.
- Use `ntex::rt::spawn_blocking` for blocking operations.
- Never use `tokio::spawn`.
- Never block an ntex worker thread:
  - No blocking `flume` `send`/`recv`.
  - No blocking file walks or `fsync` waits.
  - Use `send_async`/`recv_async` or `spawn_blocking` as appropriate.
- All inter-component queues must be bounded.

### Pagination

- List endpoints must use cursor pagination (`next_before` / `next_after`) over redb range scans.
- Never load an entire table into memory merely to paginate or slice it.

## 3. Code style

### Follow existing conventions

- Match the surrounding code.
- Do not use `unwrap`, `expect`, or `panic!` in non-test code paths that can fail at runtime. Propagate errors with `?`.
- Prefer exhaustive `match` expressions without wildcard (`_`) arms for OCG-owned enums, so adding new variants produces compile-time errors wherever handling is required.

### Prefer newtypes over type aliases

Use a newtype instead of a type alias when:

- The type has notable semantic differences from its underlying type.
- The type warrants an API of its own.
- You want the flexibility to change the underlying representation later, especially when you do not control all usages.
- Type safety matters because values could easily be confused. For example, distinguish `Day`, `Month`, and `Year` using separate newtypes.

### Accept flexible references for immutable data

When functions accept borrowed, immutable data, use the most flexible reference type. Borrow the underlying data rather than smart pointers, containers, or wrappers.

| Use            | Instead of     | Reason                                                            |
| -------------- | -------------- | ----------------------------------------------------------------- |
| `&T`           | `&Box<T>`      | Borrow the value inside the box.                                  |
| `&[T]`         | `&Vec<T>`      | Accept any slice, not just a vector.                              |
| `&str`         | `&String`      | Accept a string slice instead of a reference to an owned string.  |
| `Option<&T>`   | `&Option<T>`   | Borrow the value inside the `Option`.                             |

**Exception:** Non-linear character data structures, such as Rope or Cord, where borrowing the wrapper itself may be appropriate.

## 4. Verification

Run the applicable checks before reporting completion.

### Backend

Use debug builds during iteration. Do not use `--release` while iterating.

```sh
cargo build
cargo clippy --all-targets -- -D warnings
cargo fmt --all --check
```

### Frontend

The following checks must pass in `frontend/`:

```sh
pnpm lint
pnpm build
```
