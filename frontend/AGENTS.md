<!-- BEGIN:nextjs-agent-rules -->

# This is NOT the Next.js you know

This version has breaking changes — APIs, conventions, and file structure may all differ from your training data. Read the relevant guide in `node_modules/next/dist/docs/` (resolved from this file's directory; in monorepos the `next` package may not be visible from the repo root) before writing any code. Heed deprecation notices.

This block is written and re-added by `next dev` — verify at `node_modules/next/dist/server/lib/generate-agent-files.js`. Removing it from a diff only re-creates the uncommitted change; committing it with your work keeps the tree clean.

<!-- END:nextjs-agent-rules -->

# OCG Frontend Scope

This directory is the canonical OCG Next.js frontend. Work from the parent
repository root when a task spans frontend and Rust backend code; verify the
root with `git rev-parse --show-toplevel` before editing.

## Commands

Use pnpm with the committed lockfile:

```bash
pnpm install --frozen-lockfile
pnpm exec tsc --noEmit
pnpm lint
pnpm test
pnpm build
```

The scripts and test selection are authoritative in `package.json`.
`pnpm-workspace.yaml` is intentionally minimal; do not introduce a workspace
rewrite for a local task.

## Existing Runtime Contract

Preserve the existing runtime implementation and its tests. In particular,
`components/ocg/runtime/` owns snapshot/event reconciliation, generation and
sequence ordering, deduplication, stale-event rejection, project isolation,
transport, and command correlation. Job, execution, logs, ledger,
attention, project, layout, sidebar, and topbar components consume those
projections. Extend those canonical modules instead of creating a second
runtime store or a parallel Job representation.

Keep local `.env*`, `.next/`, `node_modules/`, build output, TypeScript build
metadata, and `.ocg/` out of commits. Do not copy a nested `.git`
directory into this repository.

## React AGENTS.md
https://github.com/vercel-labs/agent-skills/blob/main/skills/react-best-practices/AGENTS.md

## Control Wire Contract

`components/ocg/contracts/` is the only sanctioned way to talk to the loopback
control server. `generated.ts` is projected from the Rust definitions in
`src/contracts.rs` by `cargo run --bin ocg-rs-ts` (run `make contracts` from the
repository root) and is committed. It is generated, not hand-edited, and
`tests/contracts.rs` fails when it drifts from Rust.

Import contract types and their decoders from `components/ocg/contracts`, never
from `./generated` directly, so the type and the check that enforces it stay
together. Do not redeclare a type Rust already owns, and do not use an `as`
cast to turn an `unknown` payload into a contract type: a cast is what allowed a
renamed Rust field to reach the UI as `undefined`. A payload that does not match
its contract raises a `ContractError` naming the path; let it, and report it
rather than rendering a partly-typed view.

Adding a **required** field on the Rust side breaks `tsc` in the decoder, which
is the point. Adding an **optional** one does not: the decoder omits it, so the
PWA ignores a field it does not understand rather than breaking against a newer
backend. If the PWA should read such a field, add the check to its decoder
rather than reading it off the wire type.

