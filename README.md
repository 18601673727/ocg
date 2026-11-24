# OCG

A self-hosted workspace for AI conversations and coding agents.

OCG combines a Rust backend, a browser interface, and an embedded database. Connect model providers, reuse agent profiles, and give agents controlled access to project files and commands.

[Features](#features) · [Architecture](#architecture) · [Getting started](#getting-started) · [Performance](#performance) · [Development](#development) · [License](#license)

## Features

| Feature | Description |
| --- | --- |
| Self-hosted application | Run the backend and browser interface on your own infrastructure, with history stored in an embedded database. |
| Provider compatibility | Connect to providers using OpenAI Chat Completions, OpenAI Responses, or Anthropic Messages protocols. |
| Reusable profiles | Configure models and agents once and reuse them across conversations. Submitted jobs retain their configuration snapshots. |
| Conversations and projects | Start standalone conversations or associate work with a project and its workspace. |
| Workspace tools | Read and edit files, run approved commands, search project content, and inspect code structure. |
| Agent orchestration | Delegate work to child jobs and coordinate tasks with dependencies and execution plans. |
| Conversation controls | Queue prompts, edit or withdraw queued messages, and steer ongoing work. |
| Live transcripts | Follow streaming responses with gap recovery, compact client-side output retention, and paginated earlier history. |
| Context management | Use manual or automatic compaction, with optional predictive preparation. |
| Permissions and approvals | Control workspace access and command grants, with optional decision profiles for approval assistance. |
| History and usage | Inspect job history, tool activity, token usage, execution timing, and provider availability records. |

## Architecture

| Component | Technology | Responsibility |
| --- | --- | --- |
| [Web interface](frontend/) | Solid 2, Rsbuild, TypeScript | Browser workspace for conversations, jobs, and settings. |
| [Application API](src/api/v1/) | Rust, ntex | Authenticated application APIs, Job operations, and event streams. |
| [Execution runtime](src/system/) | Rust, ntex | Scheduling, job lifecycle, agent orchestration, and tool execution. |
| [AI services](src/service/ai/) | Rust provider adapters | Model requests, streaming responses, context preparation, compaction, and decisions. |
| [Workspace tools](src/system/executor/) | Native Rust | Authorized file access, edits, directory discovery, and command execution. |
| [Search](src/service/fts/) and [code intelligence](src/service/lsp/) | In-memory BM25 index, Tree-sitter | Full-text search and syntax-based code queries for Rust, TypeScript/TSX, and Python. |
| [Persistence](src/db/) | redb | Embedded storage with a dedicated writer and dedicated read workers. |
| [API contracts](src/cli/typescript.rs) | Rust types, ts-rs | Generated TypeScript contracts, checked for drift during frontend builds. |

## Getting started

The setup below targets Linux and macOS.

### Install

```sh
curl -fsSL https://github.com/18601673727/ocg/releases/latest/download/install.sh | sh
```

The installer installs the latest release under `~/.local/share/ocg` (data in `data/`), registers a user service (systemd on Linux, launchd on macOS), and serves OCG at [http://127.0.0.1:7840](http://127.0.0.1:7840). Pass `--version X.Y.Z`, `--prefix DIR`, `--port N`, `--no-service`, or `--uninstall` as `sh -s -- …`.

**Update (CLI):** `ocg update` supports `--check`, `--version X.Y.Z`, and `--yes`.

**Update (UI):** On a service install, use **Settings → System → Update**.

A failed update goes back to the previous version.

### Build from source

For development, build and run OCG from source.

#### Requirements

- Rust 1.99 or newer and a native C/C++ build toolchain.
- Node.js 24 and pnpm 12.15.0 to build the web interface.
- Git and the OpenSSL CLI for the commands below.

Toolchain and dependency settings are recorded in [Cargo.toml](Cargo.toml), [frontend/package.json](frontend/package.json), and the lockfiles.

#### Build

```sh
git clone https://github.com/18601673727/ocg.git
cd ocg

cargo build --locked --release --bin ocg
cargo run --locked --bin ocg-rs-ts
pnpm --dir frontend install --frozen-lockfile
pnpm --dir frontend build
```

Cargo builds the backend at `target/release/ocg`. Rsbuild writes the static site to `frontend/out`.

#### Run

From the repository root:

```sh
export OCG_DATA_DIR="$HOME/.local/share/ocg"
export OCG_API_PORT=8787
export OCG_API_TOKEN="$(openssl rand -hex 32)"
export OCG_UI_DIR="$PWD/frontend/out"

./target/release/ocg
```

Open [http://127.0.0.1:8787](http://127.0.0.1:8787).

The served web interface establishes its browser session automatically. The API token authenticates API clients; it does not need to be pasted into the browser.

> **Access control:** Anyone who can reach the served UI can obtain full operator access without a sign-in code. Keep remote access behind an SSH tunnel or [Cloudflare Access](https://developers.cloudflare.com/cloudflare-one/access-controls/applications/http-apps/self-hosted-public-app/) (Zero Trust) with an enforced authentication policy.

`OCG_DATA_DIR` is required. Set `OCG_API_PORT` and `OCG_API_TOKEN` together to enable the API. The token must be a random value of 32–512 accepted characters. When `OCG_UI_DIR` points to `frontend/out`, the Rust backend serves the static pages and the API from the same origin. [Server configuration](src/api/v1/config.rs)

The HTTP listener binds to `127.0.0.1`. For a remote installation, forward the same port over SSH:

```sh
ssh -N -L 8787:127.0.0.1:8787 your-server
```

Then open the local URL above.

### Configure your first conversation

1. Open **Settings → Models** and import provider models or create a model profile with its protocol, endpoint, and credentials.
2. Open **Settings → Agents**, assign model profiles, and choose the agent's capabilities.
3. For workspace tasks, add a project under **Settings → Projects** and configure its workspace path and allowed commands.
4. Start a conversation with the configured agent. Select a project when workspace access is needed.

Keep the OCG data directory separate from project workspaces. The [data layout](src/system/data_directory.rs) places `ocg.redb` and `credentials.json` in the data root, with logs in `ocg_logs/` and attachments in `ocg_attachments/`.

> **Storage:** This release uses database **Format 5**. `ocg migrate <data-directory> --dry-run | --apply <plan-digest> | --finalize` converts Format 1-4 databases offline (Unix only). Databases from 0.5.x releases (formats 16-18) and unmarked databases are not supported; start those with a new data directory. `ocg verify <data-directory>` checks an existing database without modifying it; the installer runs this check before switching versions and stops if it fails.

## Performance

Current: 2026-11-25, commit [`0c4bdd5`](https://github.com/18601673727/ocg/commit/0c4bdd585a9320ca5700e00fe6eea7c6151ee503). Last: 2026-11-20, commit [`1c20348`](https://github.com/18601673727/ocg/commit/1c20348b660027990d7be59ab2aa1e1e80721023). Both ran on the same 2-core ARM64 Linux machine (11.66 GiB RAM, ext4, optimized build). Each job makes 3 mock model requests and 2 real tool calls. 8 concurrent clients, 1,000 jobs, 1,000 workspace files. Values are medians of 3 trials; latency is p50 / p95.

| Metric | Last | Current | Delta |
| --- | ---: | ---: | ---: |
| Startup | 35.9 ms | 36.1 ms | +0.4% |
| Memory, idle | 17.6 MiB | 17.8 MiB | +1.2% |
| Memory, under load | 118.3 MiB | 70.3 MiB | −40.6% |
| Job admission | 25.3 / 31.9 ms | 25.6 / 31.7 ms | +1.0% / −0.7% |
| Job throughput | 31.2 jobs/s | 33.0 jobs/s | +6.0% |
| Job completion | 257 / 294 ms | 243 / 276 ms | −5.6% / −6.3% |
| Streaming delivery | 16.6 / 27.3 ms | 17.2 / 27.6 ms | +3.5% / +1.0% |
| History read | 4.3 / 10.5 ms | 3.7 / 9.5 ms | −13.2% / −9.7% |
| Workspace search | 0.036 / 0.590 ms | 0.026 / 0.282 ms | −27.8% / −52.2% |
| Database per job | 46.6 KiB | 47.1 KiB | +1.2% |

All runs finished with no failed, rejected or timed-out operations. The provider is a local mock, so model latency is not included.

<details>
<summary>Run the backend benchmark</summary>

Run from the repository root. Set `OCG_BENCH_PARENT` to an ext4 directory. Never point it at your real OCG database.

```bash
set -euo pipefail
OCG_BENCH_PARENT="${OCG_BENCH_PARENT:-$HOME}"
OUT="$(mktemp -d "$OCG_BENCH_PARENT/ocg-benchmark.XXXXXX")"
unset OCG_BENCHMARK_ADMISSION_DELAY_MS OCG_BENCHMARK_SPLIT_OUTPUT_RECORDS OCG_BENCHMARK_INJECT_WORKER_ERROR

run_bench() {
  local section="$1"; shift
  cargo bench --locked --features benchmarks --bench backend -- \
    "$section" --mode reference \
    --clients 8 --jobs 1000 --files 1000 \
    --iterations 1 --trials 3 --warmup 32 \
    --deadline-ms 120000 --stream-chunks 3 --stream-pace-ms 20 \
    --page-size 20 --writer-jobs 8 \
    --dir "$OUT/data" "$@" \
    > "$OUT/$section.jsonl" 2> "$OUT/$section.stderr"
}

run_bench startup --trials 10 --warmup 2
run_bench memory
run_bench admission
run_bench throughput
run_bench streaming
run_bench history --iterations 625 --warmup 10
run_bench search --iterations 200 --warmup 10
run_bench database --warmup 0

cat "$OUT"/{startup,memory,admission,throughput,streaming,history,search,database}.jsonl \
  > "$OUT/reference.jsonl"
cargo bench --locked --features benchmarks --bench backend -- \
  summary --concurrency 8 --mode reference \
  < "$OUT/reference.jsonl" > "$OUT/summary.md"
echo "Results: $OUT"
```

</details>

<details>
<summary>Run the frontend benchmark</summary>

It starts a local backend and mock provider, seeds a 56-turn conversation, and opens the built frontend in headless Chrome. It measures JavaScript size, update-to-screen latency and scroll smoothness. Needs Google Chrome. [Source](benchmark/frontend/)

```sh
cargo build --locked --bin ocg
cargo run --locked --bin ocg-rs-ts
pnpm --dir frontend install --frozen-lockfile
pnpm --dir frontend build
pnpm --dir benchmark/frontend install --frozen-lockfile
pnpm --dir benchmark/frontend bench
```

`OCG_BENCH_BROWSER` and `OCG_BENCH_BACKEND` select a custom Chrome or OCG binary.

</details>

## Development

Use debug builds while iterating:

```sh
cargo build --locked
cargo check --locked --benches --features benchmarks
cargo clippy --locked --all-targets --features benchmarks -- -D warnings
cargo fmt --all --check
pnpm --dir frontend lint
```

The frontend build checks generated API contracts. After changing exported Rust types, regenerate them with:

```sh
make contracts
make contracts-check
pnpm --dir frontend build
```

See [AGENTS.md](AGENTS.md) for repository conventions and contribution constraints.

## License

[MIT](LICENSE).
