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
| [Web interface](frontend/) | Next.js, React, TypeScript | Browser UI built as a static export and served by the backend. |
| [Control API](src/api/v1/) | Rust, ntex | Authenticated application APIs, job control, and event streams. |
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

- Rust 1.98 or newer and a native C/C++ build toolchain.
- Node.js 24 and pnpm 12.15.0 to build the web interface.
- Git and the OpenSSL CLI for the commands below.

Toolchain and dependency settings are recorded in [Cargo.toml](Cargo.toml), [frontend/package.json](frontend/package.json), and the lockfiles.

#### Build

```sh
git clone https://github.com/18601673727/ocg.git
cd ocg

cargo build --locked --release --bin ocg
pnpm --dir frontend install --frozen-lockfile
pnpm --dir frontend build
```

The build produces the backend at `target/release/ocg` and exports the web interface to `frontend/out`.

#### Run

From the repository root:

```sh
export OCG_DATA_DIR="$HOME/.local/share/ocg"
export OCG_CONTROL_PORT=8787
export OCG_CONTROL_TOKEN="$(openssl rand -hex 32)"
export OCG_UI_DIR="$PWD/frontend/out"

./target/release/ocg
```

Open [http://127.0.0.1:8787](http://127.0.0.1:8787).

The served web interface establishes its browser session automatically. The control token authenticates API clients; it does not need to be pasted into the browser.

> **Access control:** Anyone who can reach the served UI can obtain full operator access without a sign-in code. Keep remote access behind an SSH tunnel or [Cloudflare Access](https://developers.cloudflare.com/cloudflare-one/access-controls/applications/http-apps/self-hosted-public-app/) (Zero Trust) with an enforced authentication policy.

`OCG_DATA_DIR` is required. `OCG_CONTROL_PORT` and `OCG_CONTROL_TOKEN` enable the control API together; the token must be a random value of 32–512 accepted characters. `OCG_UI_DIR` selects the exported frontend directory. See the [control configuration](src/api/v1/config.rs) for the exact rules.

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

> **Storage compatibility:** Current `main` uses database format **18**. With `OCG_DATA_DIR` set to the data directory, `ocg migrate --check` reports whether it needs migration, and `ocg migrate --to <directory>` writes a migrated copy. `install.sh` and `ocg update` run the check before switching versions and stop if migration is needed. Back up the data directory before upgrading.

## Performance

Measured on **2026-11-01** from clean [`main` (`9a5a9be`)](https://github.com/18601673727/ocg/commit/9a5a9be02f0368bc2d5999259980078c3ff50670), using a native optimized Cargo benchmark build with Rust 1.99.1. All eight sections below used the same binary and source revision.

**Machine:** Debian Linux 6.12.113, ARM64 Neoverse-N1 (2 logical CPUs at 2.0 GHz), 11.66 GiB RAM, and **ext4** storage on `/dev/sda2`. The existing OCG service remained running and was not modified. The benchmark database was isolated on ext4, not tmpfs.

Runtime workloads use a deterministic local mock provider with the real backend, workspace tools, and persistence. Each reference job makes **3 model requests**, executes **2 real tool calls**, and produces **1 validated completion**. C8 means configured concurrency 8.

| Metric | Result | Workload |
| --- | --- | --- |
| Startup | **84.35 ms** median; 83.37–85.92 ms range | 10 launches; 1,000 completed-job database; warm OS cache; authenticated readiness |
| Memory | **18.06 MiB** idle; **106.91 MiB** sampled loaded peak | Isolated backend; C8; 1,000 jobs/trial; nominal 20 ms RSS polling |
| Job admission | **60.63 ms** p50; **77.32 ms** p95 | C8; 1,000 confirmed admissions/trial; completion drain excluded |
| Job throughput | **37.47 jobs/s**; **213.50 ms** completion p50; **239.34 ms** p95 | Closed-loop C8; 1,000 validated reference jobs/trial |
| Streaming delivery | **17.10 ms** p50; **27.89 ms** p95 | C8; 1,000 jobs / 3,000 markers per trial at 20 ms pacing; 5 ms output-flush interval; upstream emission to downstream SSE observation |
| History read | **2.98 ms** p50; **8.04 ms** p95 | C8; 20-row first/cursor pages; 1,000-job fixture; 10,000 reads and 88–96 completed background jobs/trial |
| Workspace search | **0.039 ms** p50; **0.650 ms** p95 | C8; warm index; 1,000 files / 3,968,000 indexed bytes; 11,200 queries/trial across seven classes |
| Database footprint | **50.07 MiB** median; **51.18 KiB** incremental growth per completed job | 1,000 completed jobs/trial; cleanly closed `ocg.redb` |

Except for startup, displayed results are medians of three trial statistics. Latency p50 and p95 are each the median of the corresponding per-trial percentile. The memory value is the median of the three sampled loaded peaks, which ranged from **106.07 to 107.18 MiB**.

Database growth is relative to an equivalent empty baseline of **86,016 bytes** and excludes logs and attachments. Each fixture contained **1,000 validated completed jobs**, with execution configured at **C8**. Storage figures describe this measured workload and are not a fixed per-job storage cost.

<details>
<summary>Measurement details and search breakdown</summary>

The [benchmark harness](benchmark/) exercises the existing runtime and validates completed work.

- Runtime warm-up uses 32 separate jobs before each measured memory, admission, throughput, and streaming trial. Warm-up work is excluded from the results.
- Startup prepares and cleanly closes its 1,000-job fixture outside the timer, performs two warm-up reopens, then measures ten launches. Readiness includes authenticated health and basic history reads.
- Admission ends at confirmed admission. Completion drain and validation follow outside its timing window.
- Throughput and completion timing end at observed settlement. The subsequent evidence audit must pass for a trial to be valid.
- Memory samples cover the loaded measurement window. The 20 ms interval is the configured polling interval, and the peak is the highest RSS observed by sampling.
- Streaming delivery measures propagation of paced markers through the runtime, durable persistence, and downstream SSE observation. The 5 ms setting controls the pending-output buffer timer, not the end-to-end delivery time.
- History uses `/api/v1/job-history` with eight readers, 625 first-page/cursor-page pairs per reader, and background reference jobs in batches of eight.
- Search calls the production `WorkspaceFts` API directly, using a warm index and the fixed seven-class query mix. Each class has 1,600 successful samples per trial.
- Database measurements use three separately prepared baseline/loaded pairs and no warm-up jobs.

All required measured work completed successfully, with zero rejected, failed, timed-out, or unfinished operations. These are bounded 1,000-job measurements with a local mock provider; provider inference latency and maximum sustainable load are outside their scope.

The memory trials reported a median of **0.71 core** of backend CPU utilization. The workspace index held **1,000 files**, **528,499 tokens**, and **3,968,000 content bytes**. Its estimated memory footprint was **9,185,237 bytes** (8.76 MiB). Median initial build and rebuild times were **103.39 ms** and **109.73 ms**, respectively.

Search latency varies substantially by query class. The table below reports medians of the three corresponding per-trial statistics; the aggregate row above comes from the same mixed workload.

| Query class | p50 | p95 |
| --- | ---: | ---: |
| `rare` | 0.036 ms | 0.046 ms |
| `common` | 0.316 ms | 12.340 ms |
| `and` | 0.035 ms | 0.046 ms |
| `phrase` | 0.028 ms | 0.030 ms |
| `missing` | 0.016 ms | 0.018 ms |
| `broad_phrase` | 0.334 ms | 12.373 ms |
| `broad_phrase_zero` | 0.555 ms | 12.570 ms |

The positional phrase index avoids most candidate file scans for nonmatching phrases. Individual broad-query p95 values can still exceed the mixed-query p95 because each class contributes only part of the query mix. Phrase candidate, time, and index-coverage limits still apply.

</details>

<details>
<summary>Previous ext4 reference and observed changes</summary>

The previous reference was measured on **2026-10-30** from clean [`6ac6023`](https://github.com/18601673727/ocg/commit/6ac6023c0a7b9c484922855910b6a80991ad19b2) on the same two-core ARM64/ext4 machine. Both runs used Rust 1.99.1 and the same C8 reference profile, including the 5 ms output-flush interval. The previous kernel was Linux 6.12.107; this run used Linux 6.12.113.

| Metric | Previous `6ac6023` | Current `9a5a9be` | Observed change |
| --- | ---: | ---: | ---: |
| Startup median | 85.18 ms | 84.35 ms | −1.0% |
| Idle RSS | 18.66 MiB | 18.06 MiB | −3.2% |
| Loaded peak RSS | 112.34 MiB | 106.91 MiB | −4.8% |
| Admission p50 / p95 | 59.82 / 77.31 ms | 60.63 / 77.32 ms | +1.4% / +0.0% |
| Throughput | 37.84 jobs/s | 37.47 jobs/s | −1.0% |
| Completion p95 | 236.05 ms | 239.34 ms | +1.4% |
| Streaming delivery p50 / p95 | 16.91 / 27.20 ms | 17.10 / 27.89 ms | +1.1% / +2.5% |
| History read p50 / p95 | 3.37 / 9.31 ms | 2.98 / 8.04 ms | −11.3% / −13.6% |
| Workspace search p50 / p95 | 0.039 / 0.651 ms | 0.039 / 0.650 ms | +0.5% / −0.2% |
| Database file size | 60.00 MiB | 50.07 MiB | −16.6% |
| Database growth/job | 61.36 KiB | 51.18 KiB | −16.6% |

These are separate runs with different source revisions, kernel versions, run times, and OS cache states. The table reports observed differences; it does not isolate the effect of a particular code change or establish changes in steady-state performance. The previous raw evidence remains on the benchmark host in `/srv/ocg-bench-20261030-220740/`.

</details>

<details>
<summary>Reproduce the reference profile</summary>

Run from the repository root in Bash at commit `9a5a9be02f0368bc2d5999259980078c3ff50670`. The script creates a fresh result directory and runs all eight sections sequentially. To compare against the published numbers, run on a matching two-core ARM64 machine and set `OCG_BENCH_PARENT` to a directory on **ext4**. The published run used `/srv/ocg-bench-20261101-9a5a9be.MO1tF5`; do not reuse its data directory or the production OCG database.

```bash
set -euo pipefail

OCG_BENCH_PARENT="${OCG_BENCH_PARENT:-$HOME}"
OCG_BENCH_RESULTS="$(mktemp -d "$OCG_BENCH_PARENT/ocg-benchmark.XXXXXX")"

unset OCG_BENCHMARK_ADMISSION_DELAY_MS
unset OCG_BENCHMARK_SPLIT_OUTPUT_RECORDS
unset OCG_BENCHMARK_INJECT_WORKER_ERROR

run_bench() {
  local section="$1"
  shift
  cargo bench --locked --features benchmarks --bench backend -- \
    "$section" --mode reference \
    --clients 8 --jobs 1000 --files 1000 \
    --iterations 1 --trials 3 --warmup 32 \
    --deadline-ms 120000 --stream-chunks 3 --stream-pace-ms 20 \
    --page-size 20 --writer-jobs 8 \
    --dir "$OCG_BENCH_RESULTS/data" "$@" \
    > "$OCG_BENCH_RESULTS/$section.jsonl" \
    2> "$OCG_BENCH_RESULTS/$section.stderr"
}

run_bench startup --trials 10 --warmup 2
run_bench memory
run_bench admission
run_bench throughput
run_bench streaming
run_bench history --iterations 625 --warmup 10
run_bench search --iterations 200 --warmup 10
run_bench database --warmup 0

for section in startup memory admission throughput streaming history search database; do
  cat "$OCG_BENCH_RESULTS/$section.jsonl"
done > "$OCG_BENCH_RESULTS/reference.jsonl"

cargo bench --locked --features benchmarks --bench backend -- \
  summary --concurrency 8 --mode reference \
  < "$OCG_BENCH_RESULTS/reference.jsonl" \
  > "$OCG_BENCH_RESULTS/summary.md"

printf 'Results: %s\n' "$OCG_BENCH_RESULTS"
```

The published ext4 benchmark's binary SHA-256 was `1c5d88774e7f3712d27a481624c9eac1a235a31fb666ebabdaf9b2d0b513fb2d`. Its raw JSONL and stderr evidence is preserved on the benchmark host in `/srv/ocg-bench-20261101-9a5a9be.MO1tF5/`. These paths identify measurement artifacts on that host, not files included in the repository.

The harness records environment, workload, trial statistics, and outcomes in JSONL, with Git metadata when available. Keep the exact source commit, binary identity, raw records, stderr, and filesystem details together. Keep results from different revisions and filesystems separate. The [summary implementation](benchmark/summary.rs) reports medians of per-trial statistics.

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
