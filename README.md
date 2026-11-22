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

### Backend reference (2026-11-20)

Measured [`1c20348`](https://github.com/18601673727/ocg/commit/1c20348b660027990d7be59ab2aa1e1e80721023) with the benchmark executor fix in this working tree on `chuncheon`, using the optimized Cargo benchmark profile and Rust 1.99.1. The host ran Debian Linux 6.12.113 on ARM64 with 2 vCPUs and 11.66 GiB RAM. Benchmark data used a separate ext4 directory on `/dev/sda2`. The existing OCG service remained running.

The reference workload used eight clients, a benchmark Pool with capacity eight and eight compatible Executors, 1,000 jobs, 1,000 workspace files, three trials and 32 warm-up jobs. An independent preflight observed eight running allocations simultaneously. Startup used ten measured launches and two warm-up launches. History used 625 page pairs per reader and search used 200 iterations per reader. Each reference job made three mock model requests and two real workspace tool calls. Except for startup, values are medians of the three corresponding trial statistics.

| Metric | Result |
| --- | --- |
| Startup readiness | 35.93 ms median, 35.54–36.15 ms range |
| Memory | 17.60 MiB idle, 118.33 MiB sampled loaded peak |
| Job admission | 25.34 ms p50, 31.88 ms p95 |
| Job throughput | 31.17 jobs/s, 257.42 ms completion p50, 294.02 ms p95 |
| Streaming delivery | 16.62 ms p50, 27.31 ms p95 |
| History read | 4.28 ms p50, 10.48 ms p95 |
| Workspace search | 0.036 ms p50, 0.590 ms p95 |
| Database footprint | 45.57 MiB, 46.57 KiB apparent growth per completed job |

All eight sections completed successfully, with 73 successful measurements and no failed, rejected, timed-out or unfinished operations. The memory peak is sampled. Database growth uses an equivalent empty baseline and apparent file lengths, which can differ from allocated disk space.

The previous `d421854` run reported 11.67 jobs/s and 714.79 ms completion p95. It used eight clients but created only one benchmark Executor. Together with the one default Executor, that limited execution to two simultaneous allocations. Its results are preserved at `/srv/ocg-backend-bench-d421854/`.

The earlier ext4 reference below reported 38.10 jobs/s and 236.15 ms completion p95 with eight execution slots. The current reference offers 22 tools per model request, including 12 Job-management tools, while the earlier workload offered 10. In a separate three-trial experiment on the current snapshot, an agent with coding capability only retained the two file reads and increased throughput to 32.60 jobs/s median. A separate experimental cache for compiled parameter validators retained all offered tools and raised throughput to 32.10 jobs/s median while reducing backend CPU time from 27 to 24 seconds per 1,000 jobs. Combining the coding-only agent with the cache yielded 33.09 jobs/s median and 19 seconds of backend CPU time. These experiments change different aspects of the workload or implementation and do not fully explain the remaining difference. CPU sampling also identified schema compilation and redb activity; traced write-call counts increased in the newer version. The precise contribution of the remaining changes is unquantified.

The measured source snapshot, build log, preflight, per-section JSONL and stderr, combined results and summary are preserved at `/srv/ocg-backend-bench-1c20348/`. All 382 tracked source files were checked against the measured snapshot, including the uncommitted benchmark patch. The combined JSONL SHA-256 is `23e1d33ed526af3f26ad52e22f19b56de92db99faee37386ca949456b0d9da26`. The extracted source has no Git metadata, so the benchmark's reported clean-tree status does not independently identify the commit. The diagnostic cache is isolated at `/srv/ocg-bench-schema-cache-probe/`. Use the eight-section commands in the earlier ext4 reference to reproduce this run.

### Long-conversation frontend benchmark

The benchmark starts an isolated Rust backend and a local mock model provider, seeds 56 conversation turns, and opens the production frontend in headless Chrome. It measures initial JavaScript size, latency from provider updates to the browser DOM, and scroll frame intervals. [Benchmark source](benchmark/frontend/)

Install Google Chrome in addition to the build requirements. Run these commands from the repository root.

```sh
cargo build --locked --bin ocg
cargo run --locked --bin ocg-rs-ts
pnpm --dir frontend install --frozen-lockfile
pnpm --dir frontend build
pnpm --dir benchmark/frontend install --frozen-lockfile
pnpm --dir benchmark/frontend bench
```

The default schedule runs once at 1× CPU and three times at 4×. To run only the throttled rounds, use `pnpm --dir benchmark/frontend bench 4,4,4`. The runner prints the path to `benchmark.json` in its temporary output directory. Set `OCG_BENCH_BROWSER` to use a custom Chrome or Chromium executable or `OCG_BENCH_BACKEND` to use a custom OCG binary.

### Backend comparison (2026-11-17)

Compared the backend before and after the recent five-commit change: baseline [`3386cac`](https://github.com/18601673727/ocg/commit/3386cacc1f37dc4e674c74df2c1a5563f1963805) and current [`77dff6f`](https://github.com/18601673727/ocg/commit/77dff6ff166b924e214914cc1ad13d991cd84294). Both used the matching benchmark harness for their revision.

**Machine:** Linux ARM64 Neoverse-N1, 2 vCPUs, 11 GiB RAM. Rust 1.99.1, optimized Cargo bench profile, and **tmpfs** benchmark storage. The production service was not used. Runtime workloads exercised the authenticated backend HTTP surface, real workspace tools, persistence, and a deterministic mock provider.

The `all` run used reference mode, 3 trials, 1 warm-up, 1,000 jobs, 1,000 workspace files, 50 iterations, and client concurrency 1 and 8. Values below are medians of per-trial statistics.

| Metric | Baseline → current | Change |
| --- | --- | ---: |
| Startup readiness p50 | 84.18 → 32.67 ms | **−61.2%** |
| Admission p95 | C1: 2.72 → 2.86 ms; C8: 9.42 → 9.41 ms | C1 **+5.2%**; C8 −0.1% |
| Throughput | C1: 11.57 → 11.62; C8: 23.06 → 23.01 jobs/s | C1 +0.5%; C8 −0.2% |
| Streaming delivery p50 | C1: 11.94 → 11.32 ms; C8: 11.43 → 10.70 ms | −5.2%; −6.4% |
| History read p95 | C1: 1.32 → 1.47 ms; C8: 6.63 → 6.23 ms | +11.6%; −6.0% |
| Database after 1,000 completed jobs | C1: 40.7 → 36.2 MiB; C8: 72.3 → 54.2 MiB | **−11%; −25%** |
| Database growth per completed job | C1: 40.64 → 36.03 KiB; C8: 72.99 → 54.48 KiB | **−11%; −25%** |
| Loaded peak RSS (separate 100-job run) | C1: 32.96 → 32.43 MiB; C8: 34.81 → 34.17 MiB | −1.6%; −1.8% |
| FTS build, 64 MiB corpus | 506.29 → 464.60 ms | −8.2% |
| FTS full rebuild, 64 MiB corpus | 503.50 → 465.36 ms | −7.6% |
| Mixed search latency, C8 p50 / p95 / p99 | 0.039 / 2.576 / 12.649 → 0.036 / 0.624 / 16.345 ms | −8.8% / −75.8% / **+29.2%** |

The mixed-query p99 increase is a tail-latency regression to watch; its p99 varied between 12.6 and 16.6 ms across trials. Database file size also varied between trials. This was one baseline-then-current run on the same VM, so treat those tail and footprint changes as directional.

The main `all` run produced 159 successful measurements per revision, with every section complete and no diagnostics. The `all` command omits the memory section; memory was run separately with 100 jobs and 3 trials for each concurrency. This backend benchmark does not measure browser rendering or interaction latency.

To repeat the main profile on a comparable machine:

```sh
cargo bench --features benchmarks --bench backend -- \
  all --mode reference --trials 3 --warmup 1 --iterations 50 \
  --jobs 1000 --files 1000 --clients 1,8 --dir /tmp/ocg-benchmark
```

### Earlier ext4 reference (2026-11-07)

Measured on **2026-11-07** (Singapore time) from the source in this commit, based on [`728aab5`](https://github.com/18601673727/ocg/commit/728aab570cbcc0a7096e652bd4a6badd0eb7a00f), using a native optimized Cargo benchmark build with Rust 1.99.1. All eight sections below used the same binary and source snapshot, including the 5 ms output flush and the `ntex-loopback-tcp-nodelay-v1` benchmark client.

**Machine:** Debian Linux 6.12.113, ARM64 Neoverse-N1 (2 logical CPUs at 2.0 GHz), 11.66 GiB RAM, and **ext4** storage on `/dev/sda2`. The existing OCG service remained running and was not modified. The benchmark database was isolated on ext4, not tmpfs.

Runtime workloads use a deterministic local mock provider with the real backend, workspace tools, and persistence. Each reference job makes **3 model requests**, executes **2 real tool calls**, and produces **1 validated completion**. C8 means configured concurrency 8.

| Metric | Result | Workload |
| --- | --- | --- |
| Startup | **85.04 ms** median; 83.13–146.83 ms range | 10 launches; 1,000 completed-job database; warm OS cache; authenticated readiness |
| Memory | **18.14 MiB** idle; **105.49 MiB** sampled loaded peak | Isolated backend; C8; 1,000 jobs/trial; nominal 20 ms RSS polling |
| Job admission | **20.91 ms** p50; **37.09 ms** p95 | C8; 1,000 confirmed admissions/trial; completion drain excluded |
| Job throughput | **38.10 jobs/s**; **209.43 ms** completion p50; **236.15 ms** p95 | Closed-loop C8; 1,000 validated reference jobs/trial |
| Streaming delivery | **16.37 ms** p50; **26.49 ms** p95 | C8; 1,000 jobs / 3,000 markers per trial at 20 ms pacing; 5 ms output-flush interval; upstream emission to downstream SSE observation |
| History read | **2.93 ms** p50; **8.52 ms** p95 | C8; 20-row first/cursor pages; 1,000-job fixture; 10,000 reads and 88–96 completed background jobs/trial |
| Workspace search | **0.040 ms** p50; **0.608 ms** p95 | C8; warm index; 1,000 files / 3,968,000 indexed bytes; 11,200 queries/trial across seven classes |
| Database footprint | **44.91 MiB** median; **45.90 KiB** apparent growth per completed job | 1,000 completed jobs/trial; cleanly closed `ocg.redb` |

Except for startup, displayed results are medians of three trial statistics. Latency p50 and p95 are each the median of the corresponding per-trial percentile. The memory value is the median of the three sampled loaded peaks, which ranged from **105.03 to 106.30 MiB**.

Database growth is relative to an equivalent empty baseline of **86,016 bytes** and excludes logs and attachments. Each fixture contained **1,000 validated completed jobs**, with execution configured at **C8**. These are apparent file lengths, not allocated disk bytes: sparse regions can make the two differ. Apparent growth ranged from **38.34 to 61.36 KiB per workload** across the three trials. Storage figures describe this workload and are not a fixed per-job cost.

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

The memory trials reported a median of **0.70 core** of backend CPU utilization. The workspace index held **1,000 files**, **528,499 tokens**, and **3,968,000 content bytes**. Its estimated memory footprint was **9,185,237 bytes** (8.76 MiB). Median initial build and rebuild times were **104.20 ms** and **108.56 ms**, respectively.

Search latency varies substantially by query class. The table below reports medians of the three corresponding per-trial statistics; the aggregate row above comes from the same mixed workload.

| Query class | p50 | p95 |
| --- | ---: | ---: |
| `rare` | 0.037 ms | 0.047 ms |
| `common` | 0.320 ms | 12.343 ms |
| `and` | 0.036 ms | 0.047 ms |
| `phrase` | 0.029 ms | 0.031 ms |
| `missing` | 0.016 ms | 0.018 ms |
| `broad_phrase` | 0.337 ms | 12.376 ms |
| `broad_phrase_zero` | 0.545 ms | 12.565 ms |

The positional phrase index avoids most candidate file scans for nonmatching phrases. Individual broad-query p95 values can still exceed the mixed-query p95 because each class contributes only part of the query mix. Phrase candidate, time, and index-coverage limits still apply.

</details>

<details>
<summary>Previous ext4 reference and observed changes</summary>

The previous reference was measured on **2026-11-07** from clean [`728aab5`](https://github.com/18601673727/ocg/commit/728aab570cbcc0a7096e652bd4a6badd0eb7a00f) on the same two-core ARM64/ext4 machine. It used a **50 ms** output-flush interval and the default benchmark HTTP client; this run uses **5 ms** and `ntex-loopback-tcp-nodelay-v1`. The current source also contains the Executor admission consistency correction.

| Metric | Previous clean `728aab5` | Current snapshot | Observed change |
| --- | ---: | ---: | ---: |
| Startup median | 85.00 ms | 85.04 ms | +0.0% |
| Idle RSS | 18.02 MiB | 18.14 MiB | +0.7% |
| Loaded peak RSS | 105.41 MiB | 105.49 MiB | +0.1% |
| Admission p50 / p95 | 58.67 / 78.71 ms | 20.91 / 37.09 ms | −64.4% / −52.9% |
| Throughput | 37.56 jobs/s | 38.10 jobs/s | +1.4% |
| Completion p95 | 239.38 ms | 236.15 ms | −1.4% |
| Streaming delivery p50 / p95 | 42.69 / 72.49 ms | 16.37 / 26.49 ms | −61.7% / −63.5% |
| History read p50 / p95 | 2.95 / 8.23 ms | 2.93 / 8.52 ms | −0.5% / +3.5% |
| Workspace search p50 / p95 | 0.040 / 0.608 ms | 0.040 / 0.608 ms | +0.2% / −0.0% |
| Apparent database file size | 50.07 MiB | 44.91 MiB | −10.3% |
| Apparent database growth/workload | 51.18 KiB | 45.90 KiB | −10.3% |

These are separate runs with different source snapshots, client behavior, run times, and OS cache states. The table reports observations and does not isolate a particular change or establish steady-state performance. Database file lengths may include sparse regions and do not establish allocated disk usage. Previous raw evidence remains on the benchmark host in `/srv/ocg-bench-20261107-728aab5.7UgXe3/`.

</details>

<details>
<summary>Reproduce the reference profile</summary>

Run from the repository root in Bash at the commit containing this README. The script creates a fresh result directory and runs all eight sections sequentially. To compare against the published numbers, run on a matching two-core ARM64 machine and set `OCG_BENCH_PARENT` to a directory on **ext4**. The published run used `/srv/ocg-bench-current.zewYsE`; do not reuse its data directory or the production OCG database.

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

The measured benchmark binary SHA-256 was `aaf91651c70cc64e04f19fe78788cc20556ad08ca0b4250c97eb026a206b193c`. Raw JSONL, stderr, the source snapshot, source manifest, patch, build records, and checksums are preserved on the benchmark host in `/srv/ocg-bench-current.zewYsE/`. Before the run, all **324 tracked non-README files** in that snapshot were verified byte for byte against the working tree. The manifest SHA-256 is `dee8390539e65b24354480af8748a3724fbc115ec6c8e35f2713a5951f58a142`; the source and benchmark patch against `728aab5` has SHA-256 `d0b3a38941db70734ddd066cb06191bf05ae2dbd7a2c24191bea9707af15befe`. The extracted snapshot has no Git metadata: its reported `dirty_tree: false` does not independently verify cleanliness. These paths identify artifacts on that host, not files included in the repository.

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
