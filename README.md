# OCG 0.5.0

OCG is a local, single-operator host that runs durable Provider Jobs against a
Workspace using an OpenAI-compatible LLM. It records Jobs, Profiles, outputs and
attachments in a local data directory, and can serve an authenticated Control
Plane and a static browser console on the loopback interface.

## Build

Rust 1.98 or later, Node.js and pnpm are required.

```sh
cargo build --release
```

This writes the executable to `target/release/ocg`.

```sh
cd frontend
pnpm install
pnpm build
```

`pnpm build` exports the static console to `frontend/out`.

## Run the host

With no arguments, the host runs until it receives `SIGINT`, `SIGTERM`,
`SIGQUIT` or `SIGHUP`, then stops in the reverse order of starting.

```sh
OCG_DATA_DIR=/absolute/path/to/ocg-data ./target/release/ocg
```

The data directory is created when missing, and its path is printed to stderr.

To also serve the Control Plane and the browser console:

```sh
OCG_DATA_DIR=/absolute/path/to/ocg-data \
OCG_CONTROL_PORT=7840 \
OCG_CONTROL_TOKEN='replace-with-a-random-32-or-more-character-token' \
OCG_UI_DIR=/absolute/path/to/frontend/out \
./target/release/ocg
```

The Control Plane binds `127.0.0.1:<OCG_CONTROL_PORT>` and prints
`OCG control plane listening on http://127.0.0.1:<port>`.

### Environment variables

| Variable | Required | What it does |
| --- | --- | --- |
| `OCG_DATA_DIR` | Yes | Root of OCG's data directory, for the host and every command. The directory and its subdirectories are created when missing. |
| `OCG_CONTROL_PORT` | No | The loopback TCP port (1–65535) the Control Plane listens on. It must be set together with `OCG_CONTROL_TOKEN`; setting only one is a startup error. |
| `OCG_CONTROL_TOKEN` | No | The bearer token programmatic Control Plane clients present. It must be 32–512 characters with at least 8 distinct characters, using `A-Z`, `a-z`, `0-9`, `-`, `.`, `_`, `~`, `+`, `/` or `=`. It is never logged. |
| `OCG_UI_DIR` | No | A directory of the exported frontend for the Control Plane to serve. It must exist, is read only when the Control Plane is enabled, and without it no console or browser pairing is offered. |
| `OCG_PROJECT` | `workspace-*` | The Project, written `project:<n>`, whose root is the Workspace the Job may use; see [Projects](#projects). It must exist, and its root must still pass the checks below. |
| `OCG_PROVIDER_ENDPOINT` | `register-llm` and `workspace-*` | The Provider's OpenAI-compatible endpoint, which must be an `https://` URL. |
| `OCG_PROVIDER_MODEL` | `register-llm` and `workspace-*` | The Provider's model identifier. |
| `OCG_PROVIDER_CREDENTIAL_REF` | `register-llm` and `workspace-*` | The name of the environment variable that holds the Provider's API key. That variable is read when a request is built; its value is never stored, printed or logged. |

When a Profile's API key is not in the data directory's credential store, OCG
reads the environment variable named by that Profile's `credential_ref`.

Chat tools need an Agent with Workspace access and an LLM Profile that declares
Tools, and every Job of such an Agent names a Project. Tools can reach only that
Project's root. `OCG_WORKSPACE_ROOT` is no longer read: when it is still set,
the host logs one warning that it is ignored.

## Projects

A Project names a directory, its root, and the commands an Agent may run there.
Create one with the Control Plane (bearer token shown; a signed-in browser also
sends its CSRF token):

```sh
curl -X POST http://127.0.0.1:7840/api/v1/projects \
  -H "Authorization: Bearer $OCG_CONTROL_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"name": "ocg", "root": "/absolute/path/to/ocg",
       "commands": [{"name": "lint", "argv": ["cargo", "clippy", "--all-targets"]}]}'
```

- The root is resolved when the Project is created and never changes. It must
  be an absolute path to an existing directory with a UTF-8 path, not `/` or a
  directory that contains your home directory, and apart from the data
  directory. It may not equal, contain or lie inside the root of another
  Project. Every Job re-checks the root when it starts and fails, running
  nothing, when the root no longer authorizes or resolves anywhere else.
- Known limit: on macOS, firmlinked aliases of a directory
  (`/System/Volumes/Data/...`) are not detected as overlapping roots.
- A command is a name matching `[a-z][a-z0-9_]{0,63}` (not `cargo_check` or
  `cargo_test`, which every `code` grant already has) and an `argv` of 1 to 64
  entries, each 1 to 4 KiB without NUL. `argv[0]` is a bare program name, found
  on OCG's own absolute `PATH` entries, or an absolute path. A Project has at
  most 32 commands, and their encoding takes at most 16 KiB. Commands run in the
  root with the same filtered environment and time limit as every other
  command, never through a shell.
- **The argv of every command is sent to the Provider with each request, so put
  no secret in it.**
- `POST /api/v1/projects/{project}` replaces the name, description and commands;
  a body that names `root` is refused. `GET /api/v1/projects` lists Projects
  newest first with `before=project:<n>` and `limit`. There is no delete.
- A Job names its Project when it is submitted (`"project": "project:<n>"`); a
  conversation fixes its Project at creation. Follow-up messages name the
  conversation and omit `project`.

Pools and conversations have routes too: `GET /api/v1/pools` lists every Pool,
and `POST /api/v1/pools/{execution_pool}` sets `{"concurrency_limit": n}` or
`{"enabled": bool}` (lowering the limit stops new claims; running Jobs
continue); `POST /api/v1/conversations/{job}` sends `{"title": "..."}` or
`{"hidden": true}`, and hiding is permanent.

## Conversations and turns

A conversation is a long-lived parent that owns its Agent, Project and child
turns. `POST /api/v1/jobs` opens a conversation with its first user message;
its admission reply names `conversation`, `job`, `seq`, `joined` and the frozen
`model`. Send another message with `"conversation": "job:N"`. Messages can be
sent while an answer runs: the first creates a waiting turn, and more messages
join that turn until it is claimed. A joined message uses that turn's frozen
model. The Agent stays fixed; an omitted Model uses the tip's Model.

`GET /api/v1/conversations` lists visible root conversations by newest tip,
with `state` (`open`, `closing`, `settled`), `on_hold` and `project`.
`GET /api/v1/conversations/job:N/turns?limit=20&before=job:M` pages turns newest
first (maximum 100), including each input entry, outputs and uncertain calls.
`GET /api/v1/conversations/job:N/notices?after_seq=0&limit=20` reads notice rows.

Send `{}` to `POST /api/v1/conversations/job:N/stop` to stop the current answer.
Queued messages remain in the chain for the next turn to read; Stop leaves the
conversation open. Send `{}` to `/close` to close its whole subtree: it accepts
no new turns and settles once every running execution is confirmed stopped.
An idle conversation settles in the Close transaction. `/hold` prevents new
claims in the subtree; `/unhold` places eligible waiting turns again. Hold does
not stop an answer already running. `/retry` accepts `{"turn":"job:M"}` and
appends a retry reference; started calls with uncertain outcomes are disclosed
and never automatically replayed. System conversations are managed in Settings.

Unclaimed stored turns survive clean shutdown and crashes. `ocg serve` claims
them after restart using their frozen configuration and their Project's current
commands. A running turn abandoned by a crash becomes Unknown; when its stop
cannot be proven, its quarantined slot stalls the conversation until the stop
is confirmed. CLI process turns belong to their admitting process and are
cancelled if reopened without a claim. The stored format is 20, with no migration;
recreate data directories made by earlier versions.

## Sign a browser in

1. Start the host with `OCG_CONTROL_PORT`, `OCG_CONTROL_TOKEN` and
   `OCG_UI_DIR` set.
2. Read the pairing code the host prints to stderr:
   `OCG local browser pairing code: <code>`.
3. Open `http://127.0.0.1:<OCG_CONTROL_PORT>` in a browser on the same machine
   and enter that code on the sign-in form.

The code is generated fresh for each run, is not the bearer token, and stops
accepting sign-ins after 10 failed attempts. A browser session lasts eight
hours, only one session exists at a time, and it ends with the OCG process.

## API routes

- `GET /api/v1/health` — report host status, database readiness, vitals (database commits/timings, claims/timings, queue counts, process memory) and quarantined executions.
- `GET /api/v1/tool-stats` — report aggregated tool execution statistics per tool name: `name`, `admitted`, `refused_arguments`, `completed`, `failed`, `open`, `duration_us_mean` and `duration_us_max`.
- `GET /api/v1/usage/summary` — report total token usage and request count, optionally filtered by `from_us`, `until_us` or `agent` (a `profile:N` id). There is no model filter. Each row names its `agent` as `profile:N` or `null`, and its `provider` as `llm_provider:N`.
- `POST /api/v1/provider-models` — test a Provider connection and list its models. Uses the same authentication as `/api/v1/profiles` (browser session with CSRF, or bearer token). Send `{endpoint, credential?}` or `{endpoint, credential_ref?}` to use a saved credential. Returns `{models: [{id, context_window}]}` sorted by id, with `null` for unknown context windows. Refusals are `provider_unreachable`, `provider_rejected` (with `status_code` and the Provider's message), or `models_not_listed`; the latter allows manual model entry. HTTPS only, no redirects, 30 s timeout and a 1 MiB response limit. Credentials are never saved or returned.

## Commands

Each command below also needs `OCG_DATA_DIR`, except `--version` and `help`.

- `ocg --version`, `ocg -V` — print the OCG version and exit.
- `ocg help`, `ocg --help`, `ocg -h` — print command-line usage and exit.

### Pools

- `ocg pool list` — print every Pool as `id`, name, concurrency limit and `enabled`/`disabled`.
- `ocg pool create <name> [nonzero-concurrency]` — create an enabled Pool for OpenAI Chat Completions (default concurrency 16) and print its id.
- `ocg pool enable <pool>` — enable the Pool named `execution_pool:<n>` or `<n>`.
- `ocg pool disable <pool>` — disable the Pool named `execution_pool:<n>` or `<n>`.
- `ocg pool set-limit <pool> <nonzero-concurrency>` — replace the Pool's concurrency limit.

### Workspace Jobs

These run one Job through the host and print what was recorded for it. They
also read `OCG_PROJECT` and the three `OCG_PROVIDER_*` variables. The Project
must already exist, and its root must authorize before anything is recorded.

- `ocg workspace-read <objective>` — run one Job with only `workspace_read_file`.
- `ocg workspace-edit <objective>` — also allow creating and conditionally updating files.
- `ocg workspace-code <objective>` — also allow build and test commands, the Project's commands, and directory listing/search.
- `ocg workspace-vision <image-path> <objective>` — store one PNG or JPEG from the Workspace as an Attachment and send it with the objective, offering no tool.

`workspace-edit` and `workspace-code` are built for Linux, Android and Apple
platforms; the other two are available on Unix.

### LLM target

- `ocg register-llm` — find or register the Control Plane's chat Pool, Connection, LLM Profile and Agent Profile from `OCG_PROVIDER_ENDPOINT`, `OCG_PROVIDER_MODEL` and `OCG_PROVIDER_CREDENTIAL_REF`, then print their identities as JSON.

### Slot recovery

- `ocg release-slot <job> [--yes]` — confirm that a quarantined Job's processes have terminated and release its held slot.

## Data and logs

Everything is kept under `OCG_DATA_DIR`:

```text
$OCG_DATA_DIR/
├── ocg_db/            redb database, system_policy.json and credentials.json
├── ocg_attachments/   Attachment content
├── ocg_logs/          runtime logs
└── ocg.key            reserved key path (not created in this release)
```

Runtime log files are named `YYYYMMDDHHMMSSZ_runtime_NNN.log`. The default
policy logs at `info` level, rotates at 10 MiB and keeps logs for 7 days.
