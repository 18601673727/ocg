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

Workspace tools need an Agent with Workspace access and an LLM Profile that declares
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
cancelled if reopened without a claim. The stored format is 24, with no migration;
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
- `GET /api/v1/usage/summary` — report total token usage and request count, optionally filtered by `from_us`, `until_us` or `agent` (a `profile:N` id). There is no model filter. Each row names its `agent` as `profile:N` or `null`, and its `provider` as `llm_provider:N`. Optional `bucket=day|week` (weeks start Monday) requires both bounds, rounds them out to whole buckets and allows at most 400 buckets. `tz_offset_minutes` is local minus UTC, defaults to 0 and accepts whole hours from -840 to 840; the client rounds half-hour zones to whole hours and ignores DST. Bucketed rows add `bucket_us`, the bucket start in UTC microseconds. Paging stops only at bucket boundaries; continue with `from_us=resume_from_us` and the original `until_us`.
- `POST /api/v1/provider-models` — test a Provider connection and list its models. Uses the same authentication as `/api/v1/profiles` (browser session with CSRF, or bearer token). Send `{endpoint, credential?}` or `{endpoint, credential_ref?}` to use a saved credential. Returns `{models: [{id, context_window}]}` sorted by id, with `null` for unknown context windows. Refusals are `provider_unreachable`, `provider_rejected` (with `status_code` and the Provider's message), or `models_not_listed`; the latter allows manual model entry. HTTPS only, no redirects, 30 s timeout and a 1 MiB response limit. Credentials are never saved or returned.

## External Job API

Use `Authorization: Bearer <token>` for every request. Upload each image with
`POST /api/v1/attachments` using `{mime, data_base64, display_name?}`, then submit
`POST /api/v1/jobs` with an `Idempotency-Key` of 16–128 characters. The JSON body
names `executor` and `objective`, image `attachments` by id, and
`agent_profile`/`llm_profile`. For a new conversation, omitting `llm_profile`
uses the named Agent's `default_model`.

Optional `response_format` accepts `{"type":"json_object"}` or
`{"type":"json_schema","json_schema":{"name":"answer","schema":{},"strict":true}}`;
`strict` is optional and the name is 1–64 letters, digits, underscores or hyphens.
The format applies to its own turn only; model text is returned verbatim in
`result.value.text`. Optional `keep_open` defaults to true; set it to false for
a one-shot conversation. A follow-up names `conversation` and cannot set
`keep_open`. A closed one-shot cannot be retried: submit again with a new key.

After `202`, or if the reply is lost, repeat the same submission with the same
key or send `GET /api/v1/admissions` with the same `Idempotency-Key` header. Poll
`GET /api/v1/jobs/{job}` until `job.settlement.state` is `completed`, `failed`,
`cancelled` or `unknown`, then read the envelope's `result`, `usage` and `unknown_reason`.
`result` is the newest result output or null; `usage` is null for a conversation
id or a Job without request events. `unknown_reason` is the failure message of
the Job's latest request, cut to 2048 bytes, when the settlement is `unknown`,
else null. Usage sums requests and complete requests,
input/output, cache-read/cache-write and reasoning tokens; `truncated` reports
the scan limit. Failed results have a machine-readable `code`:
`provider_rejected`, `not_delivered`, `build_failed`, `context_window_exceeded`,
`not_sent`, `tool_loop_stopped`, `completion_refused` or `internal`.
Provider rejections expose `status_code`, `provider_code`, `request_id` and
`retryable` in `result.value`, without the provider response body.

OCG binds only `127.0.0.1` and accepts Host only as `127.0.0.1:PORT` or
`localhost:PORT`. A reverse proxy must terminate TLS, set Host to
`127.0.0.1:$OCG_CONTROL_PORT`, remove `Origin` and `Sec-Fetch-Site`, allow request
bodies of at least 6 MiB for image uploads, avoid buffering `/events`, and limit
who may connect. The bearer token travels only over TLS outside loopback.

Images are PNG, JPEG, GIF or WEBP checked by signature: at most 4 MiB each,
8 images and 16 MiB per provider request. The newest earlier images that fit are
sent; a system notice discloses omitted images, and every image remains in stored
history. Submit bodies are at most 64 KiB and objectives at most 32 KiB.

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

## Agent dispatch and recurring conversations

An Agent may name allowed Agent Profiles in `dispatch_to` (sorted, unique
`profile:N` ids, including itself). Each target needs a `default_model` selected
from its `llm_targets`. Both fields are accepted and returned by `/api/v1/profiles`.
Stored turns offer `job_dispatch`, `job_message`, `job_stop`, `job_close`,
`job_hold`, `job_unhold` and `job_retry` when their frozen Agent has dispatch
permission. Dispatch needs model Tools support; it does not require a Workspace
grant. An agent may manage its strict subtree when the target subtree has no
queued or running user turns, and cannot lift a user or loop hold. Effects commit
atomically with their tool outcomes; restart reconciles an admitted host call without an outcome
as NotApplied.

`job_dispatch` accepts `agent`, `objective`, optional `project`, `keep_open`,
`every_seconds` (60 through 2,592,000), and `depends_on` entries containing a sibling
`conversation` and `condition` (`settled` or `success`). Ordinary children close
automatically after completion unless kept open or joined by a user. Recurrences
stay open, continue after failed iterations, and use the objective plus the
previous iteration's result as context. Close, a removed model assignment, or a
refused iteration halts a recurrence. Hold pauses future turns until Unhold.

Eight consecutive attention-woken turns that successfully act on children put
the parent on a loop hold; children keep running. A user message or Unhold resets
the count, and a user message lifts a loop hold. Reporting without child actions
does not consume this bound.

`GET /api/v1/conversations/{job:N}/children?before=job:N&limit=20` pages child
conversations, including settled children, with title, Agent, state, hold,
keep-open and recurrence details. The chat's child strip opens a child in the
same chat view and provides Stop, Close and Hold/Unhold.

`GET /api/v1/jobs/{job:N}/tree?after=job:M&limit=50&depth=2` reads the tree of
the work below a Job: its ancestors, and its children in creation order, each
with the state, Agent, model and dependencies the rows that own them hold. The
root's own children are one page of at most 100; every node deeper than that
lists at most 20 children, one answer holds at most 300 nodes, and the tree
expands at most four levels. A node whose `children` is null has children the
answer did not list: ask for the tree rooted at that node to read them.

Agents also accept `approve: ["commands", "writes", "dispatch", "close"]` on
`POST /api/v1/profiles`. It defaults to empty; an omitted field on update keeps
the policy, and `[]` clears it. These classes gate `workspace_run_command`,
`workspace_write_file`, `job_dispatch` and `job_close`, respectively. A conversation
needs an origin-user ancestor, itself included, to open with this policy.

A gated call is recorded without running. Other calls in its response run, then
the turn completes with the pending requests in its result. Child conversations
with a deciding user and a Model that supports Tools also offer
`ask_user {"question":"..."}`. Questions complete the turn in the same way.

`GET /api/v1/conversations/{job:N}/cards` reads undecided approvals, questions,
loop holds and quarantines throughout the live subtree. Each card names its
conversation and parent; the chat nests cards under their parent conversation.
System conversations show only quarantine cards.

`POST /api/v1/approvals/{job:N}/{request}/{index}` takes
`{"decision":"approve"}`, `{"decision":"reject"}`, or, for a question,
`{"answer":"..."}`. A decision commits once and appends one Decision entry.
The next claimed turn executes approved calls once under a host-made request 1,
rechecking the current registry and grant; its first provider request is 2.
Closing voids undecided calls. Approved calls that never ran appear in the final
conversation result, and an unresolved approved execution blocks Retry.

`POST /api/v1/jobs/{job:N}/release-slot` takes `{}` and first tries the host's
process-group proof. A live group refuses with `slot_live`. When no group is
tracked, `confirm_required` asks for `{"confirm":true}`, which records user stop
confirmation. Successful replies are `released` or `already_released`. This route
is available for any origin; the CLI continues to record operator confirmation.
