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
| `OCG_PROJECT` | `workspace-*` | The Project, identified by its canonical UUID, whose root is the Workspace the Job may use; see [Projects](#projects). It must exist, and its root must still pass the checks below. |
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
  newest first with `before=<project-uuid>` and `limit`. There is no delete.
- A Job names its Project when it is submitted (`"project": "<project-uuid>"`); a
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
`model`. Send another message with `"conversation": "<job-uuid>"`. Messages can be
sent while an answer runs: the first creates a waiting turn, and more messages
join that turn until it is claimed. A joined message uses that turn's frozen
model. The Agent stays fixed; an omitted Model uses the tip's Model.

`GET /api/v1/conversations` lists visible root conversations by newest tip,
with `state` (`open`, `closing`, `settled`), `on_hold` and `project`.
`GET /api/v1/conversations/<job-uuid>/turns?limit=20&before=<cursor-uuid>` pages turns newest
first (maximum 100), including each input entry, outputs and uncertain calls.
`GET /api/v1/conversations/<job-uuid>/notices?after_seq=0&limit=20` reads notice rows.

Send `{}` to `POST /api/v1/conversations/<job-uuid>/stop` to stop the current answer.
Queued messages remain in the chain for the next turn to read; Stop leaves the
conversation open. Send `{}` to `/close` to close its whole subtree: it accepts
no new turns and settles once every running execution is confirmed stopped.
An idle conversation settles in the Close transaction. `/hold` prevents new
claims in the subtree; `/unhold` places eligible waiting turns again. Hold does
not stop an answer already running. `/retry` accepts `{"turn":"<turn-uuid>"}` and
appends a retry reference; started calls with uncertain outcomes are disclosed
and never automatically replayed. System conversations are managed in Settings.

Unclaimed stored turns survive clean shutdown and crashes. `ocg serve` claims
them after restart using their frozen configuration and their Project's current
commands. A running turn abandoned by a crash becomes Unknown; when its stop
cannot be proven, its quarantined slot stalls the conversation until the stop
is confirmed. CLI process turns belong to their admitting process and are
cancelled if reopened without a claim. The stored format is 31, with no migration;
older or newer directories are refused before writing and left intact. Use a
separate fresh directory for this build.

Conversation depth is derived from parent links. Limits default to depth 3
(root depth 0), 8 automatic notice-driven follow-up turns, and 100 scheduled
iterations per recurring conversation. Conversation Inspector controls and
`GET /api/v1/conversations/{job}/limits` expose effective limits; the existing
conversation edit endpoint accepts a `limits` object. Limits inherit from
ancestors. Users can raise them up to depth 16, 255 automatic turns and 10,000
iterations; agents can only tighten their own or descendant limits through
`job_limits`. Zero disables new work of that kind. Already-running work and
recorded history remain intact.

Agents with a nonempty `dispatch_to` allowlist can inspect permitted Agent
profiles through `job_agents` and their own direct workers through `job_workers`.
Both tools page at most 20 rows. `job_assign` atomically reuses an idle, open,
non-recurring child with an allowed profile; unrelated or user-managed
conversations are refused. `job_dispatch` creates a new child. Results use the
existing child-notice mechanism.

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
- `GET /api/v1/usage/summary` — report total token usage and request count, optionally filtered by `from_us`, `until_us` or `agent` (a `<profile-uuid>` id). There is no model filter. Each row names its `agent` as `<profile-uuid>` or `null`, and its `provider` as `<provider-uuid>`. Optional `bucket=day|week` (weeks start Monday) requires both bounds, rounds them out to whole buckets and allows at most 400 buckets. `tz_offset_minutes` is local minus UTC, defaults to 0 and accepts whole hours from -840 to 840; the client rounds half-hour zones to whole hours and ignores DST. Bucketed rows add `bucket_us`, the bucket start in UTC microseconds. Paging stops only at bucket boundaries; continue with `from_us=resume_from_us` and the original `until_us`.
- `POST /api/v1/profiles/import` — import 1–200 model selections atomically using either an existing `source_profile_id` or a new `provider: {name, endpoint, credential?, credential_ref?}`, plus `models: [{id, context_window?}]`, and `declared_capabilities`. Reuses matching Provider connections; matching models keep their existing settings. Returns `{profile_ids, created, existing}`. Settings fetches the Provider list for selection and keeps manual model creation/editing available.
- `DELETE /api/v1/profiles/{profile}` — archive an LLM Profile and remove it from Agent model choices/defaults in one transaction. Historical Jobs keep their frozen configuration; no history or credential is deleted. Agent Profiles cannot be deleted through this route.
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
- `ocg pool enable <pool>` — enable the Pool named by its canonical UUID.
- `ocg pool disable <pool>` — disable the Pool named by its canonical UUID.
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
`<profile-uuid>` ids, including itself). Each target needs a `default_model` selected
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

Consecutive notice-driven follow-up turns put the parent on a loop hold at its
effective automatic-turn limit (8 by default); children keep running. All such
turns count, including reporting without child actions. A user message or Unhold
resets the count, and a user message lifts a loop hold.

`GET /api/v1/conversations/{job}/children?before=<job-uuid>&limit=20` pages child
conversations, including settled children, with title, Agent, state, hold,
keep-open and recurrence details. The chat's child strip opens a child in the
same chat view and provides Stop, Close and Hold/Unhold.

`GET /api/v1/jobs/{job}/tree?after=<cursor-uuid>&limit=50&depth=2` reads the tree of
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

`GET /api/v1/conversations/{job}/cards` reads undecided approvals, questions,
loop holds and quarantines throughout the live subtree. Each card names its
conversation and parent; the chat nests cards under their parent conversation.
System conversations show only quarantine cards.

`POST /api/v1/approvals/{job}/{request}/{index}` takes
`{"decision":"approve"}`, `{"decision":"reject"}`, or, for a question,
`{"answer":"..."}`. A decision commits once and appends one Decision entry.
The next claimed turn executes approved calls once under a host-made request 1,
rechecking the current registry and grant; its first provider request is 2.
Closing voids undecided calls. Approved calls that never ran appear in the final
conversation result, and an unresolved approved execution blocks Retry.

`POST /api/v1/jobs/{job}/release-slot` takes `{}` and first tries the host's
process-group proof. A live group refuses with `slot_live`. When no group is
tracked, `confirm_required` asks for `{"confirm":true}`, which records user stop
confirmation. Successful replies are `released` or `already_released`. This route
is available for any origin; the CLI continues to record operator confirmation.

Conversation naming runs after the first successful Provider reply in an unnamed, open-ended user conversation whose first Job has no explicit name. A separate tool-free System Job uses a bounded excerpt and the original frozen Provider/model, with a 256-token output budget. Its JSON title result is applied only while the conversation remains unnamed; manual names win. Naming requests, outputs, usage and failures belong to that Job and never become chat turns or notices in the user conversation. One-shot API Jobs do not request titles. Naming respects conversation child limits and reuses the existing stored Job layout (format 31).


## OpenAI Responses providers

LLM profiles and new-provider imports accept `protocol: "openai_responses"`.
Select **OpenAI Responses** in Settings when the model requires that API (for
example, OpenCode Go Muse Spark). Use the provider's base URL, such as
`https://opencode.ai/inference/go/openai/v1`; OCG posts to `/responses`.
An endpoint already ending in `/responses` is also accepted. Existing profiles
continue to use Chat Completions unless their protocol is explicitly changed.
For environment-based registration, set `OCG_PROVIDER_PROTOCOL=openai_responses`;
omitting it retains `openai_chat_completions`.

Responses requests stream with `store: false` and include the complete input
history. Function calls and outputs, images, JSON response formats, reasoning
summaries and encrypted reasoning continuation are supported. Token limits map
to `max_output_tokens`; reasoning effort maps to `reasoning.effort`. Presence and
frequency penalties are unsupported and cause a request-build refusal. Native
Responses usage and output items are retained in provider history. A terminal
`response.completed` ends reading immediately; failed, incomplete or truncated
responses never authorize tool execution.

Responses adds protocol code 2 alongside the unchanged Chat Completions code 1.
Database format 31 includes indexed conversation search, Anthropic transport,
Decision Profiles and compaction policies. It also encodes the Decision Pool
operation, typed prediction bindings, explicit Shadow/Prepare policy and acceptance
pins, hourly preparation budgets, and chunk/reduction work with chain references.
A summarization Job references a sealed manifest stored once in the existing
content-addressed MESSAGES table; its final result references that same digest.
Format 30 and all older formats are refused without migration or deletion.

Decision Models and Decision Agents reuse config identities, versions and
archival. `POST /api/v1/profiles` accepts `kind: "decision_model"` and
`kind: "decision_agent"`; paged Profile responses include their respective
`decision_profiles` and `decision_agent_profiles` arrays. Decision Agents have no
tool, dispatch or approval authority. Only Input Only (`workspace_access: "none"`)
is currently executable; workspace readers are not implemented.
`POST /api/v1/decisions` submits typed work through ordinary Jobs and Pools
advertising the `decision` operation. A busy eligible Pool queues work. Optional
Agent `decision_assistance` is a Decision Agent reference and may be unavailable
without preventing generative work.

The native TypeSafe System One adapter supports Choice (up to 255 options),
BinaryProbability (Noul) and OrdinalScore (up to 10 ordered levels). It includes
Decision Agent instructions, validates complete distributions and records the
requested and reported model identities separately. A reported model name is
not assumed to identify a version. Choice and Score confidence use distinct
provider semantic identifiers; neither is a calibrated probability of correctness.
Noul is the probability of the specified binary proposition. Invalid provider
answers fail explicitly. Input limits use conservative UTF-8 byte upper bounds;
response-size and reported-usage limits are checked locally. The provider API
has no output-token-limit parameter, so these controls are not a remote spending
guarantee. Request/response evidence and usage remain on the ordinary Job request;
Decision metrics are not included in generative Provider/model charts.

Conversation compaction is independent of database/storage compaction. Set a
whole `compaction_policy` on an Agent Profile or through
`POST /api/v1/conversations/{id}`. Omission preserves the stored value; `null`
clears an override, and `{"mode":{"kind":"off"}}` explicitly disables automatic
compaction. Precedence is conversation, then current Agent Profile, then host
Off. Manual compaction requires no Decision Profile: send `{"model":"<profile UUID>"}`
with an Idempotency-Key to `POST /api/v1/conversations/{id}/compact`.
Repeated keys return the same producing Job. During execution it seals an
older settled prefix for a later turn, without changing a running request.

Automatic policy uses the foreground turn's frozen model window and output
reserve. The summarizer can be assigned separately; otherwise it uses the
foreground model. Estimates use UTF-8 bytes/3, framing and image allowances,
not a provider tokenizer. Let B be window minus output reserve and protected
instruction allowance, and H the larger of output reserve and the largest
estimated growth across the last eight settled turns. The initial, explicitly **uncalibrated** presets are:

| Preset | Trigger / target | Recent settled turns kept | Cooldown |
| :--- | :--- | :--- | :--- |
| Conservative | B-H / B-2H | 4 | 600s |
| Balanced | B-2H / B-3H | 2 | 300s |
| Aggressive | B-3H / B-4H | 1 | 120s |

Preset minimum source is 2H; minimum savings is H. Invalid or unknown budgets
do not schedule automatic work. Custom mode supplies explicit trigger, target,
output reserve, minimum source/savings, recent-turn count and cooldown; reserve
cannot reduce the frozen model's output budget. Default remains Off.
Automatic preparation is bounded to two outstanding chains and two concurrent
executions globally and never holds the waiting foreground turn's Pool slot.
A failed or unusable preparation does not repeat forever with the same submission
key. A source larger than the summarizer window uses at most seven UTF-8-safe
map chunks and one reduction, with ordinary Success dependencies. Only the final
reduction can be adopted. All stages reference one sealed manifest; partial
results are distinct typed fragments. The rendered source is bounded to 1.75 MiB
and each map slice to 256 KiB; source or reduction budgets beyond those bounds
are refused explicitly without truncation. Admission checks the whole chain's
child, subtree and queue room in one transaction.

Summarization Jobs receive the sealed source as untrusted data, including final
results, tool outcomes and provenance. Valid JSON results declare omitted details
and must meet target/savings bounds. Before claim, adoption checks source hashes
and contiguous ancestry, completion, current automatic-policy binding and
compatibility with appended messages. Original user/Agent inputs, approvals,
attachments and recent turns remain in effective context; covered assistant
answers are replaced by a generated summary labeled with its possible loss of
detail. Enabled compaction fails explicitly when this protected context cannot
fit. Off retains the existing fitter behavior. All original persisted records
remain recoverable. Preparation/use metadata is append-only and excluded from
prompt text, titles and search. An explicit null artifact selection restores
original context. No history is rewritten and no artifact table is added.

Predictive opt-in is a separate explicit eligible Decision Agent assignment,
independent of decision assistance. Shadow is the default: it records predictions
after foreground completion using counts and a deterministic growth forecast,
without conversation text. Optional experimental Prepare mode additionally
requires enabled automatic compaction, an exact Decision Model config version,
an exact provider-reported model identity, a confidence-semantics identifier, and
user-supplied minimum probability/confidence thresholds in millionths (1..1000000).
There are no default acceptance thresholds or calibration claims. A mutable
provider alias does not prove immutable weights or calibration. An uncertain,
unusable, missing or mismatched result never starts preparation.

An accepted prediction is still advisory. The same transaction rechecks the
current policy/activity binding, deterministic forecast, source savings, cooldown,
available executors and resource budgets before admitting ordinary generative
summarization Jobs. Prediction work retains one outstanding request per
conversation, a minimum 60-second interval, two queued/running Decision Jobs,
six predictions per minute, bounded scans and a configured token bound. All
speculative work together is capped at eight queued Jobs and one execution
globally; foreground Jobs have priority. New foreground work suppresses predictions
and active speculative summaries yield through the existing cancellation token.
Manual and ordinary automatic compaction remain independent.

Prepare mode requires positive per-conversation hourly preparation, request and
token limits as well as a per-preparation token limit. The chain anchor owns one
conservative reservation for all stages. Reservations survive crashes and are not
refunded for failure, missing usage or cancellation. Authoritative request usage
can raise the charge above the estimate; an overrun prevents further stages and
future admissions beyond the hourly budget. The reservation stays charged for
an hour after the latest recorded request activity, so delayed execution cannot
escape its budget by aging in the queue. Speculative requests are not retried
automatically. These estimates and provider-reported token facts do not guarantee
remote monetary charges. Any provider billing cap must be configured separately.

Policy/Profile/model changes, archival or opt-out stop queued preparation and
cancel active automatic work. Compatible new message appends retain the sealed
prefix; adoption waits for the next claim and revalidates it. Stopping a summary
System conversation stops its pending chain, returns running stages for ordinary
cancellation, and discards the artifact for future adoption. Existing claimed-turn
context selections remain stable. Deterministic submission keys deduplicate
promotion; bounded claim-time recovery can promote a still-current completed
prediction after restart. Inconclusive recovery scans safely skip speculation.
Historical acceptance settings are referenced by conversation/Profile revision;
an edited binding cannot be replayed or promoted against newer settings.

`GET /api/v1/conversations/{id}/context?before=<child UUID>&limit=50` returns
effective policy and its source, capacity/estimate provenance, predictor and
summarizer identities, last context use and bounded pages of summary/prediction
work, including chunk/reduction stages, prediction links and reservations.
It distinguishes queued, preparing, part_ready, prepared, adopted, discarded,
incompatible, failed, cancelled and recorded prediction states; `next_before`
can advance over an empty scan page. The existing conversation stop route can
cancel typed summary or decision System children. General System/title work
remains protected. Settings provides Decision Model/Profile forms, optional
Agent assistance and compaction settings. The conversation inspector offers
manual compaction, effective policy overrides, status and stop/discard controls.
The manual Retry action keeps its Idempotency-Key after a lost response.

Offline scripted tests cover accepted/uncertain/over-budget predictions,
restart promotion, active opt-out/foreground cancellation, chunk reduction and
claim-boundary adoption/discard. They verify control behavior, not predictive
accuracy, probability calibration or summary retention quality. Provider-specific
quality evaluation with representative conversations remains necessary before
choosing production presets or acceptance thresholds.

## Conversation full-text search

`GET /api/v1/conversations/search?query=...&limit=50` searches titles, all
user/agent messages and saved assistant results across projects in the current
account. Hidden conversations stay excluded. Matching is case-insensitive literal
substring matching; whitespace-separated terms are combined with AND across the
conversation. Double quotes keep a phrase together, for example
`query=%22connection+reset%22+proxy`. Queries take at most 500 UTF-8 bytes and ten
unique terms or phrases, each at most 50 Unicode characters. Unclosed quotes
are refused. `next_before` resumes the next page without skipping a result;
The sidebar search exposes additional pages with **Load more search results**.

A derived Unicode gram index points to immutable message and result rows. It is
updated in the same transaction as its source and rebuilt from source rows after
an unclean shutdown. Search verifies candidate text against those rows, with no
100-turn history cutoff. Each page still scans at most 1,000 recent conversations;
a short or empty page with `has_more: true` must be followed using its cursor.

## Anthropic Messages providers

Select **Anthropic Messages** in Settings, or use
`protocol: "anthropic_messages"` when creating or importing an LLM profile.
Use `https://api.anthropic.com/v1` (or the full `/v1/messages` endpoint), a model
available to your account, and your API key. Environment registration accepts
`OCG_PROVIDER_PROTOCOL=anthropic_messages`. OCG uses the native Messages API with
`x-api-key` and the stable `anthropic-version: 2023-06-01` header. Credentials
remain sensitive headers and are redacted from provider failures.

Streaming text, images, tool calls/results, signed thinking continuation and
paginated native model discovery are supported. System messages become the
top-level `system` prompt. Images are sent as base64 content blocks, while request
history keeps attachment references. Anthropic input usage includes ordinary,
cache-read and cache-write input tokens; cache counters are also retained
separately. Only `message_stop` completes a stream; truncated responses never
run tools.

`max_tokens` or `max_completion_tokens` maps to `max_tokens`, defaulting to 4,096.
Setting both is refused. `json_schema` response formats map to
`output_config.format`; `json_object` is refused, so supply an explicit schema.
Presence/frequency penalties are refused. Reasoning effort enables adaptive
thinking and maps to `output_config.effort` (`minimal` maps to `low`, `none`
disables thinking); choose a model that supports these options. OCG does not
infer model support from names. Anthropic uses stored protocol code 3; the
protocol and search index are included in database format 31, without migration.
