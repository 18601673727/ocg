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
  follow-up to a conversation takes its previous turn's and may not name
  another.

Pools and conversations have routes too: `GET /api/v1/pools` lists every Pool,
and `POST /api/v1/pools/{execution_pool}` sets `{"concurrency_limit": n}` or
`{"enabled": bool}` (lowering the limit stops new claims; running Jobs
continue); `POST /api/v1/conversations/{job}` sends `{"title": "..."}` or
`{"hidden": true}`, and hiding is permanent.

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

- `POST /api/v1/provider-models` — test a Provider connection and list its models. Uses the same authentication as `/api/v1/profiles` (browser session with CSRF, or bearer token). Send `{endpoint, credential?}` or `{endpoint, credential_ref?}` to use a saved credential. Returns `{models: [{id, context_window}]}` sorted by id, with `null` for unknown context windows. Refusals are `provider_unreachable`, `provider_rejected` (with `status_code` and the Provider's message), or `models_not_listed`; the latter allows manual model entry. HTTPS only, no redirects, 30 s timeout and a 1 MiB response limit. Credentials are never saved or returned.

## Commands

Each command below also needs `OCG_DATA_DIR`.

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
