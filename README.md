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
| `OCG_WORKSPACE_ROOT` | No for the host; yes for `workspace-*` | An absolute path to the Workspace Jobs may use. The host offers Workspace tools to Agents with a Workspace grant from this root; it cannot be your home directory, `/`, or overlap the data directory. |
| `OCG_PROVIDER_ENDPOINT` | `register-llm` and `workspace-*` | The Provider's OpenAI-compatible endpoint, which must be an `https://` URL. |
| `OCG_PROVIDER_MODEL` | `register-llm` and `workspace-*` | The Provider's model identifier. |
| `OCG_PROVIDER_CREDENTIAL_REF` | `register-llm` and `workspace-*` | The name of the environment variable that holds the Provider's API key. That variable is read when a request is built; its value is never stored, printed or logged. |

When a Profile's API key is not in the data directory's credential store, OCG
reads the environment variable named by that Profile's `credential_ref`.

Chat tools need an Agent with Workspace access and an LLM Profile that declares
Tools. Tools can reach only `OCG_WORKSPACE_ROOT`.

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
also read `OCG_WORKSPACE_ROOT` and the three `OCG_PROVIDER_*` variables.

- `ocg workspace-read <objective>` — run one Job with only `workspace_read_file`.
- `ocg workspace-edit <objective>` — also allow creating and conditionally updating files.
- `ocg workspace-code <objective>` — also allow build and test commands and directory listing/search.
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
