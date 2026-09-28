# Phase 2A: OCG backend adapter contract

Status: proposed contract. This document does not implement an HTTP client,
server route, SSE stream, authentication, or persistence change.

The contract is intentionally owned by OCG. OpenCode is one possible executor
behind OCG; its HTTP routes and event names are not browser protocol.

## 1. Findings from the existing repositories

### 1.1 What `ocg` owns today

The existing OCG repository is a Rust CLI and library, not a
browser-facing application server. Its current boundaries are:

- `src/runtime/lifecycle.rs` defines a runtime-neutral lifecycle seam for
  resolving, creating, recovering, inspecting, and preparing an opaque runtime
  execution, observing context, and staging/resuming a continuation.
- `src/runtime/compat/v2_client.rs` is a concrete OpenCode 2 adapter. It uses a
  private, invocation-scoped OpenCode server and currently knows these OpenCode
  routes:

  ```text
  GET  /api/config
  POST /api/session
  GET  /api/session?directory=...&parentID=null&order=desc&limit=...
  GET  /api/session/{id}
  GET  /api/session/{id}/context
  POST /api/session/{id}/agent
  POST /api/session/{id}/model
  GET  /api/model?directory=...
  POST /api/session/{id}/synthetic
  ```

- `src/runtime/compat/v2_server.rs` starts `opencode serve` on loopback for
  one invocation, reads a child-only URL/password handshake, authenticates a
  readiness request, and terminates the child on drop. The URL and password
  are invocation secrets, not browser configuration.
- `src/orchestration/bridge.rs` is a local `ocg __bridge` command. It receives
  plugin observations over stdin and returns bounded JSON to the generated
  plugin. It is not an HTTP API.
- `src/orchestration/plugin.rs` is a thin OpenCode plugin adapter. OpenCode 2
  exposes a global in-process event subscription to the plugin. The observed
  envelope is OpenCode-owned (`{id, created, type, data, ...}`), and observed
  event types include `session.step.started`, `session.text.delta`,
  `session.text.ended`, `session.step.ended`, and interruption/failure events.
  OCG uses that stream only to capture a completed root Lead output and to
  submit a context observation. It does not expose that stream to a browser or
  persist a replay log.
- `src/orchestration/state.rs` stores bounded session orchestration state in
  local ignored JSON. It retains up to 16 session records and is fail-soft.
- `src/orchestration/mission.rs` stores durable Missions in separate versioned
  JSON files. A Mission has a deterministic identity, revision, generation,
  task/goal/constraints, lifecycle status, orchestration phase, attempts,
  execution binding, bounded findings/failures/evidence/checkpoints, history,
  reconcile state, and budget accounting.
- `src/orchestration/policy.rs` stores generation-bound approval records. The
  current durable approval lifecycle is only `pending`, `approved`, or
  `rejected`; an approval is bound to `(mission_id, generation, action,
  current_execution_id)`.
- `ocg approvals`, `ocg approve`, and `ocg reject` are CLI operations. There is
  no OCG-owned HTTP approval API.
- Mission budget accounting uses integer micro-units, one currency, durable
  reservations, settled/reserved/unresolved amounts, and explicit statuses. It
  does not use floating-point money.

### 1.2 Capabilities that do not exist yet

The repositories do **not** currently provide:

- an OCG browser server or `/api/ocg/...` route family;
- an OCG-owned session list/detail DTO;
- an OCG-owned message-history DTO;
- a send-message command owned by OCG;
- a browser-safe cancel command;
- an OCG-owned durable event journal with cursor replay;
- an SSE endpoint;
- a normalized tool/activity event protocol;
- durable worker records or a scheduler/resource broker;
- an HTTP approval response endpoint;
- a browser authentication boundary.

These are backend gaps, not reasons to expose the existing OpenCode routes or
plugin payloads directly.

### 1.3 Frontend requirements

The current frontend abstraction is in
`components/ocg/runtime/runtime-types.ts` and currently supports:

- runtime status;
- session listing/detail;
- message history;
- Mission lookup;
- session creation;
- sending a message;
- normalized event subscription;
- local snapshot access for React's external-store subscription;
- optional cancellation by session.

The current domain model in `components/ocg/types.ts` is a presentation model,
not a wire schema. In particular, its `Mission` has display-oriented progress,
workers, resource commitment, and formatted budget values that are not all
authoritative fields in the current Rust Mission record.

## 2. Contract shape

The future browser path should be:

```text
React components
    ↓ frontend domain types
HttpOcgRuntimeClient
    ↓ OCG wire DTOs and normalized events
OCG-owned HTTP + event service
    ↓ OCG runtime-neutral controller/lifecycle boundary
OpenCode adapter or another executor
```

The browser never receives OpenCode credentials, OpenCode event objects, raw
executor payloads, or local filesystem paths.

### 2.1 Protocol namespace and version

Use a versioned OCG namespace:

```text
/api/ocg/v1/...
```

Every JSON response and event envelope also carries:

```json
{
  "protocolVersion": "1"
}
```

The client sends `Accept: application/vnd.ocg.v1+json` and may send
`X-OCG-Protocol-Version: 1`. A major version mismatch is a protocol error; the
client must enter a failed/incompatible state and must not interpret the
payload optimistically. Additive fields and event types are allowed within
major version 1. Unknown event types are ignored after their envelope is
validated, with a diagnostic retained for observability.

`schemaVersion` is separate from `protocolVersion` when a durable projection
has its own migration lifecycle, for example the Mission projection.

## 3. Minimum HTTP operations

These are the smallest operations justified by the existing UI and
`OcgRuntimeClient`. They are not generic CRUD.

| Operation | Proposed route | Frontend use case | Result |
| --- | --- | --- | --- |
| Runtime status | `GET /api/ocg/v1/runtime` | Topbar/sidebar connection state and capability diagnostics | OCG runtime status snapshot |
| Atomic bootstrap | `GET /api/ocg/v1/snapshot?sessionId=...` | First load and safe reconnect | Runtime, sessions, selected session state, and an event cursor from one read boundary |
| List sessions | `GET /api/ocg/v1/sessions` | Sidebar grouping and selection | OCG session summaries, newest first, bounded/paginated if needed |
| Session detail | `GET /api/ocg/v1/sessions/{sessionId}` | Topbar/session selection | One session summary and authoritative revision metadata |
| Message history | `GET /api/ocg/v1/sessions/{sessionId}/messages` | ChatView history and resync | Ordered normalized message records |
| Mission snapshot | `GET /api/ocg/v1/sessions/{sessionId}/mission` | MissionView and resync | Mission projection or explicit `null` |
| Create session | `POST /api/ocg/v1/sessions` | New chat | OCG session identity and initial revision |
| Send message | `POST /api/ocg/v1/sessions/{sessionId}/messages` | Composer submission | Accepted command identity and initial message/turn identity |
| Cancel message work | `POST /api/ocg/v1/sessions/{sessionId}/active-work/cancel` | Stop the current assistant turn | Accepted command identity; the later authoritative cancelled event confirms state |
| Approval response | `POST /api/ocg/v1/approvals/{approvalId}/decision` | Future approval controls | Idempotent approval decision result and/or approval event |
| Event stream | `GET /api/ocg/v1/events?after=...` | Live updates and replay | OCG-owned SSE events, ordered by the OCG stream cursor |

The snapshot route is the important bootstrap operation. The individual reads
remain useful for lazy session selection and targeted repair, but they must not
be used as a substitute for the snapshot cursor handshake.

### 3.1 Command request rules

Commands carry a client-generated `commandId` (UUID or equivalent) and the
resource revision/generation when a command changes durable state:

```json
{
  "protocolVersion": "1",
  "commandId": "cmd_01...",
  "expected": {
    "sessionRevision": 12,
    "missionId": "mission_...",
    "generation": 3
  },
  "text": "Please inspect the failing build"
}
```

The exact request fields are operation-specific. `commandId` is not a message
ID and must not be used as one. Repeating the same command with the same
`commandId` returns the original command result instead of creating a second
side effect. A new command ID means a new command attempt.

The server chooses the project/workspace. A browser must not send an arbitrary
filesystem directory to the underlying OpenCode `/api/session` route.

## 4. Snapshot and event-stream semantics

### 4.1 Stream identity and ordering

The proposed OCG stream is one logical stream per OCG project/runtime scope,
not one stream per browser and not one stream per session:

```json
{
  "streamId": "project_<opaque-id>",
  "sequence": 1842
}
```

Every committed event receives a strictly monotonically increasing `sequence`
within that stream. The pair `(streamId, sequence)` is the resume cursor. The
event ID is also stable and unique, but the sequence is the ordering and gap
detection primitive.

Global per-stream ordering is preferred over independent per-session counters:

- it gives a snapshot one unambiguous high-watermark;
- it orders Mission, session, message, activity, and runtime changes together;
- the client can detect a missing event while switching sessions;
- the event payload still carries optional `sessionId` and `missionId` scope.

The backend must commit the state mutation and append the corresponding event
under one durable ordering boundary. An event must never be published before
the state it describes is authoritative.

### 4.2 Snapshot response

The snapshot response is conceptually:

```json
{
  "protocolVersion": "1",
  "snapshot": {
    "runtime": { "state": "connected", "detail": null },
    "sessions": [],
    "selectedSession": {
      "session": null,
      "messages": [],
      "mission": null
    }
  },
  "cursor": {
    "streamId": "project_<opaque-id>",
    "sequence": 1842
  }
}
```

The `cursor` is the event high-watermark corresponding to the exact snapshot.
It is not the time the HTTP response was sent and not an SSE `Last-Event-ID`
accidentally inferred by the browser.

The backend must guarantee:

1. all events at or below the returned cursor are reflected in the snapshot;
2. every committed event after that cursor can be replayed or causes an
   explicit cursor-expired response;
3. the cursor and snapshot are produced from one consistent read boundary.

If the current file-backed Mission/session stores cannot provide that boundary,
the future OCG server needs a projection/read model or a short lock/transaction
around snapshot creation. The browser must not try to assemble an authoritative
snapshot from several independent HTTP requests.

### 4.3 Initial connect and reconnect

The safe client algorithm is:

```text
GET snapshot
  └─ install authoritative state and cursor C
GET events?streamId=C.streamId&after=C.sequence
  └─ apply events with sequence > C.sequence
  └─ continue the live subscription
```

A reconnect uses the last successfully applied cursor instead of fetching
history first:

```text
GET events?streamId=S&after=N
```

The server must replay events strictly after `N` before delivering new live
events. The client may also send `Last-Event-ID`, but correctness must not
depend on SSE itself retaining or replaying anything.

### 4.4 Replay window and expired cursors

The stream is at-least-once and bounded. The proposed initial retention policy
is the latest **10,000 events or 24 hours, whichever is smaller**, with the
actual limits returned by the runtime capability/status response. This is a
contract default to be confirmed by OCG core, not an existing OCG capability.

If `after` is older than the retained window, the server returns a structured
`cursor_expired` error (HTTP 410 is appropriate) containing:

```json
{
  "code": "cursor_expired",
  "streamId": "project_<opaque-id>",
  "oldestAvailableSequence": 9001,
  "snapshotRequired": true
}
```

The client then performs a fresh snapshot and resumes from its returned cursor.
It must not guess missing events or silently continue from the oldest retained
event.

### 4.5 Delivery, duplicates, and gaps

Delivery is **at least once**. Duplicate delivery is expected on reconnect and
must be harmless:

- deduplicate by `(streamId, eventId)` or sequence;
- apply entity updates idempotently using authoritative entity revisions;
- apply message deltas only once per `(turnId, deltaSequence)`;
- replace partial message content with the final completed message rather than
  assuming every delta was received.

The server guarantees ordered delivery within one stream connection. The client
still checks for a sequence gap. A gap triggers reconnect/resync rather than
applying later events out of order. A duplicate sequence is ignored.

This is deliberately not exactly-once. Exactly-once browser delivery would not
survive a page crash or a connection boundary, while stable identities and
idempotent projections do.

## 5. OCG wire event envelope

The proposed OCG-owned envelope is:

```json
{
  "protocolVersion": "1",
  "eventVersion": 1,
  "streamId": "project_<opaque-id>",
  "sequence": 1843,
  "eventId": "evt_01...",
  "occurredAt": "2026-09-25T12:34:56.123Z",
  "commandId": "cmd_01...",
  "sessionId": "ses_01...",
  "missionId": "mission_01...",
  "type": "conversation.message_delta",
  "payload": {}
}
```

Rules:

- `eventId`, `streamId`, and `sequence` are OCG identifiers, not OpenCode IDs.
- `commandId` correlates an event with a browser command when applicable.
- `sessionId` and `missionId` are optional scope, not a promise that every
  event belongs to both.
- `payload` is type-specific and contains only normalized OCG semantics.
- Raw OpenCode `type`, `data`, `assistantMessageID`, `ordinal`, provider
  objects, and executor-specific objects do not cross this boundary.
- The event stream includes state changes needed by the current UI, not every
  internal OpenCode event.

The initial normalized event vocabulary should be small:

```text
runtime.status_changed
conversation.session_created
conversation.session_updated
conversation.message_started
conversation.message_delta
conversation.message_completed
conversation.message_failed
conversation.message_cancelled
activity.started
activity.updated
activity.completed
mission.updated
worker.updated
approval.requested
approval.updated
command.accepted
command.rejected
warning
error
```

The mapper translates these events to the existing frontend
`OcgRuntimeEvent` union. A wire event that has no safe frontend equivalent is
retained only as a diagnostic or ignored; it is not passed through as an
untyped escape hatch.

## 6. Message streaming

### 6.1 Identity

The backend owns two identities:

- `messageId`: the durable message record identity;
- `turnId`: the assistant-generation identity for one response attempt.

An assistant retry gets a new `turnId`. It may produce a new message record or
replace the same pending assistant record only if OCG defines that behavior
explicitly. The browser must not use array position or timestamp as identity.

### 6.2 Event semantics

`conversation.message_started` establishes the assistant turn and contains the
initial normalized message projection.

`conversation.message_delta` is append-only for one `(turnId, deltaSequence)`:

```json
{
  "turnId": "turn_01...",
  "messageId": "msg_01...",
  "deltaSequence": 7,
  "textDelta": "next text"
}
```

The delta is not an OpenCode text event. It is an OCG text append operation.
Duplicate delta sequences are ignored. A missing sequence causes resync rather
than an invented concatenation.

`conversation.message_completed` is authoritative and always contains the
complete final message content, even when all deltas were delivered:

```json
{
  "turnId": "turn_01...",
  "message": {
    "id": "msg_01...",
    "role": "assistant",
    "status": "completed",
    "content": "complete final text",
    "createdAt": "..."
  }
}
```

`conversation.message_failed` and `conversation.message_cancelled` contain the
authoritative final partial message projection plus a structured error or
cancellation reason. A failed/cancelled turn is terminal; a later retry is a
new command/turn.

On reconnect, the snapshot is authoritative for the current partial/final
message. Replayed deltas may be duplicates. A completed event replaces local
partial content wholesale, which also repairs a client that missed a delta.

The current OCG plugin only considers a root Lead step complete when the
OpenCode step has `finish == "stop"`; tool-call intermediate steps, errors, and
interruptions are not successful message completion. The future OCG mapper must
preserve that semantic without exposing those OpenCode event names.

## 7. Tool and activity semantics

The wire activity projection should contain only safe, UI-relevant metadata:

```json
{
  "activityId": "act_01...",
  "messageId": "msg_01...",
  "turnId": "turn_01...",
  "label": "Workspace inspection",
  "kind": "workspace_read",
  "status": "running",
  "durationMs": null,
  "attempt": 1,
  "workerId": "worker_01...",
  "summary": "Reading selected workspace files",
  "resultPreview": null,
  "error": null
}
```

The normalized status set is:

```text
pending, running, waiting_approval, retrying,
success, failure, cancelled
```

`kind` is an OCG allowlisted semantic category, not an arbitrary executor
tool name. It may include categories such as `workspace_read`, `workspace_write`,
`verification`, `delegation`, or `other`. The browser may display `label`,
`summary`, bounded `resultPreview`, duration, attempt, and a safe error code;
it must not receive command lines, environment variables, credentials, raw
provider responses, or unrestricted executor metadata.

The existing frontend mapper can map `label`/`kind` to `ToolActivity.name`,
`status` to the existing activity status, and bounded result/error text to its
`summary`/`detail`. If the UI later needs worker IDs or structured errors,
those should be added to the frontend domain deliberately rather than leaking
the wire DTO wholesale.

## 8. Approval semantics

The current OCG approval primitive is a durable policy admission approval, not
an OpenCode permission prompt. It is bound to:

```text
approvalId
missionId
generation
action
currentExecutionId
```

The proposed request projection is:

```json
{
  "approvalId": "approval_01...",
  "missionId": "mission_01...",
  "generation": 3,
  "action": "continue_execution",
  "currentExecutionId": "exec_01...",
  "status": "pending",
  "requestedAt": "...",
  "expiresAt": null,
  "summary": "Resume the current Mission execution"
}
```

The response command is:

```json
{
  "commandId": "cmd_01...",
  "decision": "approve",
  "expected": {
    "missionId": "mission_01...",
    "generation": 3,
    "action": "continue_execution",
    "currentExecutionId": "exec_01..."
  },
  "note": null
}
```

Decisions currently supported by OCG core are only `approve` and `reject`.
There is no verified `allow once` or `allow always` policy. The browser must
not offer either label until OCG core defines its persistence and scope.

An approval response is idempotent for the same approval and decision. A stale,
already-resolved, generation-mismatched, or execution-mismatched approval
returns a typed `approval_stale`/`conflict` error and does not authorize work.
The approval request must not be auto-approved, auto-rejected, or treated as
dismissed when the browser disconnects. Dismissal is a UI action only unless
OCG defines an explicit reject/cancel policy.

If Mission state changes while approval is pending, the backend revalidates the
exact binding at command time. A terminal Mission or changed generation makes
the approval stale.

Whether an OpenCode runtime permission request should share this approval
primitive is unresolved. The current repositories do not expose such a
permission API, and it must not be conflated with policy admission.

## 9. Cancellation

Cancellation must be explicit about its target:

1. **Message generation cancellation** stops one assistant `turnId` for a
   session. This is the current frontend `cancel(sessionId)` use case; Phase 2B
   should add the turn/command identity once the HTTP adapter exists.
2. **Worker cancellation** stops one OCG worker activity. It must not imply the
   Mission is terminal.
3. **Mission cancellation** is a separate durable terminal transition bound to
   Mission ID and generation. It requires an explicit product command and is
   not the implementation of the current session cancel button.
4. **Browser disconnect** only tears down event delivery. It never sends a
   cancellation command and never changes Mission state.

The cancel command returns `command.accepted` when OCG accepted the request.
Only a later `conversation.message_cancelled`, `activity.completed` with
`cancelled`, or explicit Mission cancellation event is authoritative. If the
runtime cannot prove that work stopped, it must report a typed failure or
`cancellation_pending` state rather than claiming cancelled.

The existing Rust Mission rule supports an explicit durable `Cancelled`
terminal state, but the current runtime lifecycle trait has no cancel method and
the OpenCode adapter has no verified cancel route. The backend gap must be
closed before exposing Mission cancellation in the browser.

## 10. Mission and worker projection

The OCG wire Mission projection should contain the minimum authoritative facts
needed by MissionView:

```json
{
  "schemaVersion": 1,
  "missionId": "mission_01...",
  "revision": 18,
  "generation": 3,
  "goal": "...",
  "status": "active",
  "phase": "build",
  "currentTaskId": "task_02",
  "progress": { "completed": 3, "total": 7 },
  "tasks": [],
  "workers": [],
  "attempts": { "build": 1, "verify": 1, "debug": 0 },
  "warnings": [],
  "resourceCommitment": null,
  "budget": {
    "currency": "USD",
    "hardLimitMicros": 5000000,
    "settledMicros": 1200000,
    "reservedMicros": 100000,
    "unresolvedMicros": 0,
    "status": "active"
  },
  "createdAt": "...",
  "updatedAt": "...",
  "elapsedMs": 2520000
}
```

Authority rules:

- `missionId`, `revision`, `generation`, `status`, `phase`, task facts,
  attempts, warnings, and budget accounting are backend facts.
- `revision` is the optimistic-concurrency witness. The frontend does not
  increment it.
- `progress` and `tasks` must be explicitly supplied if the UI is expected to
  show exact totals. The current Rust Mission stores phase/checkpoints/findings,
  but does not currently expose a task list or completed/total projection.
  The adapter must not invent totals from checkpoint count.
- `workers` and `resourceCommitment` are optional until OCG owns durable worker
  and resource semantics. An absent value means unknown/not exposed, not zero.
- budget values are integer micro-units. The current frontend's display number
  fields are presentation-only and cannot be the long-term accounting schema.
- `elapsedMs` is a backend timestamp projection. The browser may format it but
  must not turn a disconnected wall clock into Mission progress.
- `currentTaskId`/`phase` should map to the current frontend `current` label in
  the adapter. Raw Rust role/transition internals remain out of React.

Terminal Mission statuses are authoritative and distinct: `completed`,
`failed`, and `cancelled`. `budget_exhausted` is best represented as a budget
status/reason plus an active or blocked Mission unless OCG core explicitly
adopts it as a durable Mission lifecycle status. The current Rust Mission
status set does not include `budget_exhausted`.

Worker events should be OCG worker projections (`workerId`, label, status,
taskId, attempt, summary), not OpenCode agent names. The current OCG code has
worker role/plugin knowledge but no durable worker registry or scheduler; that
is a backend gap.

## 11. Errors

Every non-success response uses one stable envelope:

```json
{
  "protocolVersion": "1",
  "error": {
    "code": "stale_command",
    "message": "The requested state changed before this command was applied.",
    "retryable": false,
    "requestId": "req_01...",
    "details": {
      "currentRevision": 19
    }
  }
}
```

`message` is safe for display but not for program logic. The frontend branches
on `code`:

| Code family | Meaning |
| --- | --- |
| `validation_error` | Request shape or field value is invalid |
| `authentication_required` / `forbidden` | Browser is not authorized; auth is outside Phase 2A |
| `runtime_unavailable` | OCG cannot reach or has no usable executor |
| `provider_failure` | Executor/provider failed an admitted operation |
| `mission_failed` | Durable Mission transition reached failed state |
| `transport_unavailable` | Temporary OCG transport failure; safe to retry/reconnect |
| `stale_command` / `conflict` | Revision/generation/owner precondition failed |
| `unknown_resource` | Session, Mission, turn, activity, or approval does not exist |
| `approval_stale` | Approval no longer matches the exact action binding |
| `cursor_expired` | Event replay is outside the retention window |
| `protocol_incompatible` | Major protocol mismatch |

Provider diagnostics may carry a bounded safe provider/model identifier, but no
raw provider response, credential, endpoint password, environment variable, or
unbounded executor error.

## 12. Mapping to the frontend domain

The future `HttpOcgRuntimeClient` should have an explicit mapper layer:

```text
OCG wire snapshot/event DTO
        ↓ validate protocol, revisions, enums, bounds
wire mapper
        ↓ map only supported semantics
frontend ChatSession / ChatMessage / Mission / ToolActivity
        ↓
OcgRuntimeEvent and RuntimeSnapshot
        ↓
React
```

Initial mapping rules:

| Wire value | Frontend value | Rule |
| --- | --- | --- |
| OCG session summary | `ChatSession` | Map only OCG-owned id/title/work grouping/update timestamp; never expose OpenCode session JSON |
| OCG message | `ChatMessage` | Map stable id, role, content, created time, and normalized status |
| message delta | `conversation.message-delta` | Apply only once per turn/delta sequence; do not expose executor event names |
| OCG activity | `ToolActivity` | Map allowlisted label/kind/status/duration/summary/result preview/error |
| Mission projection | `Mission` | Map backend-authoritative fields; format presentation labels in the mapper/view |
| integer budget micros | current budget display | Format for display only; do not use frontend float as accounting authority |
| `runtime.status_changed` | `RuntimeStatus` | Preserve connected/connecting/disconnected/failed semantics |
| structured error | warning/error event | Branch on stable code; retain safe message only for display |

The current `getSnapshot()` method is a local external-store optimization, not
a wire operation. The HTTP client can implement it from its locally installed
snapshot, but it must initialize that snapshot from the atomic bootstrap
response and update it only through mapped authoritative events.

The current `subscribe(listener)` method should hide the SSE connection,
reconnect cursor, duplicate filtering, gap detection, and resync loop. React
must receive only the existing normalized frontend event union.

The current `cancel?(sessionId)` is underspecified for a real backend. Before
HTTP integration, it should become a message-turn cancellation command carrying
`commandId` and `turnId`, while Mission and worker cancellation remain separate
operations.

Approval response is not currently represented in `OcgRuntimeClient`. Before
an approval control is wired, add a frontend-owned method such as
`respondToApproval(input)` whose input contains `approvalId`, decision, and the
expected Mission binding. Do not make React call an arbitrary URL.

## 13. Security boundary

The browser may receive:

- opaque session/Mission/turn/activity IDs;
- safe runtime state and capability names;
- bounded display labels and summaries;
- safe provider/model display metadata only if product requires it;
- integer budget projections and safe reason codes.

The browser must never receive:

- OpenCode service URL/password;
- provider API keys or OpenCode `auth.json` contents;
- environment variables or filesystem secrets;
- raw OpenCode request/event payloads;
- arbitrary command lines or tool arguments;
- unbounded provider/executor diagnostics;
- internal local paths unless explicitly redacted/approved for display.

Authentication, authorization, session ownership, and CSRF/origin policy are
not implemented in Phase 2A. They are required before exposing the proposed
routes outside a trusted local development process.

## 14. Backend gaps and unresolved decisions

These require OCG core/product decisions before a production adapter:

1. Where does the OCG-owned browser server run and how is it attached to an
   invocation-scoped OpenCode runtime without exposing the loopback secret?
2. What durable projection/read model provides an atomic snapshot plus event
   high-watermark across file-backed session and Mission state?
3. What event journal is retained, and are the proposed 10,000/24-hour replay
   defaults acceptable?
4. How does OCG send a normal user message through the runtime? The current
   V2 adapter intentionally exposes synthetic continuation, not ordinary prompt
   admission from a browser.
5. What verified OpenCode/runtime command stops an assistant turn or worker?
   The current OCG lifecycle trait has no cancellation operation.
6. Which OpenCode permission prompts, if any, become OCG approval requests?
   Current OCG approvals are control-plane policy approvals only.
7. Does OCG own a task list/progress model and worker registry, or should the
   frontend Mission projection show unknown values until one exists?
8. Which resource commitment facts are authoritative while Resource Broker,
   placement, and scheduling remain deferred?
9. Does the product want a Mission cancellation endpoint separate from message
   cancellation, and what confirmation/authorization is required?
10. What safe provider/model metadata is useful enough to expose to the
    browser?
11. What browser authentication and project/session authorization model will
    protect these operations?

## 15. Phase 2B recommendation

Do not begin with `HttpOcgRuntimeClient` or guessed routes. First implement one
small OCG-owned server-side vertical slice behind the proposed versioned
namespace:

1. define the protocol/error schemas and protocol-version handshake;
2. create an authoritative snapshot projection for runtime, sessions, one
   message history, and one Mission projection;
3. add a durable/global event cursor with a bounded replay window;
4. expose read-only snapshot + replay first;
5. prove duplicate delivery, cursor expiry, sequence gaps, and snapshot/resume
   with contract tests;
6. then add message admission/streaming and explicit message cancellation;
7. add approvals only after deciding whether the product approval is the
   existing OCG policy approval or a separate runtime permission primitive.

The frontend's next implementation should be a contract-test fixture or a
transport DTO package only after the backend confirms these shapes. It should
not contain OpenCode route knowledge or a real network call until that point.
