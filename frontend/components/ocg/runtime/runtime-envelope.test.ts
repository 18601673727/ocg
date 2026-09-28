import { test } from "node:test";
import assert from "node:assert/strict";
import {
  RuntimeEnvelopeFactory,
  envelopeFromRuntimeEvent,
  toRuntimeEvent,
  validateRuntimeEnvelope,
} from "./runtime-envelope";

test("valid runtime envelopes normalize and round-trip to the presentation event", () => {
  const factory = new RuntimeEnvelopeFactory("stream:envelope", 1, { startSequence: 4 });
  const envelope = factory.envelope(
    "runtime.status-changed",
    { status: { state: "connected" } },
    { projectId: null },
  );
  assert.equal(envelope.protocolVersion, 1);
  assert.equal(envelope.eventVersion, 1);
  assert.equal(envelope.sequence, 4);
  assert.equal(envelope.eventId, "stream:envelope:4");
  assert.equal(envelope.projectId, null);

  const result = validateRuntimeEnvelope(envelope);
  assert.equal(result.ok, true);
  if (result.ok) {
    assert.deepEqual(toRuntimeEvent(result.envelope), { type: "runtime.status-changed", status: { state: "connected" } });
  }
});

test("session events carry inferred scope in the header and round-trip", () => {
  const envelope = envelopeFromRuntimeEvent(
    { type: "mission.updated", sessionId: "design-pwa-shell", mission: { status: "running", title: "T" } as never },
    {
      streamId: "stream:s",
      generation: 1,
      sequence: 1,
      eventId: "stream:s:1",
      occurredAt: new Date(0).toISOString(),
      projectId: "zhuju",
    },
  );
  assert.equal(envelope.sessionId, "design-pwa-shell");
  assert.equal(envelope.projectId, "zhuju");
});

test("unsupported protocol versions are rejected explicitly", () => {
  const result = validateRuntimeEnvelope({
    protocolVersion: 2,
    eventVersion: 1,
    streamId: "stream:s",
    generation: 1,
    sequence: 1,
    eventId: "stream:s:1",
    projectId: null,
    occurredAt: new Date(0).toISOString(),
    type: "warning",
    payload: { message: "hi" },
  });
  assert.equal(result.ok, false);
  if (!result.ok) assert.equal(result.diagnostic.code, "protocol-incompatible");
});

test("malformed envelopes yield schema diagnostics instead of escaping untyped", () => {
  const base = {
    protocolVersion: 1,
    eventVersion: 1,
    streamId: "stream:s",
    generation: 1,
    sequence: 1,
    eventId: "stream:s:1",
    projectId: null,
    occurredAt: new Date(0).toISOString(),
  };

  const missingEventId = validateRuntimeEnvelope({ ...base, eventId: undefined, type: "warning", payload: { message: "x" } });
  assert.equal(missingEventId.ok, false);
  if (!missingEventId.ok) assert.equal(missingEventId.diagnostic.code, "schema-invalid");

  const badSequence = validateRuntimeEnvelope({ ...base, sequence: -1, type: "warning", payload: { message: "x" } });
  assert.equal(badSequence.ok, false);

  const badPayload = validateRuntimeEnvelope({ ...base, type: "conversation.message-delta", payload: { messageId: "m" } });
  assert.equal(badPayload.ok, false);
  if (!badPayload.ok) assert.equal(badPayload.diagnostic.code, "schema-invalid");

  const badStatus = validateRuntimeEnvelope({
    ...base,
    type: "runtime.status-changed",
    payload: { status: { state: "teleporting" } },
  });
  assert.equal(badStatus.ok, false);
});

test("unknown event types are retained as a diagnostic, never applied optimistically", () => {
  const result = validateRuntimeEnvelope({
    protocolVersion: 1,
    eventVersion: 1,
    streamId: "stream:s",
    generation: 1,
    sequence: 1,
    eventId: "stream:s:1",
    projectId: null,
    occurredAt: new Date(0).toISOString(),
    type: "approval.granted",
    payload: { approvalId: "x" },
  });
  assert.equal(result.ok, false);
  if (!result.ok) {
    assert.equal(result.diagnostic.code, "unknown-event-type");
    assert.equal(result.diagnostic.severity, "warning");
  }
});
