import assert from "node:assert/strict";
import { test } from "node:test";
import {
  boundLogEntries,
  createLogsLiveFixture,
  filterLogEntries,
  newLogCount,
  normalizeLogEntry,
} from "./domain";
import { resolveScenario } from "../runtime/scenarios";

test("logs-live is a supported deterministic scenario", () => {
  assert.equal(resolveScenario("logs-live"), "logs-live");
});

test("logs filter by level, dynamic source, and searchable context", () => {
  const entries = createLogsLiveFixture();
  assert.equal(filterLogEntries(entries, { level: "error" }).length, 1);
  assert.equal(filterLogEntries(entries, { source: "Provider" }).length, 1);
  assert.equal(filterLogEntries(entries, { text: "Muse Spark" }).length, 6);
  assert.equal(filterLogEntries([
    ...entries,
    { ...entries[0], id: "future-source", source: "Future Adapter", message: "future event" },
  ], { source: "Future Adapter" }).length, 1);
});

test("log history remains bounded and follow state exposes new entries", () => {
  const entries = createLogsLiveFixture();
  assert.equal(boundLogEntries(entries, 3).length, 3);
  assert.equal(boundLogEntries(entries, 3)[0]?.id, "logs-error");
  assert.equal(newLogCount(12, 9, { following: false, atBottom: false }), 3);
  assert.equal(newLogCount(12, 9, { following: true, atBottom: true }), 0);
});

test("redaction replaces sensitive values and does not expose their contents", () => {
  const secret = "not-renderable-secret";
  const entry = normalizeLogEntry({
    id: "redaction-test",
    timestamp: "2026-09-25T10:00:00.000Z",
    level: "info",
    source: "Provider",
    message: "safe diagnostic",
    fields: { apiKey: secret, nested: { authorization: `Bearer ${secret}`, attempt: 2 } },
  });
  const serialized = JSON.stringify(entry);
  assert.equal(entry.redacted, true);
  assert.equal(serialized.includes(secret), false);
  assert.equal(serialized.includes("[redacted]"), true);
});

test("logs-live fixture covers startup, retry, failure, recovery, and completion", () => {
  const entries = createLogsLiveFixture();
  assert.equal(entries[0]?.source, "OCG Core");
  assert.ok(entries.some((entry) => entry.level === "warn" && entry.category === "retry"));
  assert.ok(entries.some((entry) => entry.level === "error"));
  assert.equal(entries.at(-1)?.message, "Mission completed successfully");
  assert.equal(entries.find((entry) => entry.id === "logs-redaction")?.redacted, true);
});
