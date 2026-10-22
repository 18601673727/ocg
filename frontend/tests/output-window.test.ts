import test from "node:test";
import assert from "node:assert/strict";
import type { ControlOutput } from "../components/ocg/contracts";
import {
  JOB_WATCH_RESIDENT_OUTPUTS,
  JOB_WATCH_WINDOW_STEP,
  hasEvictedOutputs,
  mergeOutputWindow,
} from "../components/ocg/job/output-window";

function output(seq: number): ControlOutput {
  return {
    id: `output:${seq}`,
    seq,
    kind: "progress",
    summary: `part ${seq}`,
    code: null,
    goal_reached: null,
    value: null,
    created_at_us: seq,
  };
}

test("a long-output Job retains a bounded resident window", () => {
  let resident: ControlOutput[] = [];
  const total = JOB_WATCH_RESIDENT_OUTPUTS * 5;
  for (let seq = 1; seq <= total; seq++) {
    resident = mergeOutputWindow(resident, [output(seq)], JOB_WATCH_RESIDENT_OUTPUTS);
    assert.ok(resident.length <= JOB_WATCH_RESIDENT_OUTPUTS, `resident rows stay bounded at seq ${seq}`);
  }
  assert.equal(resident.length, JOB_WATCH_RESIDENT_OUTPUTS);
  assert.equal(resident[0].seq, total - JOB_WATCH_RESIDENT_OUTPUTS + 1);
  assert.equal(resident.at(-1)?.seq, total);
  assert.ok(hasEvictedOutputs(total, resident.length));
});

test("merging never gaps or duplicates across overlapping walks", () => {
  const first = Array.from({ length: 200 }, (_, index) => output(index + 1));
  const resident = mergeOutputWindow([], first, JOB_WATCH_RESIDENT_OUTPUTS);
  // A re-walk rereads everything while the stream appended two more rows.
  const walked = [...first, output(201), output(202)];
  const expanded = mergeOutputWindow(resident, walked, JOB_WATCH_RESIDENT_OUTPUTS + JOB_WATCH_WINDOW_STEP);
  const seqs = expanded.map((entry) => entry.seq);
  assert.deepEqual(seqs, [...new Set(seqs)].sort((a, b) => a - b));
  assert.equal(expanded.length, 202);
  assert.equal(expanded[0].seq, 1);
  assert.equal(expanded.at(-1)?.seq, 202);
});

test("live appends trim from the front and keep stored order", () => {
  const resident = mergeOutputWindow([output(1), output(2)], [output(2), output(3)], 2);
  assert.deepEqual(resident.map((entry) => entry.seq), [2, 3]);
  assert.ok(!hasEvictedOutputs(2, 2));
  assert.ok(hasEvictedOutputs(3, 2));
});
