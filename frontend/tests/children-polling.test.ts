import test from "node:test";
import assert from "node:assert/strict";
import {
  CHILDREN_POLL_BASE_MS,
  CHILDREN_POLL_MAX_MS,
  childrenListSignature,
  nextChildrenPollDelay,
} from "../components/ocg/chat/children-polling";

test("quiet refreshes double the delay up to the idle ceiling", () => {
  assert.equal(CHILDREN_POLL_BASE_MS, 2000);
  assert.equal(CHILDREN_POLL_MAX_MS, 30_000);
  const delays = [CHILDREN_POLL_BASE_MS];
  for (let tick = 0; tick < 6; tick++) delays.push(nextChildrenPollDelay(delays.at(-1)!));
  assert.deepEqual(delays, [2000, 4000, 8000, 16_000, 30_000, 30_000, 30_000]);
});

test("request trace: an idle view issues far fewer requests than the fixed loop", () => {
  // Mirrors the strip's update rule: the first sighting of a signature stays
  // at the base cadence; only consecutive identical refreshes back off.
  let signature: string | null = null;
  let delay = CHILDREN_POLL_BASE_MS;
  const requests = [0];
  let now = 0;
  const idle = childrenListSignature([], false);
  while (now <= 120_000) {
    delay = signature === null || signature !== idle ? CHILDREN_POLL_BASE_MS : nextChildrenPollDelay(delay);
    signature = idle;
    now += delay;
    if (now > 120_000) break;
    requests.push(now);
  }
  assert.deepEqual(requests, [0, 2000, 6000, 14_000, 30_000, 60_000, 90_000, 120_000]);
  // The previous fixed two-second loop issued 61 requests in the same window.
  assert.ok(requests.length < 61 / 4);
});

test("a changed list returns to the base cadence", () => {
  const empty = childrenListSignature([], false);
  const settled = childrenListSignature([{ id: "00000002-0000-7001-8000-000000000002" }], false);
  const paged = childrenListSignature([{ id: "00000002-0000-7001-8000-000000000002" }, { id: "00000003-0000-7001-8000-000000000003" }], true);
  assert.notEqual(empty, settled);
  assert.notEqual(settled, paged);
  assert.equal(paged, childrenListSignature([{ id: "00000002-0000-7001-8000-000000000002" }, { id: "00000003-0000-7001-8000-000000000003" }], true));
  // An empty or settled list has a stable signature, so discovery backs off
  // without ever stopping.
  assert.equal(empty, childrenListSignature([], false));
});
