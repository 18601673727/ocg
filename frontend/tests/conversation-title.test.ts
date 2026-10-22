import assert from "node:assert/strict";
import test from "node:test";
import { jobTranscript } from "../components/ocg/chat/job-transcript-model";
import type { ControlOutput } from "../components/ocg/contracts";

function answer(text: string, settled = false, titleMetadata = true) {
  const output: ControlOutput = {
    id: "output:1", seq: 1, kind: settled ? "result" : "progress", summary: text,
    code: null, goal_reached: null, value: null, created_at_us: 1,
  };
  return jobTranscript("00000001-0000-7001-8000-000000000001", null, [output], { state: settled ? "completed" : "unsettled" }, !settled, undefined, undefined, titleMetadata)[0].content;
}

test("a streaming title stays hidden and the answer appears as it arrives", () => {
  for (const partial of ["<", "<ocg-title>", "<ocg-title>项目结构"]) assert.equal(answer(partial), "");
  assert.equal(answer("<ocg-title>项目结构</ocg-title>\nHere is the answer."), "Here is the answer.");
  assert.equal(answer("Here is the answer.", true), "Here is the answer.");
});

test("malformed or absent title metadata never discards a settled answer", () => {
  for (const text of ["Normal answer", "<", "<ocg-title>Incomplete", "<ocg-title> </ocg-title>Answer", "<ocg-title>A\nB</ocg-title>Answer", `<ocg-title>${"a".repeat(81)}</ocg-title>Answer`]) {
    assert.equal(answer(text, true), text);
  }
  const text = "<ocg-title>Literal example</ocg-title>Answer";
  assert.equal(answer(text, true, false), text);
});
