import test from "node:test";
import assert from "node:assert/strict";
import {
  ContractError,
  decodeConversations,
} from "../components/ocg/contracts/control-plane";

function conversation(hold: unknown) {
  return {
    api_version: 1,
    conversations: [{
      root_job: "00000001-0000-7001-8000-000000000001",
      tip_job: "00000003-0000-7001-8000-000000000003",
      agent: null,
      title: "chat",
      state: "open",
      on_hold: hold,
      project: null,
      tip_settlement_state: "unsettled",
      tip_created_at_us: 1,
    }],
    has_more: false,
    next_before: null,
  };
}

test("an unknown hold actor fails with the actor path", () => {
  assert.throws(
    () => decodeConversations(conversation({ by: { kind: "invalid-actor" } })),
    (error: unknown) => error instanceof ContractError && /conversation\.on_hold\.by\.kind/.test(error.message),
  );
});

test("an Agent hold without a valid conversation fails with the conversation path", () => {
  assert.throws(
    () => decodeConversations(conversation({ by: { kind: "agent" } })),
    (error: unknown) => error instanceof ContractError && /conversation\.on_hold\.by\.conversation/.test(error.message),
  );
  assert.throws(
    () => decodeConversations(conversation({ by: { kind: "agent", conversation: "nope" } })),
    (error: unknown) => error instanceof ContractError && /conversation\.on_hold\.by\.conversation/.test(error.message),
  );
});

test("null and every valid hold actor decode", () => {
  assert.equal(decodeConversations(conversation(null)).conversations[0].on_hold, null);
  for (const by of [
    { kind: "user" },
    { kind: "agent", conversation: "00000009-0000-7001-8000-000000000009" },
    { kind: "loop" },
    { kind: "auto" },
  ] as const) {
    assert.deepEqual(
      decodeConversations(conversation({ by })).conversations[0].on_hold,
      { by },
    );
  }
});
