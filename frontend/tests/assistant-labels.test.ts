import test from "node:test";
import assert from "node:assert/strict";
import { dictionaries, en, formatTemplate, zh } from "../components/ocg/i18n/dictionaries";

test("assistant thread labels exist in both languages and interpolate counts", () => {
  const keys = Object.keys(en).filter((key) => key.startsWith("aui."));
  assert.ok(keys.length > 0, "expected assistant thread labels");
  for (const key of keys) {
    assert.ok(en[key as keyof typeof en].length > 0, `${key} has English text`);
    assert.ok(zh[key as keyof typeof en].length > 0, `${key} has Chinese text`);
  }
  assert.equal(formatTemplate(en["aui.toolCalls.one"], { count: 1 }), "1 tool call");
  assert.equal(formatTemplate(en["aui.toolCalls.other"], { count: 3 }), "3 tool calls");
  assert.equal(formatTemplate(zh["aui.toolCalls.one"], { count: 1 }), "1 个工具调用");
  assert.equal(formatTemplate(zh["aui.toolCalls.other"], { count: 3 }), "3 个工具调用");
  for (const key of ["common.copy", "common.edit", "common.cancel", "app.loadingConversation"] as const) {
    assert.ok(en[key].length > 0, `${key} has English text`);
    assert.ok(zh[key].length > 0, `${key} has Chinese text`);
  }
});

test("English and Chinese dictionaries cover the same keys", () => {
  assert.deepEqual(Object.keys(zh).sort(), Object.keys(en).sort());
  assert.equal(dictionaries["en-US"], en);
  assert.equal(dictionaries["zh-CN"], zh);
});
