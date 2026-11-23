import assert from "node:assert/strict";
import { fork } from "node:child_process";
import { once } from "node:events";
import { access, mkdir, mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright-core";

const repo = fileURLToPath(new URL("../../", import.meta.url));
const uiDirectory = resolve(process.env.OCG_BENCH_UI_DIR ?? join(repo, "frontend/out"));
const binary = process.platform === "win32" ? "ocg.exe" : "ocg";
const backendPath = resolve(process.env.OCG_BENCH_BACKEND ?? join(repo, "target/debug", binary));
const viewport = '[data-slot="aui_thread-viewport"]';

async function control(fixture, action, method = "GET") {
  const response = await fetch(`http://127.0.0.1:${fixture.providerPort}/control/${action}`, {
    method,
    signal: AbortSignal.timeout(10_000),
  });
  assert.ok(response.ok, `fixture ${action}: ${response.status}`);
  return response;
}

async function eventually(condition, label, timeout = 12_000) {
  const deadline = Date.now() + timeout;
  let last;
  while (Date.now() < deadline) {
    try {
      if (await condition()) return;
    } catch (error) {
      last = error;
    }
    await sleep(75);
  }
  throw new Error(`Timed out waiting for ${label}${last ? `: ${last}` : ""}`);
}

async function conversations(fixture, archived) {
  const response = await fetch(
    `${fixture.origin}/api/v1/conversations${archived ? "/archived" : ""}?limit=50`,
    {
      headers: { Authorization: `Bearer ${fixture.backendToken}` },
      signal: AbortSignal.timeout(10_000),
    },
  );
  assert.ok(response.ok, `conversation read failed: ${response.status}`);
  const result = await response.json();
  assert.ok(Array.isArray(result.conversations), "unexpected conversation response");
  return result.conversations;
}

async function archiveDialog(page) {
  const actions = page.getByRole("button", { name: /^Chat actions:/ }).first();
  await actions.waitFor({ timeout: 15_000 });
  await actions.hover();
  await actions.click();
  await page.getByRole("menuitem", { name: "Archive", exact: true }).click();
  const dialog = page.getByRole("dialog", {
    name: /^(Archive this chat|Stop and archive this chat)\?/,
  });
  await dialog.waitFor();
  return dialog;
}

async function archiveAndRestore(page, fixture) {
  const dialog = await archiveDialog(page);
  await dialog.getByRole("button", { name: "Archive", exact: true }).click();
  await dialog.waitFor({ state: "hidden" });
  await eventually(
    async () => (await conversations(fixture, true)).some((item) => item.root_job === fixture.root),
    "archived conversation on disk",
  );

  await page.getByRole("button", { name: "Account", exact: true }).click();
  await page.getByRole("menuitem", { name: "Archived", exact: true }).click();
  const list = page.getByRole("dialog", { name: "Archived", exact: true });
  await list.getByRole("button", { name: "Unarchive", exact: true }).click();
  await eventually(
    async () =>
      (await conversations(fixture, false)).some((item) => item.root_job === fixture.root),
    "restored conversation on disk",
  );
  await page.keyboard.press("Escape");
  await page.reload({ waitUntil: "load" });
  await page
    .getByRole("button", { name: /^Chat actions:/ })
    .first()
    .waitFor();
  await page.getByRole("button", { name: "Account", exact: true }).click();
  await page.getByRole("menuitem", { name: "Archived", exact: true }).click();
  await page.getByText("No archived chats.", { exact: true }).waitFor();
  await page.keyboard.press("Escape");
  assert.ok(
    (await conversations(fixture, false)).some((item) => item.root_job === fixture.root),
    "the restored conversation disappeared after refresh",
  );
  await page.goto(`${fixture.origin}/?job=${fixture.root}`, { waitUntil: "load" });
  await page.locator(viewport).waitFor();
}

async function stopFailureKeepsConversation(page, fixture) {
  const { prefix } = await (await control(fixture, "new", "POST")).json();
  await eventually(
    async () => (await (await control(fixture, "status")).json()).waiting,
    "provider to hold the live Job",
    20_000,
  );
  let stopRequests = 0;
  let archiveWrites = 0;
  await page.route(`**/api/v1/conversations/${fixture.root}/stop`, async (route) => {
    stopRequests++;
    await route.abort("failed");
  });
  await page.route(new RegExp(`/api/v1/conversations/${fixture.root}(?:\\?|$)`), async (route) => {
    if (route.request().method() === "POST") archiveWrites++;
    await route.continue();
  });

  const dialog = await archiveDialog(page);
  await dialog.getByRole("button", { name: "Stop and archive", exact: true }).click();
  await dialog.getByRole("alert").waitFor({ timeout: 15_000 });
  assert.equal(stopRequests, 1, "the UI did not attempt exactly one stop");
  assert.equal(archiveWrites, 0, "the UI archived a chat without a confirmed stop");
  assert.ok(
    (await conversations(fixture, false)).some((item) => item.root_job === fixture.root),
    "the unsettled conversation disappeared from the active list",
  );
  assert.ok(
    !(await conversations(fixture, true)).some((item) => item.root_job === fixture.root),
    "the unsettled conversation was archived",
  );
  await page.keyboard.press("Escape");
  await page.unrouteAll();
  return prefix;
}

async function streamingKeepsManualAnchor(page, fixture, prefix) {
  const thread = page.locator(viewport);
  await thread.waitFor();
  const initial = await thread.evaluate((element) => {
    const scrollRange = element.scrollHeight - element.clientHeight;
    if (scrollRange < 200) throw new Error("the regression fixture is not scrollable");
    element.scrollTop = Math.round(scrollRange * 0.35);
    return element.scrollTop;
  });
  await control(fixture, "release", "POST");
  const finalMarker = `${prefix}-${String(fixture.updateCount).padStart(3, "0")}`;
  await page.waitForFunction(
    ({ selector, marker }) => document.querySelector(selector)?.textContent?.includes(marker),
    { selector: viewport, marker: finalMarker },
    { timeout: 30_000 },
  );
  await thread
    .locator('[data-role="assistant"]')
    .last()
    .locator('[data-slot="aui_assistant-message-footer"]')
    .waitFor({ timeout: 15_000 });
  const settled = await thread.evaluate(async (element) => {
    let prior = "";
    let repeated = 0;
    for (let frame = 0; frame < 90; frame++) {
      await new Promise((resolve) => requestAnimationFrame(resolve));
      const state = `${element.scrollTop}/${element.scrollHeight}/${element.clientHeight}`;
      repeated = state === prior ? repeated + 1 : 0;
      if (repeated >= 4) return element.scrollTop;
      prior = state;
    }
    throw new Error("streaming layout never settled");
  });
  assert.ok(
    Math.abs(settled - initial) <= 1,
    `manual scroll anchor drifted by ${settled - initial}px`,
  );
}

async function run() {
  await access(join(uiDirectory, "index.html"));
  await access(backendPath);
  const directory = await mkdtemp(join(tmpdir(), "ocg-browser-regression-"));
  console.log("Browser regression artifacts:", directory);
  await mkdir(join(directory, "project"));
  const child = fork(
    new URL("./fixture.mjs", import.meta.url),
    [directory, backendPath, uiDirectory],
    {
      env: { ...process.env, OCG_BENCH_TURNS: "20" },
    },
  );
  const exited = once(child, "exit");
  try {
    const fixture = await Promise.race([
      once(child, "message").then(([message]) => message),
      exited.then(([code, signal]) => {
        throw new Error(`fixture exited before ready (${code ?? signal})`);
      }),
    ]);
    const browser = await chromium.launch({ headless: true });
    try {
      const context = await browser.newContext({
        viewport: { width: 1440, height: 900 },
        locale: "en-US",
        reducedMotion: "reduce",
      });
      try {
        const page = await context.newPage();
        const pageErrors = [];
        page.on("pageerror", (error) => pageErrors.push(error.message));
        await page.goto(`${fixture.origin}/?job=${fixture.root}`, { waitUntil: "load" });
        await page.locator(`${viewport} [data-role="assistant"]`).nth(10).waitFor({
          timeout: 15_000,
        });
        await archiveAndRestore(page, fixture);
        const prefix = await stopFailureKeepsConversation(page, fixture);
        await streamingKeepsManualAnchor(page, fixture, prefix);
        assert.deepEqual(pageErrors, [], "browser reported uncaught errors");
        console.log("PASS: archive failure, persistent unarchive, manual scroll anchor");
      } finally {
        await context.close();
      }
    } finally {
      await browser.close();
    }
  } finally {
    if (child.connected) child.disconnect();
    await exited;
  }
}

await run();
