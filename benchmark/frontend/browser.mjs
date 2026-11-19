import { fork } from "node:child_process";
import { once } from "node:events";
import { access, mkdir, mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { gzipSync } from "node:zlib";
import { chromium } from "playwright-core";

const repo = fileURLToPath(new URL("../../", import.meta.url));
const uiDirectory = join(repo, "frontend/out");
const binary = process.platform === "win32" ? "ocg.exe" : "ocg";
const backendPath = resolve(process.env.OCG_BENCH_BACKEND ?? join(repo, "target/debug", binary));
const cpuRates = (process.argv[2] ?? "1,4,4,4").split(",").map(Number);
const viewport = '[data-slot="aui_thread-viewport"]';
const assistantMessages = '[data-role="assistant"]';

function summarize(values) {
  const sorted = values.toSorted((left, right) => left - right);
  const percentile = (percent) => sorted[Math.ceil(sorted.length * percent) - 1];
  return { n: sorted.length, p50: percentile(0.5), p95: percentile(0.95), max: sorted.at(-1) };
}

async function control(fixture, action, method = "GET") {
  const url = `http://127.0.0.1:${fixture.providerPort}/control/${action}`;
  const response = await fetch(url, { method, signal: AbortSignal.timeout(10_000) });
  if (!response.ok) throw new Error(`Mock provider ${action}: ${await response.text()}`);
  return response;
}

async function startJob(fixture) {
  const { prefix } = await (await control(fixture, "new", "POST")).json();
  for (let attempt = 0; attempt < 120; attempt++) {
    if ((await (await control(fixture, "status")).json()).waiting) return prefix;
    await sleep(50);
  }
  throw new Error("Mock provider did not receive the benchmark job");
}

async function firstScreenJavaScript(page) {
  const resources = await page.evaluate(() => {
    const files = new Set();
    for (const entry of performance.getEntriesByType("resource")) {
      const url = new URL(entry.name);
      if (url.origin === location.origin && url.pathname.endsWith(".js")) files.add(url.pathname);
    }
    return [...files];
  });
  const assets = await Promise.all(resources.map((name) => readFile(join(uiDirectory, name))));
  return {
    files: assets.length,
    raw: assets.reduce((bytes, asset) => bytes + asset.length, 0),
    gzip: assets.reduce((bytes, asset) => bytes + gzipSync(asset).length, 0),
  };
}

async function openConversation(page, fixture) {
  await page.goto(`${fixture.origin}/?job=${fixture.root}`, { waitUntil: "load" });
  await page.locator(viewport).waitFor({ timeout: 12_000 });
  const assistants = page.locator(`${viewport} ${assistantMessages}`);
  await assistants.nth(17).waitFor({ state: "attached", timeout: 12_000 });
  const js = await firstScreenJavaScript(page);
  for (let pageNumber = 0; pageNumber < 5; pageNumber++) {
    const earlier = page.getByRole("button", { name: /load earlier/i });
    if ((await earlier.count()) === 0) break;
    const previous = await assistants.count();
    await earlier.click({ timeout: 4_000 });
    await assistants.nth(previous).waitFor({ state: "attached", timeout: 10_000 });
  }
  const rows = await page.locator(`${viewport} [data-role]`).count();
  return { js, rows };
}

async function measureScroll(page) {
  await page.bringToFront();
  return page.locator(viewport).evaluate(async (element) => {
    const range = element.scrollHeight - element.clientHeight;
    element.scrollTop = range * 0.4;
    const start = element.scrollTop;
    await new Promise((done) => requestAnimationFrame(() => requestAnimationFrame(done)));
    const times = [];
    const offsets = [];
    const final = await new Promise((done) => {
      let last;
      let step = 0;
      const frame = (time) => {
        if (last !== undefined) times.push(time - last);
        last = time;
        element.scrollTop += step < 60 ? 32 : -32;
        offsets.push(element.scrollTop);
        if (++step < 120) requestAnimationFrame(frame);
        else done(element.scrollTop);
      };
      requestAnimationFrame(frame);
    });
    element.scrollTop = element.scrollHeight;
    return {
      times,
      range,
      distance: Math.max(...offsets) - start,
      finalOffset: Math.abs(final - start),
    };
  });
}

async function observe(page, prefix) {
  await page.locator(viewport).evaluate(
    (element, { prefix, assistantMessages }) => {
      const seen = Object.create(null);
      const markers = new RegExp(`${prefix}-\\d{3}`, "g");
      const observer = new MutationObserver(() => {
        const messages = element.querySelectorAll(assistantMessages);
        const current = messages[messages.length - 1]?.textContent ?? "";
        const time = Date.now();
        for (const match of current.matchAll(markers)) seen[match[0]] ??= time;
      });
      observer.observe(element, { subtree: true, childList: true, characterData: true });
      window.ocgMeasurement = { seen, observer, initialOffset: element.scrollTop };
    },
    { prefix, assistantMessages },
  );
}

async function readSentTimes(directory, prefix, updateCount) {
  const file = join(directory, `${prefix}.json`);
  for (let attempt = 0; attempt < 150; attempt++) {
    await sleep(60);
    try {
      const sent = JSON.parse(await readFile(file, "utf8"));
      if (Object.keys(sent).length === updateCount) return sent;
    } catch (error) {
      if (error.code !== "ENOENT" && !(error instanceof SyntaxError)) throw error;
    }
  }
  throw new Error(`Stream ${prefix} did not produce ${updateCount} timestamps`);
}

async function finish(page, sent, updateCount) {
  await page.waitForFunction(
    (count) => Object.keys(window.ocgMeasurement.seen).length === count,
    updateCount,
    { timeout: 12_000 },
  );
  const { seen, lastUpdateBottomGap } = await page.locator(viewport).evaluate((element) => {
    const { observer, seen } = window.ocgMeasurement;
    observer.disconnect();
    return {
      seen,
      lastUpdateBottomGap: element.scrollHeight - element.clientHeight - element.scrollTop,
    };
  });
  const delays = Object.entries(sent).map(([marker, time]) => seen[marker] - time);
  if (delays.some((delay) => !Number.isFinite(delay) || delay < 0)) {
    throw new Error("Invalid stream timestamps");
  }

  // The final marker can render before the completed answer and its footer.
  await page
    .locator(`${viewport} ${assistantMessages}`)
    .last()
    .locator('[data-slot="aui_assistant-message-footer"]')
    .waitFor({ state: "attached", timeout: 12_000 });
  const scroll = await page.locator(viewport).evaluate(async (element) => {
    let previous = "";
    let stableFrames = 0;
    for (let frame = 0; frame < 60; frame++) {
      await new Promise((resolve) => requestAnimationFrame(resolve));
      const { scrollTop, scrollHeight, clientHeight } = element;
      const geometry = `${scrollTop}/${scrollHeight}/${clientHeight}`;
      stableFrames = geometry === previous ? stableFrames + 1 : 0;
      if (stableFrames === 3) {
        return {
          offsetDelta: scrollTop - window.ocgMeasurement.initialOffset,
          bottomGap: scrollHeight - clientHeight - scrollTop,
          visibility: document.visibilityState,
        };
      }
      previous = geometry;
    }
    throw new Error("Scroll geometry did not settle after the stream completed");
  });
  return { delays, lastUpdateBottomGap, ...scroll };
}

async function measure(browser, fixture, directory, rate, keepAnchor) {
  const context = await browser.newContext({
    viewport: { width: 1440, height: 900 },
    locale: "en-US",
    colorScheme: "dark",
    reducedMotion: "reduce",
  });
  const pageErrors = [];
  const responseErrors = [];
  const expectedResponses = [];
  const responseChecks = [];
  try {
    const prefix = await startJob(fixture);
    const page = await context.newPage();
    page.on("pageerror", (error) => pageErrors.push(error.message));
    page.on("response", (response) => {
      const url = new URL(response.url());
      if (url.pathname.startsWith("/api/v1/") && response.status() >= 400) {
        const description = `${url.pathname} ${response.status()}`;
        if (
          url.origin === fixture.origin &&
          url.pathname === "/api/v1/jobs/current" &&
          !url.search &&
          response.status() === 404 &&
          response.request().method() === "GET" &&
          response.request().headers()["x-ocg-composer-context"]
        ) {
          responseChecks.push(
            response
              .json()
              .then((body) => {
                if (body?.error?.code === "not_found") expectedResponses.push(description);
                else responseErrors.push(description);
              })
              .catch(() => responseErrors.push(description)),
          );
        } else {
          responseErrors.push(description);
        }
      }
    });
    if (rate > 1) {
      const cdp = await context.newCDPSession(page);
      await cdp.send("Emulation.setCPUThrottlingRate", { rate });
    }
    const { js, rows } = await openConversation(page, fixture);
    const scroll = await measureScroll(page);
    if (keepAnchor) {
      await page.locator(viewport).evaluate((element) => {
        element.scrollTop = element.scrollHeight * 0.3;
      });
    }
    await sleep(250);
    await observe(page, prefix);
    await control(fixture, "release", "POST");
    const sent = await readSentTimes(directory, prefix, fixture.updateCount);
    const measurement = await finish(page, sent, fixture.updateCount);
    await Promise.all(responseChecks);
    const live = {
      ...measurement,
      pageErrors,
      responseErrors,
      expectedResponses,
    };
    console.log(
      `${prefix} ${rate}x, ${rows} messages, ${js.files} JS files, ${js.gzip} gzip bytes`,
      "\n  stream",
      summarize(live.delays),
      "\n  scroll",
      summarize(scroll.times),
      "\n  bottom gap",
      live.bottomGap,
      "anchor delta",
      live.offsetDelta,
    );
    return { prefix, rate, keepAnchor, cases: { rsbuild: { rows, js, scroll, live } } };
  } catch (error) {
    console.error("Browser errors", pageErrors, responseErrors);
    throw error;
  } finally {
    await context.close();
  }
}

async function run() {
  if (cpuRates.some((rate) => !Number.isFinite(rate) || rate < 1)) {
    throw new Error("CPU rates must be comma-separated numbers at least 1");
  }
  await access(join(uiDirectory, "index.html")).catch(() => {
    throw new Error("Build the frontend with pnpm --dir frontend build first");
  });
  await access(backendPath).catch(() => {
    throw new Error("Build OCG with cargo build --locked --bin ocg or set OCG_BENCH_BACKEND");
  });

  const directory = await mkdtemp(join(tmpdir(), "ocg-conversation-benchmark-"));
  const report = join(directory, "benchmark.json");
  console.log("Benchmark output", directory);
  await mkdir(join(directory, "project"));
  const child = fork(new URL("./fixture.mjs", import.meta.url), [
    directory,
    backendPath,
    uiDirectory,
  ]);
  const exited = once(child, "exit");
  try {
    const fixture = await Promise.race([
      once(child, "message").then(([message]) => message),
      exited.then(([code, signal]) => {
        throw new Error(`Fixture exited before it was ready (${code ?? signal})`);
      }),
    ]);
    const executablePath = process.env.OCG_BENCH_BROWSER;
    const browser = await chromium.launch({
      ...(executablePath ? { executablePath } : { channel: "chrome" }),
      headless: true,
      args: [
        "--disable-background-timer-throttling",
        "--disable-renderer-backgrounding",
        "--disable-backgrounding-occluded-windows",
      ],
    });
    try {
      console.log("Chrome", browser.version(), "fixture", fixture.turnCount + " turns");
      const observations = [];
      for (const [index, rate] of cpuRates.entries()) {
        const keepAnchor = index === cpuRates.length - 1 && cpuRates.length > 1;
        observations.push(await measure(browser, fixture, directory, rate, keepAnchor));
        await writeFile(report, JSON.stringify(observations, null, 2) + "\n");
        await sleep(350);
      }
      console.log("Benchmark report", report);
    } finally {
      await browser.close();
    }
  } finally {
    if (child.connected) child.disconnect();
    await exited;
  }
}

await run();
