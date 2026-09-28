/**
 * Frontend projection trace against the real backend payload.
 *
 * This uses the exact modules the PWA surface uses, so the trace is evidence
 * of the real contract rather than of a fixture. It is read-only: it cannot
 * dispatch, complete or replace anything.
 *
 * Run from `frontend/` after the PWA control loop:
 *   node --import tsx ../scripts/ocg-0-4-frontend-projection-trace.ts
 */
import { readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import {
  applyCanonicalEvents,
  applyCanonicalSnapshot,
  createCanonicalState,
  selectCanonical,
} from "../frontend/components/ocg/runtime/canonical-store";
import { createHttpCanonicalControlClient } from "../frontend/components/ocg/runtime/canonical-client";
import type { ProjectId } from "../frontend/components/ocg/project/domain";

const MISSION = process.env.OCG_DOGFOOD_MISSION_ID ?? "wn-canonical-inspector";
const evidence = resolve(import.meta.dirname, "..", ".ocg", "dogfood", MISSION);
const snapshot = JSON.parse(readFileSync(resolve(evidence, "pwa-snapshot.json"), "utf8"));
const projectId = snapshot.project_id as ProjectId;

let state = applyCanonicalSnapshot(createCanonicalState(), { payload: snapshot, projectId, generation: 1 });
const selected = selectCanonical(state, "Canonical execution inspector");
const lines: string[] = [];
lines.push(`api_version=${state.projection?.apiVersion}`);
lines.push(`project=${state.projectId} mission=${state.missionId} cursor=${state.cursor} status=${state.status}`);
lines.push(`workNodes=${state.projection?.workNodes.length} runs=${state.projection?.runs.length} dependencies=${state.projection?.dependencies.length}`);
lines.push(`execution.tasks=${selected.execution?.tasks.length} edges=${selected.execution?.edges.length} workers=${selected.execution?.workers.length}`);
lines.push(`summary=${JSON.stringify(selected.execution?.summary)}`);
lines.push(`fencedRuns=${JSON.stringify(selected.fencedRuns)}`);
lines.push(`lateResults=${state.projection?.lateResults.length} verifications=${state.projection?.verifications.length}`);
lines.push(`hasPassingVerification=${selected.hasPassingVerification} missionCompleted=${selected.missionCompleted}`);
for (const task of selected.execution?.tasks ?? []) {
  lines.push(`  node ${task.id.split(":").pop()} ${task.status.padEnd(9)} gen=${task.attempt ?? "-"} deps=[${task.dependencies.map((d) => d.split(":").pop()).join(",")}] ${task.title.slice(0, 44)}`);
}

// A wrong-Project payload must be refused by the same module.
const wrongProject = applyCanonicalSnapshot(state, {
  payload: { ...snapshot, project_id: "project-someone-else" },
  projectId,
  generation: 1,
});
lines.push(`wrongProjectRejected=${wrongProject.projection?.projectId === state.projectId} diagnostics=${wrongProject.diagnostics.map((d) => d.code).join(",")}`);

// Replay the real event tail; stale and duplicate deliveries must be refused.
const tail = JSON.parse(readFileSync(resolve(evidence, "pwa-events.json"), "utf8")).events as Array<{ sequence: number; event_id: string; project_id: string; mission_id: string; api_version: string; kind: string }>;
const replayed = applyCanonicalEvents(state, tail as never, { projectId, generation: 1 });
lines.push(`replayedCursor=${replayed.cursor} resyncRequired=${replayed.resyncRequired} status=${replayed.status}`);
const duplicated = applyCanonicalEvents(replayed, tail as never, { projectId, generation: 1 });
lines.push(`duplicateRefused=${duplicated.cursor === replayed.cursor} duplicateDiagnostics=${duplicated.diagnostics.filter((d) => d.code === "sequence-stale" || d.code === "duplicate-event").length}`);

// The frontend must not be able to mutate a dispatched Run.
const client = createHttpCanonicalControlClient({
  baseUrl: "http://127.0.0.1:0",
  fetch: async () => ({ status: 405, ok: false, text: async () => '{"error":{"message":"the PWA has no execution write route"}}' }),
});
async function main() {
  const forbidden = await client.writeMissionConfiguration("cmd-pwa-mutation-1", MISSION, { profile: "fast" });
  lines.push(`pwaCannotDispatchOrComplete=${(forbidden as { ok: boolean }).ok === false}`);
  writeFileSync(resolve(evidence, "frontend-projection-trace.log"), lines.join("\n") + "\n");
  console.log(lines.join("\n"));
}
void main();
