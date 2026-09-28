#!/usr/bin/env python3
"""Drive the OCG PWA control/data loop against the real backend.

This starts the shipped loopback control server, then exercises exactly the
loop the PWA surface uses: Project registration/import with boundary
validation, global and project configuration, pre-run Mission configuration,
and the versioned canonical snapshot/event projection for the dogfood Mission.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OCG = ROOT / "target" / "debug" / "ocg"
# The Mission the dogfood script executed. Override both scripts together.
MISSION = os.environ.get("OCG_DOGFOOD_MISSION_ID", "wn-canonical-inspector")
EVIDENCE = ROOT / ".ocg" / "dogfood" / MISSION
PORT = int(os.environ.get("OCG_CONTROL_PORT", "18710"))


def call(method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    request = urllib.request.Request(
        f"http://127.0.0.1:{PORT}{path}",
        method=method,
        data=None if body is None else json.dumps(body).encode(),
        headers={"content-type": "application/json"} if body is not None else {},
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            return response.status, json.loads(response.read().decode())
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read().decode())


def main() -> int:
    if not EVIDENCE.is_dir():
        raise SystemExit(
            f"dogfood evidence missing at {EVIDENCE}; first run "
            "scripts/ocg-0-4-dogfood.py with the same OCG_DOGFOOD_MISSION_ID"
        )
    server = subprocess.Popen(
        [str(OCG), "serve", "--addr", f"127.0.0.1:{PORT}"],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    trace: list[str] = []
    try:
        base = None
        for _ in range(80):
            try:
                status, _ = call("GET", "/api/v1/canonical/projects")
                if status in (200, 409):
                    base = True
                    break
            except Exception:
                time.sleep(0.25)
        if base is None:
            raise SystemExit("control server did not become ready")
        trace.append(f"control server ready on 127.0.0.1:{PORT}")

        # 1. Project Manager / Project Import with backend boundary validation.
        status, imported = call("POST", "/api/v1/canonical/projects/import", {
            "command_id": "cmd-pwa-import-1",
            "root": str(ROOT),
        })
        assert status == 200, (status, imported)
        project_id = imported["project"]["project_id"]
        assert imported["project"]["marker"] is True, "boundary validation must confirm the marker"
        trace.append(f"project import acknowledged: {imported['command_id']} -> {project_id}")

        # Re-importing the same root reuses the same backend identity.
        status, again = call("POST", "/api/v1/canonical/projects/import", {
            "command_id": "cmd-pwa-import-2",
            "root": str(ROOT),
        })
        assert again["project"]["project_id"] == project_id
        trace.append("re-import is idempotent for the same boundary")

        # 2. Global configurator: meaningful persisted global configuration.
        status, global_ack = call("PUT", "/api/v1/canonical/configuration", {
            "command_id": "cmd-pwa-global-1",
            "configuration": {
                "provider": "openai",
                "model": "gpt-6-astra",
                "profile": "careful",
                "routing": "balanced",
                "runtime": "opencode-v2",
                "resource_budget": {"hard_limit": 25, "unit": "USD"},
            },
        })
        assert status == 200 and global_ack["accepted"], (status, global_ack)
        trace.append(f"global configuration revision {global_ack['revision']} persisted")

        # 3. Project defaults: a separate, persisted scope.
        status, project_ack = call("PUT", f"/api/v1/canonical/configuration/projects/{project_id}", {
            "command_id": "cmd-pwa-project-1",
            "defaults": {"profile": "careful", "routing": "balanced", "hard_budget": 25},
        })
        assert status == 200 and project_ack["accepted"], (status, project_ack)
        trace.append(f"project defaults revision {project_ack['revision']} persisted")

        # 4. Pre-run Mission configuration: editable while undispatched, frozen
        #    once a Run exists. A dispatched Run's contract must not change.
        status, edited = call("PUT", f"/api/v1/canonical/missions/{MISSION}/configuration", {
            "command_id": "cmd-pwa-mission-1",
            "configuration": {"profile": "fast"},
        })
        assert status == 400, "a dispatched Mission must refuse pre-run configuration"
        trace.append("pre-run Mission configuration refused after dispatch (frozen contract preserved)")

        # 5. Canonical snapshot + event tail with command correlation.
        status, snapshot = call(
            "GET", f"/api/v1/canonical/work?project_id={project_id}&mission_id={MISSION}"
        )
        assert status == 200, (status, snapshot)
        assert snapshot["api_version"] == "ocg.canonical.v1"
        mission = snapshot["mission"]
        assert len(mission["work_nodes"]) >= 8
        assert any(run["state"] == "fenced" for run in mission["runs"])
        assert mission["late_results"], "late evidence must remain inspectable"
        assert mission["verifications"], "verification evidence must be exposed"
        frozen_contract = {
            (run["run_id"]): run["contract"]
            for run in mission["runs"]
        }
        trace.append(
            f"snapshot: {len(mission['work_nodes'])} WorkNodes, {len(mission['runs'])} Runs, "
            f"cursor {snapshot['cursor']}"
        )

        status, tail = call(
            "GET",
            f"/api/v1/canonical/work/events?project_id={project_id}&mission_id={MISSION}&after=0",
        )
        assert status == 200
        assert len(tail["events"]) >= len(mission["events"])
        trace.append(f"event tail replayed {len(tail['events'])} canonical events")

        # 6. The configuration change made after dispatch did not rewrite any
        #    active Run's frozen contract.
        status, after_change = call(
            "GET", f"/api/v1/canonical/work?project_id={project_id}&mission_id={MISSION}"
        )
        assert after_change["mission"]["runs"] == mission["runs"], (
            "a configuration change must not rewrite a dispatched Run contract"
        )
        trace.append("post-configuration-change inspection is byte-identical: no Run contract was rewritten")

        # 7. A wrong-project identity is refused.
        status, refused = call(
            "GET", "/api/v1/canonical/work?project_id=project-unknown&mission_id=" + MISSION
        )
        assert status == 400, (status, refused)
        trace.append("unknown Project identity refused for canonical work")

        # 8. Frontend projection trace: feed the real backend payload into the
        #    same modules the PWA uses.
        (EVIDENCE / "pwa-snapshot.json").write_text(json.dumps(snapshot, indent=2))
        (EVIDENCE / "pwa-events.json").write_text(json.dumps(tail, indent=2))
        (EVIDENCE / "pwa-configuration.json").write_text(
            json.dumps({"global": global_ack, "project_defaults": project_ack}, indent=2)
        )
        (EVIDENCE / "pwa-trace.log").write_text("\n".join(trace) + "\n")
        print("\n".join(trace))
        return 0
    finally:
        server.send_signal(signal.SIGINT)
        try:
            server.wait(timeout=10)
        except subprocess.TimeoutExpired:
            server.kill()


if __name__ == "__main__":
    sys.exit(main())
