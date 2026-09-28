#!/usr/bin/env python3
"""Execute the OCG 0.4 dogfood Mission through the real production path.

Every step uses the shipped `ocg work` control surface, which is the same
canonical substrate the generated plugin bridge uses. Nothing here writes
execution state directly: the script only issues commands and records what the
durable store reported.

The Mission is the section 10 gate:

    root: define and implement the canonical execution inspector
      A : trace the substrate and define the projection   (depends on nothing)
        A1: inspect WorkNode/Run/dependency/event invariants
      B : implement backend canonical inspection          (depends on A)
        B1: witness-aware Run history and late-result display
      C : implement canonical event/snapshot contract fixtures (depends on A)
      D : connect RuntimeStore and Execution Graph projection (depends on B, C)
        D1: exercise canonical execution visualization
      E : verify, recover and document the result          (depends on D)
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OCG = ROOT / "target" / "debug" / "ocg"
# A new Mission id makes repeated dogfood runs non-destructive. The documented
# default preserves the committed fixture's stable witness identities; select
# a new suffix for another run in a repository with existing canonical state.
MISSION = os.environ.get("OCG_DOGFOOD_MISSION_ID", "wn-canonical-inspector")
SECOND_MISSION = f"{MISSION}-preflight"
LEAD_SESSION = f"dogfood-lead-{MISSION}"
EVIDENCE = ROOT / ".ocg" / "dogfood" / MISSION

TRANSCRIPT: list[str] = []


def run(args: list[str], *, expect_fail: bool = False) -> subprocess.CompletedProcess:
    process = subprocess.run(
        [str(OCG), *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    TRANSCRIPT.append("$ ocg " + " ".join(args))
    if process.stdout.strip():
        TRANSCRIPT.append(process.stdout.strip())
    if process.stderr.strip():
        TRANSCRIPT.append("[stderr] " + process.stderr.strip())
    if expect_fail:
        if process.returncode == 0:
            raise SystemExit(f"expected a fail-closed result: ocg {' '.join(args)}")
    elif process.returncode != 0:
        raise SystemExit(f"command failed: ocg {' '.join(args)}\n{process.stderr}")
    return process


def work(sub: str, *args: str, expect_fail: bool = False) -> dict:
    process = run(["work", sub, *args], expect_fail=expect_fail)
    text = process.stdout.strip() or "{}"
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {"raw": text}


def m(*args: str) -> str:
    return f"--mission={MISSION}"


def real_verification(stage: str) -> tuple[bool, str]:
    """Run the project's own trusted verification stage and report its verdict."""
    process = subprocess.run(
        [str(OCG), "verify", stage],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    passed = f"verification stage: {stage} (passed)" in process.stdout
    return passed, process.stdout.strip().splitlines()[0] if process.stdout.strip() else ""


def verify(witness: dict, stage: str, *, force_fail: str | None = None) -> dict:
    """Record verification evidence for one dispatch.

    Normal case: the CLI runs the project's own trusted stage and records the
    runner's verdict. The script never supplies one.

    `force_fail` is only used for the one deliberate replacement scenario, and
    it is stored as an explicit operator assertion rather than as a trusted
    command result, so it can never read as a passing verification.
    """
    if force_fail is not None:
        report = {"source": "ocg work verify", "evidence": "operator-assertion",
                  "forced_failure": force_fail}
        return work("verify", f"--witness={json.dumps(witness)}",
                    "--outcome=failed", f"--report={json.dumps(report)}")
    passed, headline = real_verification(stage)
    if not passed:
        raise SystemExit(
            f"trusted verification stage '{stage}' did not pass: {headline}"
        )
    return work("verify", f"--witness={json.dumps(witness)}", f"--stage={stage}")


def deliver(witness: dict, result: str, outcome: str = "completed") -> dict:
    return work("deliver", f"--witness={json.dumps(witness)}", f"--result={result}",
                f"--outcome={outcome}")


def main() -> int:
    if not OCG.is_file():
        raise SystemExit("build the CLI first: cargo build")
    if not re.fullmatch(r"wn-[a-z0-9-]{1,120}", MISSION):
        raise SystemExit("OCG_DOGFOOD_MISSION_ID must be a safe wn- Mission id")
    if len(SECOND_MISSION) > 128:
        raise SystemExit("OCG_DOGFOOD_MISSION_ID is too long for a preflight Mission")
    # Never delete, reset or overwrite the canonical store: it may contain
    # live Missions unrelated to this dogfood run. Probe both IDs *before*
    # creating any WorkNode, and abort on every storage error other than an
    # unknown Mission. A repeated run must choose a new Mission id explicitly.
    for mission_id in (MISSION, SECOND_MISSION):
        probe = subprocess.run(
            [str(OCG), "work", "inspect", f"--mission={mission_id}"],
            cwd=ROOT, capture_output=True, text=True,
        )
        if probe.returncode == 0:
            raise SystemExit(
                f"Mission {mission_id} already exists; canonical history was preserved. "
                "Use OCG_DOGFOOD_MISSION_ID=wn-canonical-inspector-<new-suffix> to run again."
            )
        if "unknown canonical Mission" not in probe.stderr:
            raise SystemExit(f"cannot inspect canonical store safely: {probe.stderr.strip()}")
    if EVIDENCE.exists():
        raise SystemExit(f"evidence already exists at {EVIDENCE}; choose a new Mission id")
    EVIDENCE.mkdir(parents=True)

    # ---- root identity and boundary -------------------------------------
    root = work("admit", m(), f"--session={LEAD_SESSION}", "--agent=lead-high",
                "--model=openai/gpt-6-astra", "--role=lead",
                "--objective=define and implement the canonical execution inspector")
    (EVIDENCE / "root-witness.json").write_text(json.dumps(root, indent=2))
    TRANSCRIPT.append(f"root dispatch: {json.dumps(root)}")

    # Pre-run configuration is frozen the moment the root Run is dispatched.
    work("configure", m(), '--config={"profile":"fast"}', expect_fail=True)
    TRANSCRIPT.append("pre-run configuration after the dogfood root dispatch: fail-closed as expected")

    # An undispatched Mission (root WorkNode, no Run) still accepts pre-run
    # configuration; dispatching its root Run freezes the same command.
    created = work("create", f"--mission={SECOND_MISSION}",
                   "--objective=second mission used to prove pre-run configuration is frozen after dispatch")
    pre_run = work("configure", f"--mission={SECOND_MISSION}",
                   '--config={"profile":"careful","routing":"balanced","hard_budget":25}')
    TRANSCRIPT.append(
        f"pre-run configuration on an undispatched Mission: created={json.dumps(created)} config={json.dumps(pre_run)}"
    )

    # The second Mission admits and dispatches so the freeze is proven on both.
    work("start", f"--mission={SECOND_MISSION}", f"--session={LEAD_SESSION}-preflight",
         "--agent=lead-high", "--model=openai/gpt-6-astra", "--role=lead")
    work("configure", f"--mission={SECOND_MISSION}", '--config={"profile":"fast"}', expect_fail=True)
    TRANSCRIPT.append("pre-run configuration after that Mission's dispatch: fail-closed as expected")

    before = work("inspect", m())
    (EVIDENCE / "snapshot-before.json").write_text(json.dumps(before, indent=2))

    # ---- A and its nested child A1 --------------------------------------
    node_a = work("plan", m(), "--node=0", "--run=0",
                  "--objective=A: trace current substrate/controller and define the projection")["node_id"]
    wit_a = work("dispatch", m(), f"--node={node_a}", "--run=0",
                 "--runtime-execution=session-dogfood-a", "--agent=ocg-explore",
                 "--model=openai/gpt-6-astra", "--role=explore")
    # Recursive depth: A's own Run creates A1.
    node_a1 = work("plan", m(), f"--node={node_a}", f"--run={wit_a['run_id']}",
                   "--objective=A1: inspect WorkNode/Run/dependency/event invariants")["node_id"]
    wit_a1 = work("dispatch", m(), f"--node={node_a1}", f"--run={wit_a['run_id']}",
                  "--runtime-execution=session-dogfood-a1", "--agent=ocg-explore",
                  "--model=openai/gpt-6-astra", "--role=explore")
    work("bind", f"--witness={json.dumps(wit_a1)}", "--host-session=host-session-a1")
    verify(wit_a1, "fast")
    deliver(wit_a1, "A1: WorkNode/Run/dependency/event invariants inspected")
    verify(wit_a, "normal")
    deliver(wit_a, "A: projection contract defined and traced")

    # ---- B and C become independently ready after the shared prerequisite -
    node_b = work("plan", m(), "--node=0", "--run=0", f"--depends-on={node_a}",
                  "--objective=B: implement backend canonical inspection")["node_id"]
    node_c = work("plan", m(), "--node=0", "--run=0", f"--depends-on={node_a}",
                  "--objective=C: implement canonical event/snapshot contract fixtures")["node_id"]
    ready = work("recover", m())
    TRANSCRIPT.append(f"ready set after A: {json.dumps(ready['ready'])} (B={node_b}, C={node_c})")
    assert [item["node_id"] for item in ready["ready"]] == [node_b, node_c], "B and C must be independently ready"

    wit_b = work("dispatch", m(), f"--node={node_b}", "--run=0",
                 "--runtime-execution=session-dogfood-b", "--agent=ocg-build",
                 "--model=openai/gpt-6-astra", "--role=build")
    wit_c = work("dispatch", m(), f"--node={node_c}", "--run=0",
                 "--runtime-execution=session-dogfood-c", "--agent=ocg-verify",
                 "--model=openai/gpt-6-astra", "--role=verify")
    work("bind", f"--witness={json.dumps(wit_b)}", "--host-session=host-session-b1")
    work("bind", f"--witness={json.dumps(wit_c)}", "--host-session=host-session-c")

    # Restart between dispatch and completion: a fresh process reads the durable
    # pending dispatch and the exact witness, with no chat-text reconstruction.
    recovered = work("recover", m())
    (EVIDENCE / "recovery-after-restart.json").write_text(json.dumps(recovered, indent=2))
    assert any(item["dispatch_id"] == wit_b["dispatch_id"] for item in recovered["pending_dispatches"])
    TRANSCRIPT.append("restart recovery: B's witness recovered from durable state")

    verify(wit_c, "fast")
    deliver(wit_c, "C: canonical event/snapshot contract fixtures implemented")

    # ---- Worker Run Replacement plus a stale late result -----------------
    verify(wit_b, "normal", force_fail="simulated provider loss on the first generation")
    deliver(wit_b, "provider lost mid-run", outcome="failed")
    wit_b2 = work("replace", m(), f"--node={node_b}", f"--run={wit_b['run_id']}",
                  "--runtime-execution=session-dogfood-b2", "--agent=ocg-build",
                  "--model=openai/gpt-6-astra", "--role=build")
    assert wit_b2["run_generation"] == wit_b["run_generation"] + 1
    # The fenced generation's late result must be retained as evidence only.
    late = deliver(wit_b, "stale success from the fenced generation")
    assert late["disposition"] == "late_evidence", late
    assert late["evidence_only"] is True
    TRANSCRIPT.append("stale delivery from the fenced generation: retained as evidence, not applied")

    # B1 is created by the replacement Run, proving recursive ownership survives.
    node_b1 = work("plan", m(), f"--node={node_b}", f"--run={wit_b2['run_id']}",
                   "--objective=B1: implement witness-aware Run history and late-result display")["node_id"]
    wit_b1 = work("dispatch", m(), f"--node={node_b1}", f"--run={wit_b2['run_id']}",
                  "--runtime-execution=session-dogfood-b1", "--agent=ocg-build",
                  "--model=openai/gpt-6-astra", "--role=build")
    work("bind", f"--witness={json.dumps(wit_b1)}", "--host-session=host-session-b1-child")
    verify(wit_b1, "normal")
    deliver(wit_b1, "B1: witness-aware Run history and late-result display implemented")
    verify(wit_b2, "normal")
    deliver(wit_b2, "B: backend canonical inspection implemented on the replacement generation")

    # ---- D and its nested child D1 (reconvergence) -----------------------
    node_d = work("plan", m(), "--node=0", "--run=0", f"--depends-on={node_b},{node_c}",
                  "--objective=D: connect RuntimeStore and Execution Graph projection")["node_id"]
    wit_d = work("dispatch", m(), f"--node={node_d}", "--run=0",
                 "--runtime-execution=session-dogfood-d", "--agent=ocg-build",
                 "--model=openai/gpt-6-astra", "--role=build")
    node_d1 = work("plan", m(), f"--node={node_d}", f"--run={wit_d['run_id']}",
                   "--objective=D1: exercise canonical execution visualization")["node_id"]
    wit_d1 = work("dispatch", m(), f"--node={node_d1}", f"--run={wit_d['run_id']}",
                  "--runtime-execution=session-dogfood-d1", "--agent=ocg-docs",
                  "--model=openai/gpt-6-astra", "--role=docs")
    work("bind", f"--witness={json.dumps(wit_d1)}", "--host-session=host-session-d1")
    verify(wit_d1, "fast")
    deliver(wit_d1, "D1: canonical execution visualization exercised")
    verify(wit_d, "normal")
    deliver(wit_d, "D: RuntimeStore and Execution Graph projection connected")

    # ---- E: verify, recover, document ------------------------------------
    node_e = work("plan", m(), "--node=0", "--run=0", f"--depends-on={node_d}",
                  "--objective=E: verify, recover and document the result")["node_id"]
    wit_e = work("dispatch", m(), f"--node={node_e}", "--run=0",
                 "--runtime-execution=session-dogfood-e", "--agent=ocg-verify",
                 "--model=openai/gpt-6-astra", "--role=verify")
    work("bind", f"--witness={json.dumps(wit_e)}", "--host-session=host-session-e")
    verification = verify(wit_e, "normal")
    (EVIDENCE / "verification-e.json").write_text(json.dumps(verification, indent=2))
    deliver(wit_e, "E: canonical execution inspector verified, recovered and documented")

    # ---- terminal Mission -------------------------------------------------
    root_witness = json.loads((EVIDENCE / "root-witness.json").read_text())
    verify(root_witness, "normal")
    deliver(root_witness, "canonical execution inspector shipped")
    terminated = work("terminate", m(), "--state=completed")
    (EVIDENCE / "terminate.json").write_text(json.dumps(terminated, indent=2))

    after = work("inspect", m())
    (EVIDENCE / "snapshot-after.json").write_text(json.dumps(after, indent=2))
    (EVIDENCE / "transcript.log").write_text("\n".join(TRANSCRIPT) + "\n")

    # ---- assertions on durable state --------------------------------------
    nodes = after["work_nodes"]
    assert len(nodes) >= 8, f"expected at least eight WorkNodes, found {len(nodes)}"
    assert after["root_node_id"] == 0
    assert all(node["state"] == "completed" for node in nodes), "every WorkNode must be completed"
    assert after["lifecycle"]["state"] == "completed", after["lifecycle"]
    assert after["lifecycle"]["completed_at"] is not None
    assert any(edge["node_id"] == node_b for edge in after["dependencies"])
    assert any(edge["node_id"] == node_c for edge in after["dependencies"])
    assert any(edge["node_id"] == node_d for edge in after["dependencies"])
    fenced = [run_ for run_ in after["runs"] if run_["state"] == "fenced"]
    assert fenced, "the replaced generation must remain fenced"
    assert after["late_results"], "the late result must be retained as evidence"
    assert after["verifications"], "verification evidence must be durable"
    generations = {}
    for run_ in after["runs"]:
        generations.setdefault(run_["node_id"], set()).add(run_["generation"])
    assert max(len(value) for value in generations.values()) >= 2, "a replacement must create a new generation"
    print(json.dumps({
        "mission_id": MISSION,
        "work_nodes": len(nodes),
        "runs": len(after["runs"]),
        "fenced_runs": len(fenced),
        "late_results": len(after["late_results"]),
        "verifications": len(after["verifications"]),
        "events": len(after["events"]),
        "lifecycle": after["lifecycle"]["state"],
        "evidence": str(EVIDENCE),
    }, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
