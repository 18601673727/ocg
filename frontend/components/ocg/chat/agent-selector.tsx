"use client";

import { useEffect, useId, useMemo, useState } from "react";

import { Button } from "@/components/ui/button";
import type { ControlAgentProfile } from "@/components/ocg/contracts";
import {
  ControlTransportError,
  type HttpCanonicalControlClient,
} from "@/components/ocg/runtime/canonical-client";
import type { SubmissionTarget } from "@/components/ocg/job/job-submission-form";

/** Pages of Agent Profiles read at most; a longer list says it is partial. */
const MAX_PAGES = 10;

type Loaded =
  | { status: "ready"; agents: ControlAgentProfile[]; partial: boolean }
  | { status: "error"; code: string };

const selectClass =
  "h-11 w-full min-w-0 rounded-md border border-border bg-background px-2 text-base text-foreground outline-none focus-visible:border-ring focus-visible:ring-2 focus-visible:ring-ring/20 disabled:cursor-not-allowed disabled:opacity-60 sm:h-9 sm:text-sm";

/**
 * An LLM target a message can be sent with: it declares Chat, and some
 * Executor can run it now. Both are persisted facts the Control Plane reports.
 */
function usable(target: ControlAgentProfile["llm_targets"][number]): boolean {
  return target.declared_capabilities.includes("chat") && target.executors.length > 0;
}

/**
 * Chooses the Agent Profile a message is sent as, one of its LLM targets and
 * an Executor, from what `GET /api/v1/agent-profiles` reports. Nothing is
 * remembered or defaulted by the browser beyond the first usable choice, and
 * a message is only offered a target that exists.
 */
export function AgentSelector({
  client,
  onChange,
}: {
  client: HttpCanonicalControlClient;
  onChange: (target: SubmissionTarget | null) => void;
}) {
  const id = useId();
  const [attempt, setAttempt] = useState(0);
  const [loaded, setLoaded] = useState<Loaded | null>(null);
  const [agentChoice, setAgentChoice] = useState("");
  const [targetChoice, setTargetChoice] = useState("");
  const [executorChoice, setExecutorChoice] = useState("");

  useEffect(() => {
    const controller = new AbortController();
    (async () => {
      try {
        const agents: ControlAgentProfile[] = [];
        let before: string | undefined;
        let partial = false;
        for (let page = 0; ; page++) {
          const answer = await client.readAgentProfiles({ before, limit: 100, signal: controller.signal });
          agents.push(...answer.agent_profiles);
          if (!answer.has_more) break;
          if (page + 1 >= MAX_PAGES) {
            partial = true;
            break;
          }
          before = answer.next_before ?? undefined;
        }
        if (!controller.signal.aborted) setLoaded({ status: "ready", agents, partial });
      } catch (error) {
        if (controller.signal.aborted) return;
        setLoaded({ status: "error", code: error instanceof ControlTransportError ? error.code : "unreachable" });
      }
    })();
    return () => controller.abort();
  }, [client, attempt]);

  const agents = useMemo(
    () => (loaded?.status === "ready" ? loaded.agents.filter((agent) => agent.llm_targets.some(usable)) : []),
    [loaded],
  );
  const agent = agents.find((candidate) => candidate.id === agentChoice) ?? agents[0];
  const targets = agent ? agent.llm_targets.filter(usable) : [];
  const llm = targets.find((candidate) => candidate.id === targetChoice) ?? targets[0];
  const executor = llm?.executors.find((candidate) => candidate === executorChoice) ?? llm?.executors[0];
  const choice =
    agent && llm && executor ? { agentProfile: agent.id, llmProfile: llm.id, executor } : null;
  const agentProfile = choice?.agentProfile;
  const llmProfile = choice?.llmProfile;
  const executorId = choice?.executor;

  useEffect(() => {
    onChange(
      agentProfile && llmProfile && executorId
        ? { agentProfile, llmProfile, executor: executorId }
        : null,
    );
  }, [agentProfile, llmProfile, executorId, onChange]);

  const unusable = loaded?.status === "ready" ? loaded.agents.length - agents.length : 0;

  return (
    <section aria-labelledby={`${id}-title`} className="flex flex-col gap-3 rounded-lg border border-border bg-card p-4 text-card-foreground">
      <div className="flex items-center justify-between gap-2">
        <h2 id={`${id}-title`} className="text-xs font-semibold tracking-widest uppercase text-muted-foreground">
          Agent
        </h2>
        <Button
          variant="ghost"
          size="xs"
          className="tracking-normal normal-case"
          disabled={loaded === null}
          onClick={() => {
            setLoaded(null);
            setAttempt((current) => current + 1);
          }}
        >
          Refresh
        </Button>
      </div>
      {loaded === null ? <p className="text-sm text-muted-foreground">Loading Agent Profiles…</p> : null}
      {loaded?.status === "error" ? (
        <p role="alert" className="text-sm text-destructive">
          Agent Profiles could not be read ({loaded.code}).
        </p>
      ) : null}
      {loaded?.status === "ready" && agents.length === 0 ? (
        <p className="text-sm text-muted-foreground">
          {loaded.agents.length === 0
            ? "No Agent Profile is configured. `ocg register-llm` registers one for its LLM target."
            : "No Agent Profile has an LLM target that declares Chat and has a compatible Executor."}
        </p>
      ) : null}
      {agent ? (
        <fieldset className="grid min-w-0 grid-cols-1 gap-2 sm:grid-cols-12">
          <legend className="sr-only">Agent selection</legend>
          <label className="flex min-w-0 flex-col gap-1 text-xs text-muted-foreground sm:col-span-4">
            Agent Profile
            <select
              aria-label="Agent Profile"
              value={agent.id}
              onChange={(event) => {
                setAgentChoice(event.target.value);
                setTargetChoice("");
                setExecutorChoice("");
              }}
              className={selectClass}
            >
              {agents.map((option) => (
                <option key={option.id} value={option.id}>
                  {option.name} ({option.id})
                </option>
              ))}
            </select>
          </label>
          <label className="flex min-w-0 flex-col gap-1 text-xs text-muted-foreground sm:col-span-5">
            Model
            <select
              aria-label="LLM target"
              value={llm?.id ?? ""}
              onChange={(event) => {
                setTargetChoice(event.target.value);
                setExecutorChoice("");
              }}
              className={selectClass}
            >
              {targets.map((option) => (
                <option key={option.id} value={option.id}>
                  {option.model} ({option.id})
                </option>
              ))}
            </select>
          </label>
          <label className="flex min-w-0 flex-col gap-1 text-xs text-muted-foreground sm:col-span-3">
            Executor
            <select
              aria-label="Executor"
              value={executor ?? ""}
              onChange={(event) => setExecutorChoice(event.target.value)}
              className={selectClass}
            >
              {(llm?.executors ?? []).map((option) => (
                <option key={option} value={option}>
                  {option}
                </option>
              ))}
            </select>
          </label>
        </fieldset>
      ) : null}
      {unusable > 0 ? (
        <p className="text-xs text-muted-foreground">
          {unusable} Agent Profile{unusable === 1 ? "" : "s"} not offered: no LLM target declares Chat with a compatible Executor.
        </p>
      ) : null}
      {loaded?.status === "ready" && loaded.partial ? (
        <p className="text-xs text-muted-foreground">
          More Profiles exist than this list reads; it is partial.
        </p>
      ) : null}
    </section>
  );
}
