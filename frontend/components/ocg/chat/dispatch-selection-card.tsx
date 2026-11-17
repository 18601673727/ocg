"use client";

import { useEffect, useState } from "react";
import { Button } from "@/components/ui/button";
import { ScrollArea } from "@/components/ui/scroll-area";
import { OptionList } from "@/components/assistant-ui/elements/option-list";
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectLabel,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import type { ProfilesSnapshot, Project } from "../contracts";
import type { ApiClient } from "../runtime/api-client";
import { useI18n } from "../i18n";

type Selection = { agent: string | null; model: string; projects: string[] };

export function DispatchSelectionCard({
  client,
  arguments: argumentsText,
  disabled,
  onSelect,
}: {
  client: ApiClient;
  arguments: string;
  disabled: boolean;
  onSelect: (selection: Selection) => void;
}) {
  const { t } = useI18n();
  const [catalog, setCatalog] = useState<{
    profiles: ProfilesSnapshot;
    projects: Project[];
  } | null>(null);
  const [target, setTarget] = useState("");
  const [modelId, setModelId] = useState("");
  const [projects, setProjects] = useState<string[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [revision, setRevision] = useState(0);
  let request: {
    objective?: string;
    project?: string;
    projects?: string[];
    every_seconds?: number;
    keep_open?: boolean;
    depends_on?: unknown[];
  } = {};
  try {
    const parsed = JSON.parse(argumentsText);
    if (parsed && typeof parsed === "object") request = parsed;
  } catch {}
  useEffect(() => {
    const controller = new AbortController();
    void Promise.all([
      client.readProfiles(controller.signal),
      client.readAllProjects(controller.signal),
    ])
      .then(([profiles, projects]) => {
        if (controller.signal.aborted) return;
        setCatalog({ profiles, projects });
        setError(null);
        try {
          const request = JSON.parse(argumentsText);
          const suggested: unknown[] = Array.isArray(request.projects)
            ? request.projects
            : request.project
              ? [request.project]
              : [];
          setProjects(
            projects
              .filter((project) => suggested.includes(project.id))
              .map((project) => project.id),
          );
        } catch {
          setProjects([]);
        }
      })
      .catch((cause) => {
        if (!controller.signal.aborted)
          setError(cause instanceof Error ? cause.message : String(cause));
      });
    return () => controller.abort();
  }, [client, argumentsText, revision]);
  const models = (catalog?.profiles.llm_profiles ?? []).filter(
    (model) => model.enabled && model.declared_capabilities.includes("chat"),
  );
  const agents = (catalog?.profiles.agent_profiles ?? []).filter(
    (agent) =>
      (agent.capabilities.includes("chat") || agent.capabilities.includes("coding")) &&
      agent.llm_targets.some((id) => models.some((model) => model.id === id)),
  );
  const agent = agents.find((agent) => target === `agent:${agent.id}`);
  const direct = target.startsWith("model:")
    ? models.find((model) => target === `model:${model.id}`)
    : undefined;
  const choices = agent
    ? agent.llm_targets
        .map((id) => models.find((model) => model.id === id))
        .filter((model) => model !== undefined)
    : direct
      ? [direct]
      : [];
  const model = modelId ? choices.find((model) => model.id === modelId) : choices[0];
  const needsProject = Boolean(direct || agent?.capabilities.includes("coding"));
  const validProjects = projects.filter((id) =>
    catalog?.projects.some((project) => project.id === id),
  );
  return (
    <form
      className="flex flex-col gap-3 text-sm"
      onSubmit={(event) => {
        event.preventDefault();
        if (!model || (!agent && !direct)) return;
        onSelect({ agent: agent?.id ?? null, model: model.id, projects: validProjects });
      }}
    >
      <p className="text-muted-foreground">{t("chat.dispatch.waiting")}</p>
      <ScrollArea
        className="max-h-40 rounded-md bg-muted/40"
        viewportProps={{ className: "ocg-subtle-scrollbar" }}
        contentProps={{ className: "p-3" }}
      >
        <p className="whitespace-pre-wrap break-words">{request.objective}</p>
      </ScrollArea>
      {request.every_seconds && (
        <p className="text-xs text-muted-foreground">
          {t("chat.dispatch.recurrence", { seconds: request.every_seconds })}
        </p>
      )}
      {request.keep_open && (
        <p className="text-xs text-muted-foreground">{t("chat.dispatch.keepOpen")}</p>
      )}
      {Array.isArray(request.depends_on) && request.depends_on.length > 0 && (
        <p className="text-xs text-muted-foreground">
          {t("chat.dispatch.dependencies", { count: request.depends_on.length })}
        </p>
      )}
      {error && (
        <div role="alert">
          <p className="text-destructive">{error}</p>
          <Button
            type="button"
            size="xs"
            variant="outline"
            onClick={() => setRevision((value) => value + 1)}
          >
            {t("common.retry")}
          </Button>
        </div>
      )}
      {!catalog && !error && <p role="status">{t("common.loading")}</p>}
      <label className="flex flex-col gap-1">
        {t("chat.dispatch.target")}
        <Select
          value={target}
          items={[
            { value: "", label: t("chat.dispatch.choose") },
            ...agents.map((agent) => ({
              value: `agent:${agent.id}`,
              label: `${agent.name} · ${agent.capabilities
                .map((capability) => t(`settings.agent.capability.${capability}`))
                .join(" / ")}`,
            })),
          ]}
          disabled={disabled || !catalog}
          onValueChange={(next) => {
            if (next === null) return;
            setTarget(next);
            setModelId("");
          }}
        >
          <SelectTrigger className="min-h-10 w-full rounded-md border-border bg-background px-2">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="">{t("chat.dispatch.choose")}</SelectItem>
            <SelectGroup>
              <SelectLabel>{t("chat.agent")}</SelectLabel>
              {agents.map((agent) => (
                <SelectItem key={agent.id} value={`agent:${agent.id}`}>
                  {agent.name} ·{" "}
                  {agent.capabilities
                    .map((capability) => t(`settings.agent.capability.${capability}`))
                    .join(" / ")}
                </SelectItem>
              ))}
            </SelectGroup>
          </SelectContent>
        </Select>
      </label>
      {agent?.instructions && (
        <p
          className="line-clamp-3 whitespace-pre-wrap text-xs text-muted-foreground"
          title={agent.instructions}
        >
          {agent.instructions}
        </p>
      )}
      {model && (
        <label className="flex flex-col gap-1">
          {t("chat.model")}
          <Select
            value={model.id}
            items={choices.map((model) => ({
              value: model.id,
              label: `${model.name} · ${model.model}${
                agent?.llm_targets[0] === model.id ? ` (${t("settings.agent.defaultModel")})` : ""
              }`,
            }))}
            disabled={disabled}
            onValueChange={(next) => {
              if (next !== null) setModelId(next);
            }}
          >
            <SelectTrigger className="min-h-10 w-full rounded-md border-border bg-background px-2">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {choices.map((model) => (
                <SelectItem key={model.id} value={model.id}>
                  {model.name} · {model.model}
                  {agent?.llm_targets[0] === model.id
                    ? ` (${t("settings.agent.defaultModel")})`
                    : ""}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </label>
      )}
      <fieldset disabled={disabled} className="flex flex-col gap-2">
        <legend className="mb-2">{t("chat.project")}</legend>
        <ScrollArea
          className="max-h-56 rounded-md border border-border"
          viewportProps={{ className: "ocg-subtle-scrollbar" }}
        >
          <OptionList
            aria-label={t("chat.project")}
            className="max-w-none rounded-none border-0 bg-transparent dark:bg-transparent"
            selectionMode="multiple"
            selected={validProjects}
            maxSelections={16}
            onSelectionChange={setProjects}
            options={(catalog?.projects ?? []).map((project) => {
              const commands = project.commands.filter((command) => command.status === "allowed");
              return {
                id: project.id,
                label: project.name,
                description: (
                  <>
                    <span className="block break-all">{project.root}</span>
                    {t("chat.dispatch.commands", { count: commands.length })}
                    {commands.length > 0 &&
                      ` · ${commands.map((command) => command.name).join(", ")}`}
                  </>
                ),
              };
            })}
          />
        </ScrollArea>
        {needsProject && !validProjects.length && (
          <p className="text-xs text-muted-foreground">{t("chat.dispatch.projectRequired")}</p>
        )}
      </fieldset>
      <p className="text-xs text-muted-foreground">
        {t("chat.dispatch.jobCount", { count: validProjects.length || 1 })}
      </p>
      <Button
        type="submit"
        size="sm"
        disabled={
          disabled || !model || (!agent && !direct) || (needsProject && !validProjects.length)
        }
      >
        {t("chat.dispatch.submit")}
      </Button>
    </form>
  );
}
