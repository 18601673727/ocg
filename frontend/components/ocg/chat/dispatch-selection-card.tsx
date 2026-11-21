import { createEffect, createMemo, createSignal } from "solid-js";
import { OptionList } from "@/components/ocg/chat/ui/option-list";
import { Button } from "@/components/ui/button";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Select, SelectItem } from "@/components/ui/select";
import type { ProfilesSnapshot, Project } from "../contracts";
import { useI18n } from "../i18n";
import type { ApiClient } from "../runtime/api-client";

type Selection = { agent: string | null; model: string; projects: string[] };

export function DispatchSelectionCard(props: {
  client: ApiClient;
  arguments: string;
  disabled: boolean;
  onSelect: (selection: Selection) => void;
}) {
  const i18n = useI18n();
  const [catalog, setCatalog] = createSignal<{
    profiles: ProfilesSnapshot;
    projects: Project[];
  } | null>(null);
  const [target, setTarget] = createSignal("");
  const [modelId, setModelId] = createSignal("");
  const [projects, setProjects] = createSignal<string[]>([]);
  const [error, setError] = createSignal<string | null>(null);
  const [revision, setRevision] = createSignal(0);
  let request: {
    objective?: string;
    project?: string;
    projects?: string[];
    every_seconds?: number;
    keep_open?: boolean;
    depends_on?: unknown[];
  } = {};
  try {
    const parsed = JSON.parse(props.arguments);
    if (parsed && typeof parsed === "object") request = parsed;
  } catch {}
  createEffect(revision, () => {
    const controller = new AbortController();
    void Promise.all([
      props.client.readProfiles(controller.signal),
      props.client.readAllProjects(controller.signal),
    ])
      .then(([profiles, projects]) => {
        if (controller.signal.aborted) return;
        setCatalog({ profiles, projects });
        setError(null);
        try {
          const request = JSON.parse(props.arguments);
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
  });
  const models = createMemo(() =>
    (catalog()?.profiles.llm_profiles ?? []).filter(
      (model) => model.enabled && model.declared_capabilities.includes("chat"),
    ),
  );
  const agents = createMemo(() =>
    (catalog()?.profiles.agent_profiles ?? []).filter(
      (agent) =>
        (agent.capabilities.includes("chat") || agent.capabilities.includes("coding")) &&
        agent.llm_targets.some((id) => models().some((model) => model.id === id)),
    ),
  );
  const agent = createMemo(() => agents().find((agent) => target() === `agent:${agent.id}`));
  const direct = createMemo(() =>
    target().startsWith("model:")
      ? models().find((model) => target() === `model:${model.id}`)
      : undefined,
  );
  const choices = createMemo(() =>
    agent()
      ? agent()!
          .llm_targets.map((id) => models().find((model) => model.id === id))
          .filter((model) => model !== undefined)
      : direct()
        ? [direct()]
        : [],
  );
  const model = createMemo(() =>
    modelId() ? choices().find((model) => model!.id === modelId()) : choices()[0],
  );
  const needsProject = createMemo(() =>
    Boolean(direct() || agent()?.capabilities.includes("coding")),
  );
  const validProjects = createMemo(() =>
    projects().filter((id) => catalog()?.projects.some((project) => project.id === id)),
  );
  return (
    <form
      class="flex flex-col gap-3 text-sm"
      onSubmit={(event) => {
        event.preventDefault();
        if (!model() || (!agent() && !direct())) return;
        props.onSelect({
          agent: agent()?.id ?? null,
          model: model()!.id,
          projects: validProjects(),
        });
      }}
    >
      <p class="text-muted-foreground">{i18n.t("chat.dispatch.waiting")}</p>
      <ScrollArea
        class="max-h-40 rounded-md bg-muted/40"
        viewportProps={{ class: "ocg-subtle-scrollbar" }}
        contentProps={{ class: "p-3" }}
      >
        <p class="whitespace-pre-wrap break-words">{request.objective}</p>
      </ScrollArea>
      {request.every_seconds && (
        <p class="text-xs text-muted-foreground">
          {i18n.t("chat.dispatch.recurrence", { seconds: request.every_seconds })}
        </p>
      )}
      {request.keep_open && (
        <p class="text-xs text-muted-foreground">{i18n.t("chat.dispatch.keepOpen")}</p>
      )}
      {Array.isArray(request.depends_on) && request.depends_on.length > 0 && (
        <p class="text-xs text-muted-foreground">
          {i18n.t("chat.dispatch.dependencies", { count: request.depends_on.length })}
        </p>
      )}
      {error() && (
        <div role="alert">
          <p class="text-destructive">{error()}</p>
          <Button
            type="button"
            size="xs"
            variant="outline"
            onClick={() => setRevision((value) => value + 1)}
          >
            {i18n.t("common.retry")}
          </Button>
        </div>
      )}
      {!catalog() && !error() && <p role="status">{i18n.t("common.loading")}</p>}
      <label class="flex flex-col gap-1">
        {i18n.t("chat.dispatch.target")}
        <Select
          value={target()}
          items={[
            { value: "", label: i18n.t("chat.dispatch.choose") },
            ...agents().map((agent) => ({
              value: `agent:${agent.id}`,
              label: `${agent.name} · ${agent.capabilities
                .map((capability) => i18n.t(`settings.agent.capability.${capability}`))
                .join(" / ")}`,
            })),
          ]}
          disabled={props.disabled || !catalog()}
          onValueChange={(next) => {
            if (next === null) return;
            setTarget(next);
            setModelId("");
          }}
          class="min-h-10 w-full rounded-md border-border bg-background px-2"
        >
          <SelectItem value="">{i18n.t("chat.dispatch.choose")}</SelectItem>
          <optgroup label={i18n.t("chat.agent")}>
            {agents().map((agent) => (
              <SelectItem value={`agent:${agent.id}`}>
                {agent.name} ·{" "}
                {agent.capabilities
                  .map((capability) => i18n.t(`settings.agent.capability.${capability}`))
                  .join(" / ")}
              </SelectItem>
            ))}
          </optgroup>
        </Select>
      </label>
      {agent()?.instructions && (
        <p
          class="line-clamp-3 whitespace-pre-wrap text-xs text-muted-foreground"
          title={agent()!.instructions!}
        >
          {agent()!.instructions}
        </p>
      )}
      {model() && (
        <label class="flex flex-col gap-1">
          {i18n.t("chat.model")}
          <Select
            value={model()!.id}
            items={choices().map((model) => ({
              value: model!.id,
              label: `${model!.name} · ${model!.model}${
                agent()?.llm_targets[0] === model!.id
                  ? ` (${i18n.t("settings.agent.defaultModel")})`
                  : ""
              }`,
            }))}
            disabled={props.disabled}
            onValueChange={(next) => {
              if (next !== null) setModelId(next);
            }}
            class="min-h-10 w-full rounded-md border-border bg-background px-2"
          >
            {choices().map((model) => (
              <SelectItem value={model!.id}>
                {model!.name} · {model!.model}
                {agent()?.llm_targets[0] === model!.id
                  ? ` (${i18n.t("settings.agent.defaultModel")})`
                  : ""}
              </SelectItem>
            ))}
          </Select>
        </label>
      )}
      <fieldset disabled={props.disabled} class="flex flex-col gap-2">
        <legend class="mb-2">{i18n.t("chat.project")}</legend>
        <ScrollArea
          class="max-h-56 rounded-md border border-border"
          viewportProps={{ class: "ocg-subtle-scrollbar" }}
        >
          <OptionList
            aria-label={i18n.t("chat.project")}
            class="max-w-none rounded-none border-0 bg-transparent dark:bg-transparent"
            selectionMode="multiple"
            selected={validProjects()}
            maxSelections={16}
            onSelectionChange={setProjects}
            options={(catalog()?.projects ?? []).map((project) => {
              const commands = project.commands.filter((command) => command.status === "allowed");
              return {
                id: project.id,
                label: project.name,
                description: (
                  <>
                    <span class="block break-all">{project.root}</span>
                    {i18n.t("chat.dispatch.commands", { count: commands.length })}
                    {commands.length > 0 &&
                      ` · ${commands.map((command) => command.name).join(", ")}`}
                  </>
                ),
              };
            })}
          />
        </ScrollArea>
        {needsProject() && !validProjects().length && (
          <p class="text-xs text-muted-foreground">{i18n.t("chat.dispatch.projectRequired")}</p>
        )}
      </fieldset>
      <p class="text-xs text-muted-foreground">
        {i18n.t("chat.dispatch.jobCount", { count: validProjects().length || 1 })}
      </p>
      <Button
        type="submit"
        size="sm"
        disabled={
          props.disabled ||
          !model() ||
          (!agent() && !direct()) ||
          (needsProject() && !validProjects().length)
        }
      >
        {i18n.t("chat.dispatch.submit")}
      </Button>
    </form>
  );
}
