import { Check, Copy } from "@lucide/solid";
import { createEffect, createMemo, createSignal } from "solid-js";
import type { Project } from "@/components/ocg/contracts";
import { Pill } from "@/components/ocg/primitives";
import { DismissibleNotice } from "@/components/ocg/primitives/dismissible-notice";
import type {
  ApiClient,
  ProjectCommand,
  ProjectCreate,
  ProjectUpdate,
} from "@/components/ocg/runtime/api-client";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import { useCopyToClipboard } from "@/hooks/use-copy-to-clipboard";
import { type FormEvent } from "@/lib/ui-state";
import { type I18nKey, useI18n } from "../i18n";
import { type CommandDraft, toCommandDrafts, toProjectCommands } from "./command-drafts";
import { CommandEditor } from "./command-editor";
import { ProjectDecisionConfigButton } from "./command-management";
import { describeError, refusalText, Section } from "./settings-parts";

const PROJECT_REFUSALS: Partial<Record<string, I18nKey>> = {
  project_root_invalid: "settings.projects.refusal.rootInvalid",
  project_root_overlaps: "settings.projects.refusal.rootOverlaps",
  invalid_commands: "settings.projects.refusal.invalidCommands",
  project_not_found: "settings.projects.refusal.notFound",
  invalid_request: "settings.projects.refusal.invalidRequest",
};

function CopyRootButton(props: { project: Project }) {
  const i18n = useI18n();
  const { isCopied, isCopyFailed, copyToClipboard } = useCopyToClipboard();

  const label = isCopied()
    ? i18n.t("common.copied")
    : isCopyFailed()
      ? i18n.t("common.copyFailed")
      : i18n.t("common.copy");

  return (
    <Button
      type="button"
      variant="ghost"
      size="xs"
      class="shrink-0 px-1.5 tracking-normal normal-case"
      aria-label={i18n.t("settings.projects.copyRootFor", { name: props.project.name })}
      onClick={() => copyToClipboard(props.project.root)}
    >
      {isCopied() ? <Check aria-hidden="true" /> : <Copy aria-hidden="true" />}
      <span role="status">{label}</span>
    </Button>
  );
}

export function ProjectsSection(props: { client: ApiClient }) {
  const i18n = useI18n();
  const [projects, setProjects] = createSignal<Project[]>([]);
  const [loadError, setLoadError] = createSignal<string | null>(null);

  const [editingProject, setEditingProject] = createSignal<Project | null>(null);
  const [name, setName] = createSignal("");
  const [root, setRoot] = createSignal("");
  const [description, setDescription] = createSignal("");
  const [commands, setCommands] = createSignal<CommandDraft[]>([]);
  const [saving, setSaving] = createSignal(false);
  const [error, setError] = createSignal<string | null>(null);
  const [notice, setNotice] = createSignal<string | null>(null);

  const loadProjects = (signal?: AbortSignal) =>
    props.client
      .readAllProjects(signal)
      .then((loaded) => {
        if (!signal?.aborted) {
          setProjects(loaded);
          setLoadError(null);
        }
      })
      .catch((err: unknown) => {
        if (!signal?.aborted) setLoadError(describeError(err, i18n.t));
      });

  createEffect(
    () => [loadProjects],
    () => {
      const controller = new AbortController();
      void loadProjects(controller.signal);
      return () => controller.abort();
    },
  );

  const managementChanged = (updated: Project) => {
    setProjects((previous) =>
      previous.map((project) => (project.id === updated.id ? updated : project)),
    );
    setEditingProject(updated);
    setCommands(toCommandDrafts(updated.commands));
  };
  const commandsDirty = createMemo(() =>
    editingProject()
      ? JSON.stringify(toProjectCommands(commands())) !==
        JSON.stringify(toProjectCommands(toCommandDrafts(editingProject()!.commands)))
      : true,
  );

  function resetForm() {
    setEditingProject(null);
    setName("");
    setRoot("");
    setDescription("");
    setCommands([]);
    setError(null);
  }

  function startEdit(project: Project) {
    if (editingProject()?.id === project.id) {
      resetForm();
      return;
    }
    setEditingProject(project);
    setName(project.name);
    setRoot(project.root);
    setDescription(project.description ?? "");
    setCommands(toCommandDrafts(project.commands));
    setError(null);
    setNotice(null);
  }

  async function handleSave(e: FormEvent) {
    e.preventDefault();
    setSaving(true);
    setError(null);
    setNotice(null);

    const trimmedDesc = description().trim();
    const finalDescription = trimmedDesc.length > 0 ? trimmedDesc : null;
    const finalCommands: ProjectCommand[] = toProjectCommands(commands());

    try {
      if (editingProject()) {
        const payload: ProjectUpdate = {
          name: name().trim(),
          description: finalDescription,
          commands: finalCommands,
        };
        const updated = await props.client.updateProject(editingProject()!.id, payload);
        setProjects((prev) => prev.map((p) => (p.id === updated.id ? updated : p)));
        resetForm();
        setNotice(i18n.t("settings.projects.updatedNotice"));
      } else {
        const payload: ProjectCreate = {
          name: name().trim(),
          root: root().trim(),
          description: finalDescription,
          commands: finalCommands,
        };
        const created = await props.client.createProject(payload);
        setProjects((prev) => [created, ...prev]);
        resetForm();
        setNotice(i18n.t("settings.projects.createdNotice"));
      }
    } catch (err) {
      setError(refusalText(i18n.t, err, PROJECT_REFUSALS));
    } finally {
      setSaving(false);
    }
  }

  return (
    <Section
      id="settings-projects"
      title={i18n.t("settings.projects.title")}
      description={i18n.t("settings.projects.description")}
    >
      {loadError() ? (
        <DismissibleNotice message={loadError()!} class="py-2 text-sm text-destructive">
          {loadError()}
        </DismissibleNotice>
      ) : null}

      <form
        onSubmit={(e) => void handleSave(e)}
        class="mb-2 flex flex-col gap-3 border-b border-border pb-4"
      >
        <div class="flex items-center justify-between gap-2">
          <h3 class="text-sm font-semibold">
            {editingProject()
              ? i18n.t("settings.projects.editTitle", { id: editingProject()!.id })
              : i18n.t("settings.projects.createTitle")}
          </h3>
          {editingProject() ? (
            <Button type="button" variant="ghost" size="xs" onClick={resetForm}>
              {i18n.t("settings.projects.cancelEdit")}
            </Button>
          ) : null}
        </div>

        <div class="grid grid-cols-1 gap-3 sm:grid-cols-2">
          <label class="flex flex-col gap-1 text-xs font-medium text-muted-foreground">
            {i18n.t("settings.projects.name")}
            <Input
              value={name()}
              onInput={(e) => setName(e.target.value)}
              placeholder={i18n.t("settings.projects.namePlaceholder")}
              spellcheck="false"
            />
          </label>

          {editingProject() ? (
            <div class="flex flex-col gap-1 text-xs font-medium text-muted-foreground">
              <span>{i18n.t("settings.projects.root")}</span>
              <span class="font-mono text-xs text-foreground py-2 break-all">
                {editingProject()!.root}
              </span>
            </div>
          ) : (
            <label class="flex flex-col gap-1 text-xs font-medium text-muted-foreground">
              {i18n.t("settings.projects.root")}
              <Input
                value={root()}
                onInput={(e) => setRoot(e.target.value)}
                placeholder={i18n.t("settings.projects.rootPlaceholder")}
                spellcheck="false"
              />
            </label>
          )}

          <label class="flex flex-col gap-1 text-xs font-medium text-muted-foreground sm:col-span-2">
            {i18n.t("settings.projects.descriptionLabel")}
            <Textarea
              value={description()}
              onInput={(e) => setDescription(e.target.value)}
              placeholder={i18n.t("settings.projects.descriptionPlaceholder")}
              rows={2}
            />
          </label>
        </div>

        <CommandEditor
          commands={commands()}
          onChange={setCommands}
          disabled={saving()}
          managementAction={
            <ProjectDecisionConfigButton
              client={props.client}
              project={editingProject()}
              disabled={saving() || commandsDirty()}
              onChanged={managementChanged}
            />
          }
        />
        {!editingProject() && (
          <p class="text-xs text-muted-foreground">{i18n.t("commands.saveFirst")}</p>
        )}
        {editingProject() && commandsDirty() && (
          <p class="text-xs text-muted-foreground">{i18n.t("commands.saveChanges")}</p>
        )}

        {error() ? (
          <DismissibleNotice message={error()!} class="text-sm text-destructive">
            {error()}
          </DismissibleNotice>
        ) : null}
        {notice() ? (
          <p role="status" class="text-sm text-emerald-700 dark:text-emerald-400">
            {notice()}
          </p>
        ) : null}

        <div class="flex justify-end gap-2">
          <Button type="submit" disabled={saving()}>
            {saving() ? i18n.t("settings.projects.saving") : i18n.t("settings.projects.save")}
          </Button>
        </div>
      </form>

      <div class="divide-y divide-border/70 py-2">
        {projects().length === 0 ? (
          <p class="py-2 text-sm text-muted-foreground">{i18n.t("settings.projects.empty")}</p>
        ) : (
          projects().map((project) => (
            <div class="flex flex-col gap-2 py-3 sm:flex-row sm:items-start sm:justify-between">
              <div class="min-w-0 space-y-1">
                <div class="flex flex-wrap items-center gap-2">
                  <span class="text-sm font-semibold">{project.name}</span>
                  <Pill tone="slate" class="text-xs normal-case">
                    {project.id}
                  </Pill>
                </div>
                {project.description ? (
                  <p class="text-xs text-muted-foreground">{project.description}</p>
                ) : null}
                <div class="flex min-w-0 flex-wrap items-center gap-1">
                  <p class="min-w-0 font-mono text-xs break-all text-muted-foreground">
                    {project.root}
                  </p>
                  <CopyRootButton project={project} />
                </div>
                {project.commands.length > 0 ? (
                  <details class="pt-1 text-xs text-muted-foreground">
                    <summary class="cursor-pointer py-1 hover:text-foreground">
                      {i18n.t("settings.projects.configuredCommands", {
                        count: project.commands.length,
                      })}
                    </summary>
                    <div class="ocg-subtle-scrollbar mt-1 max-h-48 min-w-0 space-y-1 overflow-y-auto overscroll-contain rounded-md border border-border p-2">
                      {project.commands.map((cmd) => (
                        <div class="font-mono break-all">
                          <span class="font-semibold text-foreground">{cmd.name}</span>:{" "}
                          {cmd.argv.join(" ")}{" "}
                          <span class="font-sans">
                            ·{" "}
                            {i18n.t(
                              cmd.owner === "decision"
                                ? "commands.ownerDecision"
                                : "commands.ownerUser",
                            )}
                            {cmd.status !== "allowed"
                              ? ` · ${i18n.t(cmd.status === "review" ? "commands.review" : "commands.blocked")}`
                              : ""}
                          </span>
                        </div>
                      ))}
                    </div>
                  </details>
                ) : (
                  <p class="text-xs text-muted-foreground">
                    {i18n.t("settings.projects.noCommands")}
                  </p>
                )}
                <p class="text-[11px] text-muted-foreground">
                  {i18n.t("settings.projects.updatedAt", {
                    time: new Date(project.updated_at_us / 1000).toLocaleString(i18n.locale),
                  })}
                </p>
              </div>
              <div class="shrink-0">
                <Button
                  type="button"
                  variant="outline"
                  size="xs"
                  onClick={() => startEdit(project)}
                >
                  {editingProject()?.id === project.id
                    ? i18n.t("settings.projects.cancelEdit")
                    : i18n.t("settings.projects.edit")}
                </Button>
              </div>
            </div>
          ))
        )}
      </div>
    </Section>
  );
}
