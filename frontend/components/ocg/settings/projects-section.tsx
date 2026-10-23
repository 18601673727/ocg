"use client";

import { DismissibleNotice } from "@/components/ocg/primitives/dismissible-notice";
import { useCallback, useEffect, useState, type FormEvent } from "react";
import { Check, Copy } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import { Pill } from "@/components/ocg/primitives";
import type { ControlProject } from "@/components/ocg/contracts";
import type {
  ControlProjectCommand,
  ControlProjectCreate,
  ControlProjectUpdate,
  HttpCanonicalControlClient,
} from "@/components/ocg/runtime/control-client";
import { Section, describeError, refusalText } from "./settings-parts";
import { useI18n, type I18nKey } from "../i18n";
import { useCopyToClipboard } from "@/hooks/use-copy-to-clipboard";
import { toCommandDrafts, toProjectCommands, type CommandDraft } from "./command-drafts";
import { CommandEditor } from "./command-editor";

const PROJECT_REFUSALS: Partial<Record<string, I18nKey>> = {
  project_root_invalid: "settings.projects.refusal.rootInvalid",
  project_root_overlaps: "settings.projects.refusal.rootOverlaps",
  invalid_commands: "settings.projects.refusal.invalidCommands",
  project_not_found: "settings.projects.refusal.notFound",
  invalid_request: "settings.projects.refusal.invalidRequest",
};

/** Copies a Project's root path verbatim and reports the outcome inline. */
function CopyRootButton({ project }: { project: ControlProject }) {
  const { t } = useI18n();
  const { isCopied, isCopyFailed, copyToClipboard } = useCopyToClipboard();

  const label = isCopied
    ? t("common.copied")
    : isCopyFailed
      ? t("common.copyFailed")
      : t("common.copy");

  return (
    <Button
      type="button"
      variant="ghost"
      size="xs"
      className="shrink-0 px-1.5 tracking-normal normal-case"
      aria-label={t("settings.projects.copyRootFor", { name: project.name })}
      onClick={() => copyToClipboard(project.root)}
    >
      {isCopied ? <Check aria-hidden="true" /> : <Copy aria-hidden="true" />}
      <span role="status">{label}</span>
    </Button>
  );
}

export function ProjectsSection({ client }: { client: HttpCanonicalControlClient }) {
  const { t, locale } = useI18n();
  const [projects, setProjects] = useState<ControlProject[]>([]);
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState<string | null>(null);

  const [editingProject, setEditingProject] = useState<ControlProject | null>(null);
  const [name, setName] = useState("");
  const [root, setRoot] = useState("");
  const [description, setDescription] = useState("");
  const [commands, setCommands] = useState<CommandDraft[]>([]);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  const refresh = useCallback(
    (signal?: AbortSignal) => client.readAllProjects(signal)
      .then((loaded) => {
        if (!signal?.aborted) {
          setProjects(loaded);
          setLoadError(null);
        }
      })
      .catch((err: unknown) => {
        if (!signal?.aborted) setLoadError(describeError(err, t));
      })
      .finally(() => {
        if (!signal?.aborted) setLoading(false);
      }),
    [client, t]
  );

  useEffect(() => {
    const controller = new AbortController();
    void refresh(controller.signal);
    return () => controller.abort();
  }, [refresh]);

  function resetForm() {
    setEditingProject(null);
    setName("");
    setRoot("");
    setDescription("");
    setCommands([]);
    setError(null);
  }

  function startEdit(project: ControlProject) {
    if (editingProject?.id === project.id) {
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

    const trimmedDesc = description.trim();
    const finalDescription = trimmedDesc.length > 0 ? trimmedDesc : null;
    const finalCommands: ControlProjectCommand[] = toProjectCommands(commands);

    try {
      if (editingProject) {
        const payload: ControlProjectUpdate = {
          name: name.trim(),
          description: finalDescription,
          commands: finalCommands,
        };
        const updated = await client.updateProject(editingProject.id, payload);
        setProjects((prev) => prev.map((p) => (p.id === updated.id ? updated : p)));
        resetForm();
        setNotice(t("settings.projects.updatedNotice"));
      } else {
        const payload: ControlProjectCreate = {
          name: name.trim(),
          root: root.trim(),
          description: finalDescription,
          commands: finalCommands,
        };
        const created = await client.createProject(payload);
        setProjects((prev) => [created, ...prev]);
        resetForm();
        setNotice(t("settings.projects.createdNotice"));
      }
    } catch (err) {
      setError(refusalText(t, err, PROJECT_REFUSALS));
    } finally {
      setSaving(false);
    }
  }

  return (
    <Section
      id="settings-projects"
      title={t("settings.projects.title")}
      description={t("settings.projects.description")}
      action={
        <Button
          variant="ghost"
          size="sm"
          className="h-9 rounded-md tracking-normal normal-case"
          disabled={loading}
          onClick={() => { setLoading(true); setLoadError(null); void refresh(); }}
        >
          {loading ? t("settings.projects.refreshing") : t("settings.projects.refresh")}
        </Button>
      }
    >
      {loadError ? (
        <DismissibleNotice message={loadError}  className="py-2 text-sm text-destructive">
          {loadError}
        </DismissibleNotice>
      ) : null}

      <div className="divide-y divide-border/70 py-2">
        {projects.length === 0 ? (
          <p className="py-2 text-sm text-muted-foreground">
            {t("settings.projects.empty")}
          </p>
        ) : (
          projects.map((project) => (
            <div
              key={project.id}
              className="flex flex-col gap-2 py-3 sm:flex-row sm:items-start sm:justify-between"
            >
              <div className="min-w-0 space-y-1">
                <div className="flex flex-wrap items-center gap-2">
                  <span className="text-sm font-semibold">{project.name}</span>
                  <Pill tone="slate" className="text-xs normal-case">
                    {project.id}
                  </Pill>
                </div>
                {project.description ? (
                  <p className="text-xs text-muted-foreground">{project.description}</p>
                ) : null}
                <div className="flex min-w-0 flex-wrap items-center gap-1">
                  <p className="min-w-0 font-mono text-xs break-all text-muted-foreground">{project.root}</p>
                  <CopyRootButton project={project} />
                </div>
                {project.commands.length > 0 ? (
                  <details className="pt-1 text-xs text-muted-foreground">
                    <summary className="cursor-pointer py-1 hover:text-foreground">{t("settings.projects.configuredCommands", { count: project.commands.length })}</summary>
                    <div className="mt-1 max-h-48 space-y-1 overflow-y-auto overscroll-contain rounded-md border border-border p-2">
                      {project.commands.map((cmd) => (
                        <div key={cmd.name} className="font-mono break-all">
                          <span className="font-semibold text-foreground">{cmd.name}</span>:{" "}
                          {cmd.argv.join(" ")}
                        </div>
                      ))}
                    </div>
                  </details>
                ) : (
                  <p className="text-xs text-muted-foreground">
                    {t("settings.projects.noCommands")}
                  </p>
                )}
                <p className="text-[11px] text-muted-foreground">
                  {t("settings.projects.updatedAt", {
                    time: new Date(project.updated_at_us / 1000).toLocaleString(locale),
                  })}
                </p>
              </div>
              <div className="shrink-0">
                <Button
                  type="button"
                  variant="outline"
                  size="xs"
                  onClick={() => startEdit(project)}
                >
                  {editingProject?.id === project.id
                    ? t("settings.projects.cancelEdit")
                    : t("settings.projects.edit")}
                </Button>
              </div>
            </div>
          ))
        )}
      </div>

      <form
        onSubmit={(e) => void handleSave(e)}
        className="mt-2 flex flex-col gap-3 border-t border-border pt-4"
      >
        <div className="flex items-center justify-between gap-2">
          <h3 className="text-sm font-semibold">
            {editingProject
              ? t("settings.projects.editTitle", { id: editingProject.id })
              : t("settings.projects.createTitle")}
          </h3>
          {editingProject ? (
            <Button type="button" variant="ghost" size="xs" onClick={resetForm}>
              {t("settings.projects.cancelEdit")}
            </Button>
          ) : null}
        </div>

        <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
          <label className="flex flex-col gap-1 text-xs font-medium text-muted-foreground">
            {t("settings.projects.name")}
            <Input
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder={t("settings.projects.namePlaceholder")}
              spellCheck={false}
            />
          </label>

          {editingProject ? (
            <div className="flex flex-col gap-1 text-xs font-medium text-muted-foreground">
              <span>{t("settings.projects.root")}</span>
              <span className="font-mono text-xs text-foreground py-2 break-all">
                {editingProject.root}
              </span>
            </div>
          ) : (
            <label className="flex flex-col gap-1 text-xs font-medium text-muted-foreground">
              {t("settings.projects.root")}
              <Input
                value={root}
                onChange={(e) => setRoot(e.target.value)}
                placeholder={t("settings.projects.rootPlaceholder")}
                spellCheck={false}
              />
            </label>
          )}

          <label className="flex flex-col gap-1 text-xs font-medium text-muted-foreground sm:col-span-2">
            {t("settings.projects.descriptionLabel")}
            <Textarea
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              placeholder={t("settings.projects.descriptionPlaceholder")}
              rows={2}
            />
          </label>
        </div>

        <CommandEditor
          key={editingProject?.id ?? "new-project"}
          commands={commands}
          onChange={setCommands}
          disabled={saving}
        />

        {error ? (
          <DismissibleNotice message={error}  className="text-sm text-destructive">
            {error}
          </DismissibleNotice>
        ) : null}
        {notice ? (
          <p role="status" className="text-sm text-emerald-700 dark:text-emerald-400">
            {notice}
          </p>
        ) : null}

        <div className="flex justify-end gap-2">
          <Button type="submit" disabled={saving}>
            {saving ? t("settings.projects.saving") : t("settings.projects.save")}
          </Button>
        </div>
      </form>
    </Section>
  );
}
