"use client";

import { useCallback, useEffect, useState, type FormEvent } from "react";
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

type CommandDraft = {
  name: string;
  argvText: string;
};

const PROJECT_REFUSALS: Partial<Record<string, I18nKey>> = {
  project_root_invalid: "settings.projects.refusal.rootInvalid",
  project_root_overlaps: "settings.projects.refusal.rootOverlaps",
  invalid_commands: "settings.projects.refusal.invalidCommands",
  project_not_found: "settings.projects.refusal.notFound",
  invalid_request: "settings.projects.refusal.invalidRequest",
};

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
        if (!signal?.aborted) setLoadError(describeError(err));
      })
      .finally(() => {
        if (!signal?.aborted) setLoading(false);
      }),
    [client]
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
    setCommands(
      project.commands.map((cmd) => ({
        name: cmd.name,
        argvText: cmd.argv.join("\n"),
      }))
    );
    setError(null);
    setNotice(null);
  }

  function addCommand() {
    setCommands((prev) => [...prev, { name: "", argvText: "" }]);
  }

  function updateCommandName(index: number, newName: string) {
    setCommands((prev) =>
      prev.map((cmd, i) => (i === index ? { ...cmd, name: newName } : cmd))
    );
  }

  function updateCommandArgv(index: number, newArgvText: string) {
    setCommands((prev) =>
      prev.map((cmd, i) => (i === index ? { ...cmd, argvText: newArgvText } : cmd))
    );
  }

  function removeCommand(index: number) {
    setCommands((prev) => prev.filter((_, i) => i !== index));
  }

  async function handleSave(e: FormEvent) {
    e.preventDefault();
    setSaving(true);
    setError(null);
    setNotice(null);

    const trimmedDesc = description.trim();
    const finalDescription = trimmedDesc.length > 0 ? trimmedDesc : null;
    const finalCommands: ControlProjectCommand[] = commands.map((cmd) => ({
      name: cmd.name.trim(),
      argv: cmd.argvText
        .split("\n")
        .map((line) => line.trim())
        .filter((line) => line.length > 0),
    }));

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
        <p role="alert" className="py-2 text-sm text-destructive">
          {loadError}
        </p>
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
                <p className="font-mono text-xs text-muted-foreground">{project.root}</p>
                {project.commands.length > 0 ? (
                  <div className="flex flex-col gap-0.5 pt-1 text-xs text-muted-foreground">
                    {project.commands.map((cmd) => (
                      <div key={cmd.name} className="font-mono">
                        <span className="font-semibold text-foreground">{cmd.name}</span>:{" "}
                        {cmd.argv.join(" ")}
                      </div>
                    ))}
                  </div>
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
              placeholder="My Project"
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
                placeholder="/path/to/project"
                spellCheck={false}
              />
            </label>
          )}

          <label className="flex flex-col gap-1 text-xs font-medium text-muted-foreground sm:col-span-2">
            {t("settings.projects.descriptionLabel")}
            <Textarea
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              placeholder="Optional project description"
              rows={2}
            />
          </label>
        </div>

        <div className="flex flex-col gap-2">
          <div className="flex items-center justify-between">
            <span className="text-xs font-medium text-muted-foreground">
              {t("settings.projects.commands")}
            </span>
            <Button type="button" variant="outline" size="xs" onClick={addCommand}>
              {t("settings.projects.addCommand")}
            </Button>
          </div>

          {commands.map((cmd, idx) => (
            <div
              key={idx}
              className="flex flex-col gap-2 rounded-md border border-border p-2.5 sm:flex-row sm:items-start"
            >
              <div className="flex-1 space-y-2">
                <Input
                  value={cmd.name}
                  onChange={(e) => updateCommandName(idx, e.target.value)}
                  placeholder={t("settings.projects.commandName")}
                  spellCheck={false}
                  className="text-xs"
                />
                <Textarea
                  value={cmd.argvText}
                  onChange={(e) => updateCommandArgv(idx, e.target.value)}
                  placeholder={t("settings.projects.commandArgv")}
                  rows={2}
                  className="font-mono text-xs"
                  spellCheck={false}
                />
              </div>
              <Button
                type="button"
                variant="ghost"
                size="xs"
                className="text-destructive hover:text-destructive self-end sm:self-start"
                onClick={() => removeCommand(idx)}
              >
                {t("settings.projects.removeCommand")}
              </Button>
            </div>
          ))}

          <p className="text-xs text-muted-foreground">
            {t("settings.projects.commandsNote")}
          </p>
        </div>

        {error ? (
          <p role="alert" className="text-sm text-destructive">
            {error}
          </p>
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
