"use client";

import { useEffect, useState } from "react";
import { Check } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectLabel,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import type { ControlProfilesSnapshot, ControlProject } from "../contracts";
import type { HttpCanonicalControlClient } from "../runtime/control-client";
import { useI18n } from "../i18n";
import { describeError } from "./settings-parts";

export function ProjectDecisionConfigButton({
  client,
  project,
  disabled,
  onChanged,
}: {
  client: HttpCanonicalControlClient;
  project: ControlProject | null;
  disabled: boolean;
  onChanged(project: ControlProject): void;
}) {
  const { t } = useI18n();
  const [open, setOpen] = useState(false);
  const [profiles, setProfiles] = useState<ControlProfilesSnapshot | null>(null);
  const [selected, setSelected] = useState("");
  const [cutoff, setCutoff] = useState("0.6");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string>();
  const [status, setStatus] = useState("");
  useEffect(() => {
    if (!open) return;
    const controller = new AbortController();
    void client
      .readProfiles(controller.signal)
      .then((value) => {
        if (!controller.signal.aborted) setProfiles(value);
      })
      .catch((cause) => {
        if (!controller.signal.aborted) setError(describeError(cause, t));
      });
    return () => controller.abort();
  }, [client, open, t]);

  async function configure() {
    if (!project) return;
    setBusy(true);
    setError(undefined);
    try {
      const updated = await client.configureProjectDecision(
        project.id,
        selected === "default" || selected === "off" ? null : selected,
        selected === "off" ? (project.decision?.minimum_confidence ?? 0.6) : Number(cutoff),
        selected !== "off",
      );
      onChanged(updated);
      setStatus(t(selected === "off" ? "commands.disabled" : "commands.enabled"));
      setOpen(false);
    } catch (cause) {
      setError(describeError(cause, t));
    } finally {
      setBusy(false);
    }
  }
  const decisionProfiles = profiles?.decision_agent_profiles ?? [];
  const models = [
    ...(profiles?.llm_profiles ?? []).filter((model) => model.enabled),
    ...(profiles?.decision_profiles ?? []),
  ];
  const choices = [...decisionProfiles, ...models];
  const confidence = Number(cutoff);
  const validCutoff =
    cutoff.trim() !== "" &&
    Number.isFinite(confidence) &&
    confidence >= 0 &&
    confidence <= 1 &&
    Math.abs(confidence * 1000 - Math.round(confidence * 1000)) < 1e-9;
  return (
    <Dialog
      open={open}
      onOpenChange={(value) => {
        if (value) {
          setSelected(
            project?.decision?.enabled === false
              ? "off"
              : (project?.decision?.decision_profile ?? "default"),
          );
          setCutoff(String(project?.decision?.minimum_confidence ?? 0.6));
          setError(undefined);
          setStatus("");
        }
        setOpen(value);
      }}
    >
      <DialogTrigger
        render={
          <button
            type="button"
            className="flex cursor-pointer items-center gap-2 text-sm disabled:cursor-not-allowed disabled:opacity-50"
          />
        }
        disabled={disabled || !project}
        title={!project ? t("commands.saveFirst") : undefined}
      >
        <span
          aria-hidden="true"
          className="flex size-4 items-center justify-center rounded-sm border border-input"
        >
          {project?.decision?.enabled !== false && <Check className="size-3" />}
        </span>
        <span>{t("commands.assist")}</span>
        <span className="sr-only">
          {t(
            project?.decision?.enabled !== false
              ? "settings.pools.enabled"
              : "settings.pools.disabled",
          )}
        </span>
      </DialogTrigger>
      <DialogContent className="grid-cols-[minmax(0,1fr)] max-w-lg">
        <DialogHeader>
          <DialogTitle>{t("commands.manage")}</DialogTitle>
          <DialogDescription>{t("commands.description")}</DialogDescription>
        </DialogHeader>
        <p className="text-sm text-muted-foreground">{t("commands.scope")}</p>
        <label className="flex flex-col gap-1 text-sm">
          {t("commands.profile")}
          <Select
            value={selected}
            items={[
              { value: "default", label: t("commands.default") },
              { value: "off", label: t("commands.disable") },
              ...models.map((model) => ({
                value: model.id,
                label: `${model.name} · ${model.model}`,
              })),
              ...decisionProfiles.map((profile) => ({
                value: profile.id,
                label: profile.name,
              })),
            ]}
            disabled={busy}
            onValueChange={(next) => {
              if (next !== null) setSelected(next);
            }}
          >
            <SelectTrigger className="h-10 w-full min-w-0 rounded-md border-input bg-background px-3">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="default">{t("commands.default")}</SelectItem>
              <SelectItem value="off">{t("commands.disable")}</SelectItem>
              <SelectGroup>
                <SelectLabel>{t("settings.nav.models")}</SelectLabel>
                {models.map((model) => (
                  <SelectItem key={model.id} value={model.id}>
                    {model.name} · {model.model}
                  </SelectItem>
                ))}
              </SelectGroup>
              <SelectGroup>
                <SelectLabel>{t("decision.title")}</SelectLabel>
                {decisionProfiles.map((profile) => (
                  <SelectItem key={profile.id} value={profile.id}>
                    {profile.name}
                  </SelectItem>
                ))}
              </SelectGroup>
            </SelectContent>
          </Select>
        </label>
        <label className="flex flex-col gap-1 text-sm">
          {t("commands.cutoff")}
          <Input
            type="number"
            min="0"
            max="1"
            step="0.001"
            value={cutoff}
            disabled={busy || selected === "off"}
            onChange={(event) => setCutoff(event.target.value)}
          />
        </label>
        <p className="text-xs text-muted-foreground">{t("commands.cutoffHelp")}</p>
        {status && (
          <p role="status" className="text-sm text-muted-foreground">
            {status}
          </p>
        )}
        {error && (
          <p role="alert" className="text-sm text-destructive">
            {error}
          </p>
        )}
        <DialogFooter>
          <Button
            type="button"
            disabled={
              busy ||
              (selected !== "off" && !validCutoff) ||
              (selected !== "default" &&
                selected !== "off" &&
                !choices.some((profile) => profile.id === selected))
            }
            onClick={() => void configure()}
          >
            {t(busy ? "commands.saving" : "commands.enable")}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
