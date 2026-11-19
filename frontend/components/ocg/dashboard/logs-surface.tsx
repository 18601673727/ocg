import { createEffect, createMemo, createSignal } from "solid-js";
import type * as Api from "@/components/ocg/contracts/api";
import { DismissibleNotice } from "@/components/ocg/primitives/dismissible-notice";
import { Select, SelectItem } from "@/components/ui/select";
import { elementRef } from "@/lib/ui-state";
import { useI18n } from "../i18n";
import { ContentContainer, ContentHeader } from "../layout/content-container";
import { ListStatus } from "../primitives/list-status";
import { LoadMore } from "../primitives/load-more";
import { Pill } from "../primitives/pill";
import { StatusDot } from "../primitives/status-dot";
import type { Tone } from "../primitives/tone";
import type { ApiClient } from "../runtime/api-client";

const levelTone: Record<string, Tone> = {
  ERROR: "red",
  WARN: "amber",
};

export function LogsSurface(props: { client: ApiClient }) {
  const i18n = useI18n();
  const [file, setFile] = createSignal("");
  const [timeZone, setTimeZone] = createSignal(
    (() =>
      typeof window === "undefined"
        ? "UTC"
        : Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC")(),
  );
  const [snapshot, setSnapshot] = createSignal<Api.Logs | null>(null);
  const [error, setError] = createSignal<string | null>(null);
  const [loading, setLoading] = createSignal(true);
  const [loadingMore, setLoadingMore] = createSignal(false);
  const loadMoreRef = elementRef<() => void>(() => {});

  createEffect(
    () => [props.client, file(), i18n.t],
    () => {
      const controller = new AbortController();
      let current: Api.Logs | null = null;
      let busy = false;
      let queued: Api.Logs | null = null;
      const merge = (entries: Api.Logs["entries"], page: Api.Logs) => {
        const merged = new Map(entries.map((entry) => [entry.offset, entry]));
        page.entries.forEach((entry) => merged.set(entry.offset, entry));
        return [...merged.values()].sort((a, b) => a.offset - b.offset);
      };
      const fail = (cause: unknown) => {
        if (!controller.signal.aborted) {
          setError(cause instanceof Error ? cause.message : i18n.t("logs.readFailed"));
          setLoading(false);
        }
      };
      const receive = async (answer: Api.Logs) => {
        if (busy) {
          queued = answer;
          return;
        }
        busy = true;
        setLoadingMore(true);
        try {
          if (!current || current.file !== answer.file) {
            current = answer;
          } else {
            const oldEnd = current.entries.at(-1)?.offset;
            let incoming = answer.entries;
            let before = answer.next_before;
            while (
              answer.file &&
              oldEnd !== undefined &&
              incoming.length &&
              incoming[0].offset > oldEnd &&
              before !== null
            ) {
              const page = await props.client.readLogs({
                file: answer.file,
                before,
                limit: 100,
                signal: controller.signal,
              });
              if (controller.signal.aborted) return;
              incoming = merge(incoming, page);
              if (page.next_before === before) throw new Error("Log cursor did not advance.");
              before = page.next_before;
            }
            current = {
              ...answer,
              entries: merge(current.entries, { ...answer, entries: incoming }),
              next_before: current.next_before,
            };
          }
          setSnapshot(current);
          setError(null);
          setLoading(false);
        } catch (cause) {
          fail(cause);
        } finally {
          busy = false;
          if (!controller.signal.aborted) setLoadingMore(false);
          if (queued && !controller.signal.aborted) {
            const latest = queued;
            queued = null;
            void receive(latest);
          }
        }
      };
      loadMoreRef.current = () => {
        if (busy || !current?.file || current.next_before === null) return;
        busy = true;
        setLoadingMore(true);
        const before = current.next_before;
        void props.client
          .readLogs({ file: current.file, before, limit: 100, signal: controller.signal })
          .then((page) => {
            if (controller.signal.aborted || !current) return;
            if (page.next_before === before) throw new Error("Log cursor did not advance.");
            current = {
              ...current,
              entries: merge(current.entries, page),
              next_before: page.next_before,
            };
            setSnapshot(current);
            setError(null);
          })
          .catch(fail)
          .finally(() => {
            busy = false;
            if (!controller.signal.aborted) setLoadingMore(false);
            if (queued && !controller.signal.aborted) {
              const latest = queued;
              queued = null;
              void receive(latest);
            }
          });
      };
      const unsubscribe = props.client.subscribeLogs(
        file() || undefined,
        (answer) => void receive(answer),
        fail,
      );
      return () => {
        controller.abort();
        unsubscribe();
      };
    },
  );
  const loadMore = () => loadMoreRef.current();
  const dateTime = createMemo(
    () =>
      new Intl.DateTimeFormat(i18n.locale, {
        timeZone: timeZone(),
        dateStyle: "medium",
        timeStyle: "medium",
      }),
  );
  const formatEntry = (line: string) => {
    const match = /^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z) (\w+) (.*)$/.exec(line);
    if (!match) return null;
    const date = new Date(match[1]);
    if (Number.isNaN(date.getTime())) return null;
    return {
      time: dateTime().format(date),
      dateTime: match[1],
      level: match[2],
      message: match[3],
    };
  };
  const timeZones = createMemo(() =>
    [
      "UTC",
      timeZone(),
      "America/Los_Angeles",
      "America/New_York",
      "Europe/London",
      "Europe/Berlin",
      "Asia/Shanghai",
      "Asia/Tokyo",
      "Australia/Sydney",
    ].filter((value, index, values) => values.indexOf(value) === index),
  );

  return (
    <ContentContainer>
      <ContentHeader
        title={i18n.t("logs.title")}
        actions={
          <>
            <label class="sr-only" for="runtime-log-time-zone">
              {i18n.t("logs.timeZone")}
            </label>
            <Select
              value={timeZone()}
              items={timeZones().map((zone) => ({ value: zone, label: zone }))}
              onValueChange={(next) => {
                if (next !== null) setTimeZone(next);
              }}
              id="runtime-log-time-zone"
              aria-label={i18n.t("logs.timeZone")}
              class="min-w-0 max-w-full border-input bg-card text-xs"
            >
              {timeZones().map((zone) => (
                <SelectItem value={zone}>{zone}</SelectItem>
              ))}
            </Select>
            <label class="sr-only" for="runtime-log-file">
              {i18n.t("logs.runtimeLogFile")}
            </label>
            <Select
              value={file()}
              items={[
                { value: "", label: i18n.t("logs.latestFile") },
                ...(snapshot()?.files.map((name) => ({ value: name, label: name })) ?? []),
              ]}
              onValueChange={(next) => {
                if (next === null) return;
                setFile(next);
                setSnapshot(null);
                setLoading(true);
                setLoadingMore(false);
              }}
              id="runtime-log-file"
              aria-label={i18n.t("logs.runtimeLogFile")}
              class="min-w-0 max-w-full border-input bg-card text-xs"
            >
              <SelectItem value="">{i18n.t("logs.latestFile")}</SelectItem>
              {snapshot()?.files.map((name) => (
                <SelectItem value={name}>{name}</SelectItem>
              ))}
            </Select>
          </>
        }
      />
      {error() ? (
        <DismissibleNotice message={error()!} class="text-sm text-destructive">
          {error()}
        </DismissibleNotice>
      ) : null}
      {!snapshot() && loading() ? <ListStatus loading message={i18n.t("logs.loading")} /> : null}
      {snapshot() ? (
        <section
          aria-label={i18n.t("logs.output")}
          class="min-w-0 overflow-hidden rounded-xl border border-border bg-card"
        >
          <div class="flex flex-wrap items-center justify-between gap-x-4 gap-y-2 border-b border-border bg-muted/30 px-4 py-3 sm:px-5">
            <h2 class="min-w-0 font-mono text-sm font-medium break-all">
              {snapshot()!.file ?? i18n.t("logs.noFile")}
            </h2>
            <span class="inline-flex items-center gap-2 text-xs text-muted-foreground">
              <StatusDot tone={error() ? "amber" : "emerald"} />
              {i18n.t("logs.status", {
                count: snapshot()!.entries.length,
                mode: error() ? i18n.t("logs.disconnected") : i18n.t("logs.liveMode"),
              })}
            </span>
          </div>
          {snapshot()!.entries.length ? (
            <ol class="divide-y divide-border/70">
              {[...snapshot()!.entries].reverse().map((entry) => {
                const parsed = formatEntry(entry.line);
                return (
                  <li class="grid grid-cols-[1fr_auto] items-baseline gap-x-3 gap-y-1 px-4 py-2.5 font-mono text-xs leading-5 even:bg-muted/20 sm:grid-cols-[13rem_4rem_minmax(0,1fr)] sm:px-5">
                    {parsed ? (
                      <>
                        <time datetime={parsed.dateTime} class="text-muted-foreground">
                          {parsed.time}
                        </time>
                        <Pill
                          tone={levelTone[parsed.level.toUpperCase()] ?? "slate"}
                          class="w-fit font-mono normal-case"
                        >
                          {parsed.level}
                        </Pill>
                        <span class="col-span-full min-w-0 whitespace-pre-wrap wrap-anywhere text-foreground sm:col-span-1">
                          {parsed.message}
                        </span>
                      </>
                    ) : (
                      <span class="col-span-full min-w-0 whitespace-pre-wrap wrap-anywhere text-foreground">
                        {entry.line}
                      </span>
                    )}
                  </li>
                );
              })}
            </ol>
          ) : (
            <ListStatus message={i18n.t("logs.noRecords")} />
          )}
          {snapshot()!.next_before !== null && (
            <LoadMore loading={loadingMore()} onLoad={loadMore} auto={!error()} />
          )}
          {snapshot()!.files_truncated ? (
            <p class="border-t border-border px-4 py-3 text-xs text-muted-foreground">
              {i18n.t("logs.filesTruncated")}
            </p>
          ) : null}
        </section>
      ) : null}
    </ContentContainer>
  );
}
