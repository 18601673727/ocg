"use client";

import type * as Api from "@/components/ocg/contracts/api";

import { DismissibleNotice } from "@/components/ocg/primitives/dismissible-notice";
import { useCallback, useEffect, useRef, useState } from "react";
import type { HttpCanonicalControlClient } from "../runtime/control-client";
import { useI18n } from "../i18n";
import { ContentContainer, ContentHeader } from "../layout/content-container";
import { LoadMore } from "../primitives/load-more";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

export function LogsSurface({ client }: { client: HttpCanonicalControlClient }) {
  const { t } = useI18n();
  const [file, setFile] = useState("");
  const [timeZone, setTimeZone] = useState(() =>
    typeof window === "undefined"
      ? "UTC"
      : Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC",
  );
  const [snapshot, setSnapshot] = useState<Api.Logs | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [loadingMore, setLoadingMore] = useState(false);
  const loadMoreRef = useRef<() => void>(() => {});

  useEffect(() => {
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
        setError(cause instanceof Error ? cause.message : t("logs.readFailed"));
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
            const page = await client.readLogs({
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
      void client
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
    const unsubscribe = client.subscribeLogs(
      file || undefined,
      (answer) => void receive(answer),
      fail,
    );
    return () => {
      controller.abort();
      unsubscribe();
    };
  }, [client, file, t]);
  const loadMore = useCallback(() => loadMoreRef.current(), []);
  const formatEntry = (line: string) => {
    const match = /^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z) (\w+) (.*)$/.exec(line);
    if (!match) return null;
    const date = new Date(match[1]);
    if (Number.isNaN(date.getTime())) return null;
    return {
      time: new Intl.DateTimeFormat(undefined, {
        timeZone,
        dateStyle: "medium",
        timeStyle: "medium",
      }).format(date),
      level: match[2],
      message: match[3],
    };
  };
  const timeZones = [
    "UTC",
    timeZone,
    "America/Los_Angeles",
    "America/New_York",
    "Europe/London",
    "Europe/Berlin",
    "Asia/Shanghai",
    "Asia/Tokyo",
    "Australia/Sydney",
  ].filter((value, index, values) => values.indexOf(value) === index);

  return (
    <ContentContainer>
      <ContentHeader
        title={t("logs.title")}
        actions={
          <>
            <label className="sr-only" htmlFor="runtime-log-time-zone">
              {t("logs.timeZone")}
            </label>
            <Select
              value={timeZone}
              items={timeZones.map((zone) => ({ value: zone, label: zone }))}
              onValueChange={(next) => {
                if (next !== null) setTimeZone(next);
              }}
            >
              <SelectTrigger
                id="runtime-log-time-zone"
                aria-label={t("logs.timeZone")}
                className="min-w-0 rounded-md border-input bg-background px-3 py-2 font-mono text-xs"
              >
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {timeZones.map((zone) => (
                  <SelectItem key={zone} value={zone}>
                    {zone}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <label className="sr-only" htmlFor="runtime-log-file">
              {t("logs.runtimeLogFile")}
            </label>
            <Select
              value={file}
              items={[
                { value: "", label: t("logs.latestFile") },
                ...(snapshot?.files.map((name) => ({ value: name, label: name })) ?? []),
              ]}
              onValueChange={(next) => {
                if (next === null) return;
                setFile(next);
                setSnapshot(null);
                setLoading(true);
                setLoadingMore(false);
              }}
            >
              <SelectTrigger
                id="runtime-log-file"
                aria-label={t("logs.runtimeLogFile")}
                className="min-w-0 rounded-md border-input bg-background px-3 py-2 font-mono text-xs"
              >
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="">{t("logs.latestFile")}</SelectItem>
                {snapshot?.files.map((name) => (
                  <SelectItem key={name} value={name}>
                    {name}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </>
        }
      />
      {error ? (
        <DismissibleNotice message={error} className="text-sm text-destructive">
          {error}
        </DismissibleNotice>
      ) : null}
      {!snapshot && loading ? <p role="status">{t("logs.loading")}</p> : null}
      {snapshot ? (
        <section
          aria-label={t("logs.output")}
          className="min-w-0 rounded-lg border border-border bg-card p-5"
        >
          <div className="mb-3 flex flex-wrap justify-between gap-2 text-xs text-muted-foreground">
            <span className="font-mono">{snapshot.file ?? t("logs.noFile")}</span>
            <span>
              {t("logs.status", {
                count: snapshot.entries.length,
                mode: error ? t("logs.disconnected") : t("logs.liveMode"),
              })}
            </span>
          </div>
          {snapshot.entries.length ? (
            <ol className="divide-y divide-border/70">
              {[...snapshot.entries].reverse().map((entry) => {
                const parsed = formatEntry(entry.line);
                return (
                  <li
                    key={entry.offset}
                    className="grid grid-cols-[minmax(8rem,auto)_auto_minmax(0,1fr)] items-start gap-x-3 gap-y-1 py-2.5 font-mono text-xs first:pt-1"
                  >
                    {parsed ? (
                      <>
                        <time className="whitespace-nowrap text-muted-foreground">
                          {parsed.time}
                        </time>
                        <span className="rounded-sm bg-muted px-1.5 py-0.5 text-foreground">
                          {parsed.level}
                        </span>
                        <span className="min-w-0 break-words text-foreground">
                          {parsed.message}
                        </span>
                      </>
                    ) : (
                      <span className="col-span-3 min-w-0 whitespace-pre-wrap break-words text-foreground">
                        {entry.line}
                      </span>
                    )}
                  </li>
                );
              })}
            </ol>
          ) : (
            <p className="text-sm text-muted-foreground">{t("logs.noRecords")}</p>
          )}
          {snapshot.next_before !== null && (
            <LoadMore loading={loadingMore} onLoad={loadMore} auto={!error} />
          )}
          {snapshot.files_truncated ? (
            <p className="mt-2 text-xs text-muted-foreground">{t("logs.filesTruncated")}</p>
          ) : null}
        </section>
      ) : null}
    </ContentContainer>
  );
}
