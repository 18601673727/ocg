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

  return (
    <ContentContainer>
      <ContentHeader
        title={t("logs.title")}
        actions={
          <>
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
            <pre className="whitespace-pre-wrap break-words font-mono text-xs leading-6">
              {[...snapshot.entries].reverse().map((entry) => (
                <div key={entry.offset}>{entry.line}</div>
              ))}
            </pre>
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
