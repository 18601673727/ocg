"use client";

import Image from "next/image";
import { useEffect, useRef, useState } from "react";
import { Check, ChevronLeft, ChevronRight, Download, Loader2, RotateCcw, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogTitle } from "@/components/ui/dialog";
import type { ChatImage } from "../contracts";
import { useI18n } from "../i18n";
import { useOcgControlUrl } from "../profile/control-url";
import { useOcgRuntime } from "../runtime/runtime-context";

const MAX_IMAGES = 8;
const MAX_IMAGE_BYTES = 4 * 1024 * 1024;
const IMAGE_TYPES = new Set(["image/png", "image/jpeg", "image/gif", "image/webp"]);

type Attachment = {
  id: string;
  file: File;
  preview: string;
  status: "uploading" | "ready" | "failed";
  image?: ChatImage;
  error?: string;
};

function readImage(file: File, signal: AbortSignal): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    const abort = () => reader.abort();
    signal.addEventListener("abort", abort, { once: true });
    reader.onloadend = () => signal.removeEventListener("abort", abort);
    reader.onload = () => typeof reader.result === "string" ? resolve(reader.result) : reject(new Error("Unable to read image."));
    reader.onerror = () => reject(reader.error ?? new Error("Unable to read image."));
    reader.onabort = () => reject(new DOMException("Upload cancelled", "AbortError"));
    if (signal.aborted) reject(new DOMException("Upload cancelled", "AbortError"));
    else reader.readAsDataURL(file);
  });
}

export function useImageAttachments(projectId?: string) {
  const { client } = useOcgRuntime();
  const { t } = useI18n();
  const [attachments, setAttachments] = useState<Attachment[]>([]);
  const [error, setError] = useState<string | null>(null);
  const current = useRef<Attachment[]>([]);
  const uploads = useRef(new Map<string, AbortController>());
  const update = (items: Attachment[]) => {
    current.current = items;
    setAttachments(items);
  };

  useEffect(() => {
    const controllers = uploads.current;
    const items = current;
    return () => {
      controllers.forEach(controller => controller.abort());
      controllers.clear();
      items.current.forEach(item => URL.revokeObjectURL(item.preview));
      items.current = [];
    };
  }, []);

  async function upload(item: Attachment) {
    if (!projectId || !client.uploadChatImage) {
      update(current.current.map(existing => existing.id === item.id ? { ...existing, status: "failed", error: t("chat.imageUploadUnavailable") } : existing));
      return;
    }
    const controller = new AbortController();
    uploads.current.set(item.id, controller);
    update(current.current.map(existing => existing.id === item.id ? { ...existing, status: "uploading", error: undefined } : existing));
    try {
      const data_url = await readImage(item.file, controller.signal);
      const image = await client.uploadChatImage({ project_id: projectId, name: item.file.name, data_url }, controller.signal);
      if (!controller.signal.aborted) update(current.current.map(existing => existing.id === item.id ? { ...existing, image, status: "ready" } : existing));
    } catch (cause) {
      if (!controller.signal.aborted) update(current.current.map(existing => existing.id === item.id ? { ...existing, status: "failed", error: cause instanceof Error ? cause.message : String(cause) } : existing));
    } finally {
      if (uploads.current.get(item.id) === controller) uploads.current.delete(item.id);
    }
  }

  function add(files: File[]) {
    if (!projectId || !client.uploadChatImage) { setError(t("chat.imageUploadUnavailable")); return; }
    setError(null);
    const added: Attachment[] = [];
    for (const file of files) {
      if (!IMAGE_TYPES.has(file.type)) { setError(t("chat.imageTypeError")); continue; }
      if (file.size === 0 || file.size > MAX_IMAGE_BYTES) { setError(t("chat.imageSizeError")); continue; }
      if (current.current.length + added.length >= MAX_IMAGES) { setError(t("chat.imageCountError")); break; }
      added.push({ id: crypto.randomUUID(), file, preview: URL.createObjectURL(file), status: "uploading" });
    }
    update([...current.current, ...added]);
    added.forEach(item => void upload(item));
  }

  function remove(id: string) {
    uploads.current.get(id)?.abort();
    uploads.current.delete(id);
    const item = current.current.find(item => item.id === id);
    if (item) URL.revokeObjectURL(item.preview);
    update(current.current.filter(item => item.id !== id));
    setError(null);
  }

  function clear() {
    current.current.forEach(item => {
      uploads.current.get(item.id)?.abort();
      URL.revokeObjectURL(item.preview);
    });
    uploads.current.clear();
    update([]);
    setError(null);
  }

  return { attachments, error, add, remove, clear, retry: (id: string) => {
    const item = current.current.find(item => item.id === id);
    if (item?.status === "failed") void upload(item);
  }, images: attachments.flatMap(item => item.image ? [item.image] : []), ready: attachments.every(item => item.status === "ready") };
}

export function AttachmentStaging({ state, disabled }: { state: ReturnType<typeof useImageAttachments>; disabled: boolean }) {
  const { t } = useI18n();
  if (!state.attachments.length && !state.error) return null;
  return <div className="border-t border-border px-3 py-2" aria-label={t("chat.imageStaging")}>
    {!!state.attachments.length && <>
      <p className="mb-2 text-[11px] text-muted-foreground" role="status">{t("chat.imageStaging")} · {state.attachments.length}/8</p>
      <ul className="flex max-h-52 flex-wrap gap-2 overflow-y-auto">
        {state.attachments.map(item => <li key={item.id} className="w-28 rounded-md border border-border bg-muted/30 p-1.5">
          <div className="relative">
            <Image src={item.preview} alt={item.file.name} width={100} height={64} unoptimized className="h-16 w-full rounded object-cover" />
            <Button type="button" variant="secondary" size="icon-xs" className="absolute -top-1 -right-1" disabled={disabled} aria-label={t("chat.removeImage", { name: item.file.name })} onClick={() => state.remove(item.id)}><X className="size-3" /></Button>
          </div>
          <p className="mt-1 truncate text-[10px]" title={item.file.name}>{item.file.name}</p>
          <p className="mt-1 flex items-center gap-1 text-[10px] text-muted-foreground" role="status">
            {item.status === "uploading" ? <><Loader2 className="size-3 animate-spin" />{t("chat.imageUploading")}</> : item.status === "ready" ? <><Check className="size-3 text-green-600" />{t("chat.imageReady")}</> : <span className="text-destructive">{t("chat.imageUploadFailed")}</span>}
          </p>
          {item.status === "failed" && <><p role="alert" className="mt-1 line-clamp-2 break-words text-[10px] text-destructive" title={item.error}>{item.error}</p><Button type="button" variant="ghost" size="xs" disabled={disabled} onClick={() => state.retry(item.id)}><RotateCcw className="size-3" />{t("common.retry")}</Button></>}
        </li>)}
      </ul>
    </>}
    {state.error && <p role="alert" className="mt-1 text-xs text-destructive">{state.error}</p>}
  </div>;
}

function imageSource(url: string, baseUrl: string | null): string | null {
  if (/^\/api\/v1\/canonical\/chat\/images\/[a-zA-Z0-9_-]+\/[a-zA-Z0-9_-]+$/.test(url)) return baseUrl ? `${baseUrl.replace(/\/$/, "")}${url}` : url;
  if (/^data:image\/(png|jpeg|gif|webp);base64,[A-Za-z0-9+/=]+$/.test(url) && url.length <= 6 * 1024 * 1024) return url;
  try {
    const parsed = new URL(url);
    return (parsed.protocol === "https:" || parsed.protocol === "http:") && url.length <= 8192 ? parsed.href : null;
  } catch { return null; }
}

function ImagePreview({ src, name, large = false, compact = false }: { src: string; name: string; large?: boolean; compact?: boolean }) {
  const { t } = useI18n();
  const [failed, setFailed] = useState(false);
  return failed ? <span className="flex h-28 items-center justify-center p-3 text-xs text-muted-foreground">{t("chat.imageUnavailable")}</span> : <Image src={src} alt={name} width={large ? 1000 : 160} height={large ? 750 : 112} unoptimized onError={() => setFailed(true)} className={large ? "max-h-[65vh] w-full object-contain" : compact ? "size-8 object-cover" : "h-28 w-full object-contain"} />;
}

export function ImageGallery({ images, compact = false }: { images?: ChatImage[]; compact?: boolean }) {
  const baseUrl = useOcgControlUrl();
  const { t } = useI18n();
  const [selected, setSelected] = useState<number | null>(null);
  const [downloading, setDownloading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const items = (images ?? []).flatMap(image => {
    const src = imageSource(image.url, baseUrl);
    return src ? [{ ...image, src }] : [];
  });
  const active = selected !== null ? items[selected] : undefined;
  if (!items.length) return null;
  async function download() {
    if (!active || downloading) return;
    setDownloading(true);
    setError(null);
    try {
      const response = await fetch(active.src);
      if (!response.ok) throw new Error(t("chat.imageUnavailable"));
      const url = URL.createObjectURL(await response.blob());
      const link = document.createElement("a");
      link.href = url;
      link.download = active.name.includes(".") ? active.name : `${active.name}.${active.media_type.split("/")[1] === "*" ? "png" : active.media_type.split("/")[1]}`;
      link.click();
      setTimeout(() => URL.revokeObjectURL(url), 1000);
    } catch { setError(t("chat.imageDownloadFailed")); }
    finally { setDownloading(false); }
  }
  return <>
    <div className={compact ? "flex shrink-0 gap-1" : "mt-2 grid max-w-lg grid-cols-2 gap-2 sm:grid-cols-3"} aria-label={t("chat.messageImages")}>
      {items.map((image, index) => <button key={image.id} type="button" className={compact ? "size-8 overflow-hidden rounded border border-border" : "overflow-hidden rounded-md border border-border bg-muted/30 hover:border-ring"} aria-label={t("chat.previewImage", { name: image.name })} onClick={() => { setSelected(index); setError(null); }}>
        <ImagePreview src={image.src} name={image.name} compact={compact} />
      </button>)}
    </div>
    <Dialog open={Boolean(active)} onOpenChange={open => { if (!open) setSelected(null); }}>
      <DialogContent className="sm:max-w-3xl">
        <DialogTitle className="pr-10 text-sm normal-case">{active?.name}</DialogTitle>
        {active && <ImagePreview key={active.id} src={active.src} name={active.name} large />}
        <div className="flex items-center justify-between gap-2">
          <div className="flex items-center gap-2">
            <Button type="button" size="icon-xs" variant="outline" disabled={selected === 0} aria-label={t("chat.previousImage")} onClick={() => { setSelected(index => Math.max(0, (index ?? 0) - 1)); setError(null); }}><ChevronLeft className="size-4" /></Button>
            <span className="text-xs text-muted-foreground">{(selected ?? 0) + 1} / {items.length}</span>
            <Button type="button" size="icon-xs" variant="outline" disabled={selected === items.length - 1} aria-label={t("chat.nextImage")} onClick={() => { setSelected(index => Math.min(items.length - 1, (index ?? 0) + 1)); setError(null); }}><ChevronRight className="size-4" /></Button>
          </div>
          <Button type="button" size="xs" variant="outline" disabled={downloading} onClick={() => void download()}>{downloading ? <Loader2 className="size-3 animate-spin" /> : <Download className="size-3" />}{t("chat.downloadImage")}</Button>
        </div>
        {error && <p role="alert" className="text-xs text-destructive">{error}</p>}
      </DialogContent>
    </Dialog>
  </>;
}
