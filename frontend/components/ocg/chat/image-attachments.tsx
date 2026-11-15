"use client";

import { DismissibleNotice } from "@/components/ocg/primitives/dismissible-notice";
import Image from "next/image";
import { useState } from "react";
import { ChevronLeft, ChevronRight, Download, Loader2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogTitle } from "@/components/ui/dialog";
import type { ChatImage } from "../contracts";
import { useI18n } from "../i18n";
import { useOcgApiUrl } from "../profile/api-url";

function imageSource(url: string, baseUrl: string | null): string | null {
  if (
    /^\/api\/v1\/attachments\/[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\/content$/.test(
      url,
    )
  )
    return baseUrl ? `${baseUrl.replace(/\/$/, "")}${url}` : url;
  if (
    /^data:image\/(png|jpeg|gif|webp);base64,[A-Za-z0-9+/=]+$/.test(url) &&
    url.length <= 6 * 1024 * 1024
  )
    return url;
  try {
    const parsed = new URL(url);
    return (parsed.protocol === "https:" || parsed.protocol === "http:") && url.length <= 8192
      ? parsed.href
      : null;
  } catch {
    return null;
  }
}

function ImagePreview({
  src,
  name,
  large = false,
  compact = false,
}: {
  src: string;
  name: string;
  large?: boolean;
  compact?: boolean;
}) {
  const { t } = useI18n();
  const [failed, setFailed] = useState(false);
  return failed ? (
    <span className="flex h-28 items-center justify-center p-3 text-xs text-muted-foreground">
      {t("chat.imageUnavailable")}
    </span>
  ) : (
    <Image
      src={src}
      alt={name}
      width={large ? 1000 : 160}
      height={large ? 750 : 112}
      unoptimized
      onError={() => setFailed(true)}
      className={
        large
          ? "max-h-[65vh] w-full object-contain"
          : compact
            ? "size-8 object-cover"
            : "h-28 w-full object-contain"
      }
    />
  );
}

export function ImageGallery({
  images,
  compact = false,
}: {
  images?: ChatImage[];
  compact?: boolean;
}) {
  const baseUrl = useOcgApiUrl();
  const { t } = useI18n();
  const [selected, setSelected] = useState<number | null>(null);
  const [downloading, setDownloading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const items = (images ?? []).flatMap((image) => {
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
      link.download = active.name.includes(".")
        ? active.name
        : `${active.name}.${active.media_type.split("/")[1] === "*" ? "png" : active.media_type.split("/")[1]}`;
      link.click();
      setTimeout(() => URL.revokeObjectURL(url), 1000);
    } catch {
      setError(t("chat.imageDownloadFailed"));
    } finally {
      setDownloading(false);
    }
  }
  return (
    <>
      <div
        className={
          compact ? "flex shrink-0 gap-1" : "mt-2 grid max-w-lg grid-cols-2 gap-2 sm:grid-cols-3"
        }
        aria-label={t("chat.messageImages")}
      >
        {items.map((image, index) => (
          <button
            key={image.id}
            type="button"
            className={
              compact
                ? "size-8 overflow-hidden rounded border border-border"
                : "overflow-hidden rounded-md border border-border bg-muted/30 hover:border-ring"
            }
            aria-label={t("chat.previewImage", { name: image.name })}
            onClick={() => {
              setSelected(index);
              setError(null);
            }}
          >
            <ImagePreview src={image.src} name={image.name} compact={compact} />
          </button>
        ))}
      </div>
      <Dialog
        open={Boolean(active)}
        onOpenChange={(open) => {
          if (!open) setSelected(null);
        }}
      >
        <DialogContent className="sm:max-w-3xl">
          <DialogTitle className="pr-10 text-sm normal-case">{active?.name}</DialogTitle>
          {active && <ImagePreview key={active.id} src={active.src} name={active.name} large />}
          <div className="flex items-center justify-between gap-2">
            <div className="flex items-center gap-2">
              <Button
                type="button"
                size="icon-xs"
                variant="outline"
                disabled={selected === 0}
                aria-label={t("chat.previousImage")}
                onClick={() => {
                  setSelected((index) => Math.max(0, (index ?? 0) - 1));
                  setError(null);
                }}
              >
                <ChevronLeft className="size-4" />
              </Button>
              <span className="text-xs text-muted-foreground">
                {(selected ?? 0) + 1} / {items.length}
              </span>
              <Button
                type="button"
                size="icon-xs"
                variant="outline"
                disabled={selected === items.length - 1}
                aria-label={t("chat.nextImage")}
                onClick={() => {
                  setSelected((index) => Math.min(items.length - 1, (index ?? 0) + 1));
                  setError(null);
                }}
              >
                <ChevronRight className="size-4" />
              </Button>
            </div>
            <Button
              type="button"
              size="xs"
              variant="outline"
              disabled={downloading}
              onClick={() => void download()}
            >
              {downloading ? (
                <Loader2 className="size-3 animate-spin" />
              ) : (
                <Download className="size-3" />
              )}
              {t("chat.downloadImage")}
            </Button>
          </div>
          {error && (
            <DismissibleNotice message={error} className="text-xs text-destructive">
              {error}
            </DismissibleNotice>
          )}
        </DialogContent>
      </Dialog>
    </>
  );
}
