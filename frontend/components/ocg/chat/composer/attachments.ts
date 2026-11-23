import { createEffect, createSignal } from "solid-js";
import { elementRef } from "@/lib/ui-state";
import type { ChatImage } from "../../contracts";
import type { ApiClient } from "../../runtime/api-client";
import type { ConversationRuntimeClient } from "../../runtime/conversation-client";
export const IMAGE_TYPES = ["image/png", "image/jpeg", "image/gif", "image/webp"];
export type PendingImage = { file: File; url: string; failed: boolean };
function fileData(file: File): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () =>
      typeof reader.result === "string"
        ? resolve(reader.result)
        : reject(new Error("Unable to read image."));
    reader.onerror = () => reject(reader.error ?? new Error("Unable to read image."));
    reader.readAsDataURL(file);
  });
}

export async function loadImages(api: ApiClient, ids: string[]) {
  return Promise.all(
    ids.map(async (id) => {
      const item = await api.readAttachment(id);
      if (!item.available) throw new Error(`Attachment ${id} is unavailable.`);
      return {
        id,
        name: item.display_name ?? "Image",
        media_type: item.mime,
        url: api.attachmentContentUrl(id),
      };
    }),
  );
}
export function createAttachmentQueue(
  client: ConversationRuntimeClient,
  isAlive: () => boolean,
  canAttach: () => boolean,
  savedCount: () => number,
  onPendingChange: (pending: PendingImage[]) => void,
  onUpload: (image: ChatImage) => void,
  setError: (message: string) => void,
) {
  const [uploading, setUploading] = createSignal(0);
  const [pendingImages, setPendingImages] = createSignal<PendingImage[]>([]);
  const pendingImagesRef = elementRef<PendingImage[]>([]);
  const uploadTask = elementRef<Promise<void> | null>(null);
  createEffect(
    () => [],
    () => () => {
      for (const pending of pendingImagesRef.current) URL.revokeObjectURL(pending.url);
    },
  );
  function setPending(next: PendingImage[]) {
    pendingImagesRef.current = next;
    if (!isAlive()) return;
    setPendingImages(next);
    onPendingChange(next);
  }
  async function add(pending: PendingImage[]) {
    if (!client.uploadChatImage) {
      setPending(
        pendingImagesRef.current.map((item) =>
          pending.includes(item) ? { ...item, failed: true } : item,
        ),
      );
      setError("Image upload is unavailable.");
      return;
    }
    setUploading((n) => n + pending.length);
    for (const item of pending) {
      try {
        const image = await client.uploadChatImage({
          project_id: "",
          name: item.file.name,
          data_url: await fileData(item.file),
        });
        setPending(pendingImagesRef.current.filter((entry) => entry !== item));
        URL.revokeObjectURL(item.url);
        onUpload(image);
      } catch (cause) {
        setPending(
          pendingImagesRef.current.map((entry) =>
            entry === item ? { ...entry, failed: true } : entry,
          ),
        );
        if (isAlive()) setError(cause instanceof Error ? cause.message : String(cause));
      } finally {
        if (isAlive()) setUploading((n) => n - 1);
      }
    }
  }
  function startUploads(items: PendingImage[]) {
    if (!items.length || uploadTask.current) return;
    const task = add(items);
    uploadTask.current = task;
    void task.finally(() => {
      if (uploadTask.current === task) uploadTask.current = null;
    });
  }
  function selectImages(files: readonly File[]) {
    if (!files.length || !canAttach() || uploadTask.current) return;
    if (savedCount() + pendingImagesRef.current.length + files.length > 8) {
      setError("At most eight images are supported.");
      return;
    }
    const selected = files.map((file) => ({
      file,
      url: URL.createObjectURL(file),
      failed: false,
    }));
    setPending([...pendingImagesRef.current, ...selected]);
    startUploads(selected);
  }
  function removePending(item: PendingImage) {
    setPending(pendingImagesRef.current.filter((entry) => entry !== item));
    URL.revokeObjectURL(item.url);
  }
  function retryUploads() {
    const retry = pendingImagesRef.current
      .filter((item) => item.failed)
      .map((item) => ({ ...item, failed: false }));
    setPending([...pendingImagesRef.current.filter((item) => !item.failed), ...retry]);
    startUploads(retry);
  }
  function paste(event: ClipboardEvent) {
    if (!canAttach()) return;
    const images = imageFiles(event.clipboardData?.files);
    if (!images.length) return;
    event.preventDefault();
    selectImages(images);
  }
  return {
    pendingImages,
    pendingImagesRef,
    uploading,
    uploadTask,
    selectImages,
    removePending,
    retryUploads,
    paste,
  };
}

export function imageFiles(files?: FileList) {
  return Array.from(files ?? []).filter((file) => IMAGE_TYPES.includes(file.type));
}
