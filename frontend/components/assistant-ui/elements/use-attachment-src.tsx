"use client";

import { useEffect, useState } from "react";
import type { Attachment } from "@assistant-ui/react";

export const useAttachmentSrc = (attachment: Attachment) => {
  const [entry, setEntry] = useState<
    { file: File; src: string } | undefined
  >(undefined);

  useEffect(() => {
    if (!attachment.file) {
      return;
    }

    const objectUrl = URL.createObjectURL(attachment.file);
    // Object URLs must be created after commit and retained until cleanup.
    // eslint-disable-next-line react-hooks/set-state-in-effect
    setEntry({ file: attachment.file, src: objectUrl });

    return () => {
      URL.revokeObjectURL(objectUrl);
    };
  }, [attachment.file]);

  const current = entry;
  if (!current || current.file !== attachment.file) return undefined;
  return current.src;
};
