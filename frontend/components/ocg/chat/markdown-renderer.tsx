'use client';

import { ImageGallery } from '@/components/ocg/chat/image-attachments';
import { MarkdownBody } from '@/components/ocg/markdown/markdown-body';
import type { ChatImage } from '@/components/ocg/contracts';

interface MarkdownRendererProps {
  content: string;
  images?: ChatImage[];
  inMuted?: boolean;
}

/**
 * The legacy Chat message body.
 *
 * Parsing, math, diagrams, highlighting, link and image safety, and copy
 * controls all live in the shared Markdown pipeline; this keeps only the
 * image gallery, which is specific to a stored Chat turn.
 */
export function MarkdownRenderer({ content, images, inMuted = false }: MarkdownRendererProps) {
  return (
    <>
      <MarkdownBody content={content} inMuted={inMuted} />
      {images && images.length > 0 && <ImageGallery images={images} />}
    </>
  );
}
