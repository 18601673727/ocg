export function AudioPlayer({ src, title }: { src: string; title?: string }) {
  // biome-ignore lint/a11y/useMediaCaption: Audio attachments have no associated caption track.
  return <audio controls className="w-full" src={src} aria-label={title} />;
}
export function VideoPlayer({ src, title }: { src: string; title?: string }) {
  // biome-ignore lint/a11y/useMediaCaption: Video attachments have no associated caption track.
  return <video controls className="max-h-80 w-full" src={src} aria-label={title} />;
}
