export function AudioPlayer({ src, title }: { src: string; title?: string }) {
  return <audio controls className="w-full" src={src} aria-label={title} />;
}
export function VideoPlayer({ src, title }: { src: string; title?: string }) {
  return <video controls className="max-h-80 w-full" src={src} aria-label={title} />;
}
