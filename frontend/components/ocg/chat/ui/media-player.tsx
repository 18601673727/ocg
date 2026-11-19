export function AudioPlayer(props: { src: string; title?: string }) {
  return <audio controls class="w-full" src={props.src} aria-label={props.title} />;
}
export function VideoPlayer(props: { src: string; title?: string }) {
  return <video controls class="max-h-80 w-full" src={props.src} aria-label={props.title} />;
}
