/**
 * The single tone vocabulary shared by every OCG surface.
 *
 * Status semantics live in `status-tone.ts`; this module only says what a tone
 * looks like. Surfaces compose these classes instead of naming colors inline so
 * that a status means the same thing in the sidebar, the ledger and the
 * inspector.
 */

export type Tone = "emerald" | "sky" | "amber" | "red" | "violet" | "slate";

/** Tinted border + background, no text colour. Use when the text must stay neutral. */
export const SURFACE_TONE: Record<Tone, string> = {
  emerald: "border-emerald-500/30 bg-emerald-500/10",
  sky: "border-sky-500/30 bg-sky-500/10",
  amber: "border-amber-500/30 bg-amber-500/10",
  red: "border-red-500/30 bg-red-500/10",
  violet: "border-violet-500/30 bg-violet-500/10",
  slate: "border-border bg-muted/50",
};

/** Readable foreground for a tone, in both themes. */
export const TEXT_TONE: Record<Tone, string> = {
  emerald: "text-emerald-700 dark:text-emerald-400",
  sky: "text-sky-700 dark:text-sky-400",
  amber: "text-amber-700 dark:text-amber-400",
  red: "text-red-700 dark:text-red-400",
  violet: "text-violet-700 dark:text-violet-400",
  slate: "text-muted-foreground",
};

/** Solid dot fill for a tone. */
export const DOT_TONE: Record<Tone, string> = {
  emerald: "bg-emerald-500",
  sky: "bg-sky-500",
  amber: "bg-amber-500",
  red: "bg-red-500",
  violet: "bg-violet-500",
  slate: "bg-muted-foreground/60",
};

/** Surface plus foreground, the shape a pill or badge wants. */
export const TONE_CLASS: Record<Tone, string> = Object.fromEntries(
  (Object.keys(SURFACE_TONE) as Tone[]).map((tone) => [tone, `${SURFACE_TONE[tone]} ${TEXT_TONE[tone]}`]),
) as Record<Tone, string>;

/** Text-only emphasis for a tone, used by the quiet pill variant. */
export function textTone(tone: Tone): string {
  return TEXT_TONE[tone];
}
