/**
 * The single tone vocabulary shared by every OCG surface.
 *
 * Status semantics live in `status-tone.ts`; this module only says what a tone
 * looks like. Surfaces compose these classes instead of naming colors inline so
 * that a status means the same thing in the sidebar, the ledger and the
 * inspector.
 */

export type Tone = "emerald" | "sky" | "amber" | "red" | "violet" | "slate";

/** Tinted border on its own, for a badge that keeps a neutral background. */
export const BORDER_TONE: Record<Tone, string> = {
  emerald: "border-emerald-500/30",
  sky: "border-sky-500/30",
  amber: "border-amber-500/30",
  red: "border-red-500/30",
  violet: "border-violet-500/30",
  slate: "border-border",
};

/** Tinted background on its own, for a row that already has a border. */
export const FILL_TONE: Record<Tone, string> = {
  emerald: "bg-emerald-500/10",
  sky: "bg-sky-500/10",
  amber: "bg-amber-500/10",
  red: "bg-red-500/10",
  violet: "bg-violet-500/10",
  slate: "bg-muted/50",
};

/** Tinted border + background, no text colour. Use when the text must stay neutral. */
export const SURFACE_TONE: Record<Tone, string> = Object.fromEntries(
  (Object.keys(BORDER_TONE) as Tone[]).map((tone) => [tone, `${BORDER_TONE[tone]} ${FILL_TONE[tone]}`]),
) as Record<Tone, string>;

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
