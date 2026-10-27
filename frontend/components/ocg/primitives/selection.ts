/**
 * The one spelling of "this is the current one".
 *
 * Every list row, navigation entry and pressed choice named a neutral
 * `bg-muted` inline. `--muted` is not the accent, so none of those surfaces
 * followed an accent change: only solid buttons, borders and focus rings did.
 * These classes derive the mark from `--primary`, which the accent preference
 * owns, so a selected row, the sidebar and the settings navigation change
 * together with the accent instead of each surface deciding for itself.
 *
 * The mark stays a tint rather than a solid fill: a selected row must still
 * read as a selected row and not as a primary button, and its text keeps the
 * regular `foreground` so a light accent cannot wash out a label. These classes
 * carry colour only — weight and padding stay with the surface that already
 * decided them.
 */

/** A row in a list or navigation group that is current. */
export const SELECTED_ROW = "bg-primary/10 text-foreground";

/** A pressed choice inside a group: theme, accent, language, package manager. */
export const SELECTED_OPTION = "border-primary/40 bg-primary/10 text-foreground";
