export function pageTurns<T extends { job: { id: string } }>(
  held: ReadonlySet<string>,
  cursor: string | undefined,
  page: { turns: readonly T[]; has_more: boolean; next_before: string | null },
): { add: T[]; next: string | null } {
  if (page.has_more && (page.next_before === null || page.next_before === cursor)) {
    throw new Error("Conversation turns returned no advancing cursor.");
  }
  return {
    add: page.turns.filter(turn => !held.has(turn.job.id)).reverse(),
    next: page.has_more ? page.next_before : null,
  };
}
