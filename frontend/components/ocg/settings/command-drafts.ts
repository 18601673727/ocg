import type { ControlProjectCommand } from "../runtime/control-client";

export type CommandDraft = {
  /** Stable local identity for row reconciliation; never persisted or sent. */
  id: string;
  name: string;
  argvText: string;
};

/** Next stable draft identity for a command editor session. */
export function nextCommandDraftId(seen: readonly { id: string }[]): string {
  let candidate = seen.length + 1;
  const taken = new Set(seen.map((draft) => draft.id));
  while (taken.has(`command-${candidate}`)) candidate++;
  return `command-${candidate}`;
}

/** Attach stable local identities to commands loaded for editing. */
export function toCommandDrafts(commands: readonly { name: string; argv: readonly string[] }[]): CommandDraft[] {
  return commands.map((cmd, index) => ({
    id: `command-${index + 1}`,
    name: cmd.name,
    argvText: cmd.argv.join("\n"),
  }));
}

/** Strip draft identities back to the persisted/wire command schema. */
export function toProjectCommands(commands: readonly CommandDraft[]): ControlProjectCommand[] {
  return commands.map((cmd) => ({
    name: cmd.name.trim(),
    argv: cmd.argvText
      .split("\n")
      .map((line) => line.replace(/\r$/, ""))
      .filter((line) => line.trim().length > 0),
  }));
}
