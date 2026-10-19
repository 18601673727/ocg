import type {
  ControlProjectUpdate,
  ControlPoolUpdate,
  ControlConversationEdit,
} from "../components/ocg/runtime/control-client";

// Item 2: Projects
export const validProjectUpdate = {
  name: "project-1",
  description: null,
  commands: [{ name: "build", argv: ["cargo", "build"] }],
} satisfies ControlProjectUpdate;

// @ts-expect-error on an update without commands
export const projectUpdateWithoutCommands: ControlProjectUpdate = { name: "project-1", description: null };

// @ts-expect-error on an update that has root
export const projectUpdateWithRoot: ControlProjectUpdate = { name: "project-1", description: null, commands: [], root: "/tmp" };

// Item 3: Pools
export const validPoolUpdateConcurrency = { concurrency_limit: 2 } satisfies ControlPoolUpdate;

export const validPoolUpdateEnabled = { enabled: false } satisfies ControlPoolUpdate;

// @ts-expect-error on {}
export const poolUpdateEmpty: ControlPoolUpdate = {};

// @ts-expect-error on a ControlPoolUpdate with both fields
export const poolUpdateBoth: ControlPoolUpdate = { concurrency_limit: 2, enabled: false };

// Item 4: Conversation edits
export const validConversationEditTitle = { title: "x" } satisfies ControlConversationEdit;

export const validConversationEditHide = { hidden: true } satisfies ControlConversationEdit;

// @ts-expect-error on { hidden: false }
export const conversationEditHiddenFalse: ControlConversationEdit = { hidden: false };

// @ts-expect-error on { title: "x", hidden: true }
export const conversationEditBoth: ControlConversationEdit = { title: "x", hidden: true };
