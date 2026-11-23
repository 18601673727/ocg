import { type Ref } from "@/lib/ui-state";
import { ModelSelector } from "../model-selector";
import type { ComposerDraft, ComposerProps } from "./draft-state";

export function ComposerSelectors(props: {
  composer: ComposerProps;
  draft: ComposerDraft;
  trigger: Ref<HTMLButtonElement>;
}) {
  const { draft, composer } = props;
  let explicit = false;
  return (
    <div
      class="min-w-0 flex-1"
      onPointerDown={() => {
        explicit = true;
      }}
      onKeyDown={() => {
        explicit = true;
      }}
    >
      <ModelSelector
        triggerRef={props.trigger}
        sessionId={composer.sessionId}
        selection={composer.selection}
        onChange={(value) => {
          const projectChanged = value.project !== composer.selection?.project;
          composer.onSelectionChange(value);
          if (draft.ready() && explicit) {
            explicit = false;
            if (!projectChanged) draft.configure(value);
          }
        }}
        onPreference={composer.onPreference}
        onImageSupportChange={draft.imageSupport}
        busy={draft.busy() || !draft.ready() || draft.uncertain() || draft.conflict()}
        statusText={
          draft.conflict()
            ? "Review the Job conflict before continuing."
            : draft.uncertain()
              ? "Waiting to reconcile submission."
              : draft.busy()
                ? "Saving Job…"
                : undefined
        }
        projectLocked={Boolean(composer.conversation || draft.confirmed() || draft.association())}
        lockedProject={
          draft.confirmed()
            ? draft.confirmed()!.project
            : composer.conversation
              ? composer.project
              : draft.association()?.project
        }
      />
    </div>
  );
}
