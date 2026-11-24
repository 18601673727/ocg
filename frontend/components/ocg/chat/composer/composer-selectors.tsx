import { type Ref } from "@/lib/ui-state";
import { useI18n } from "../../i18n";
import { ModelSelector } from "../model-selector";
import type { ComposerDraft, ComposerProps } from "./draft-state";

export function ComposerSelectors(props: {
  composer: ComposerProps;
  draft: ComposerDraft;
  trigger: Ref<HTMLButtonElement>;
}) {
  const { t } = useI18n();
  const { draft, composer } = props;
  let explicit = false;
  return (
    <div
      class="min-w-0 max-w-full shrink"
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
        onSupportChange={draft.modelSupport}
        busy={draft.busy() || !draft.ready() || draft.uncertain() || draft.conflict()}
        statusText={
          draft.conflict()
            ? t("chat.draft.reviewConflict")
            : draft.uncertain()
              ? t("chat.draft.waitReconcile")
              : draft.busy()
                ? t("chat.draft.saving")
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
