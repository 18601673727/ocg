"use client";

import type { ComponentPropsWithRef, ComponentPropsWithoutRef, ReactNode } from "react";
import { ArrowUp, Command, ListPlus, Loader2, Paperclip, Sparkles, Square } from "lucide-react";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { STATUS_TEXT } from "@/components/ocg/primitives";
import { useI18n } from "../i18n";
import { useConversationError } from "./conversation-errors";

type ComposerFormProps = Pick<ComponentPropsWithoutRef<"form">,
  "onSubmit" | "onDragEnter" | "onDragOver" | "onDragLeave" | "onDrop"
>;

export function ChatComposerView({ form, dragging, attachmentInput, selectors, children, unavailableMessage }: {
  form: ComposerFormProps;
  dragging: boolean;
  attachmentInput: ReactNode;
  selectors: ReactNode;
  children: ReactNode;
  unavailableMessage?: string;
}) {
  const { t } = useI18n();
  return (
    <div className="shrink-0 bg-transparent px-3 pt-2 pb-[max(0.75rem,env(safe-area-inset-bottom))] sm:px-5">
      <form {...form} className="relative mx-auto w-full">
        {dragging && <div className="pointer-events-none absolute inset-0 z-30 flex items-center justify-center rounded-lg border-2 border-dashed border-primary bg-background/95 text-sm font-medium" role="status">{t("chat.dropImages")}</div>}
        {attachmentInput}
        {selectors}
        {children}
        {unavailableMessage && <p id="composer-status" role="status" className="mt-1.5 text-center text-xs text-muted-foreground">
          {unavailableMessage}
        </p>}
      </form>
    </div>
  );
}

type ComposerSuggestionOption = {
  id: string;
  label: string;
  description: string;
  command: string;
  icon: "job" | "command";
};

type ComposerTextareaProps = Pick<ComponentPropsWithRef<"textarea">,
  "ref" | "readOnly" | "onChange" | "onPaste" | "onKeyDown" | "aria-describedby"
> & { value: string };

export function ChatComposerInput({ textarea, suggestions, error, actions }: {
  textarea: ComposerTextareaProps;
  suggestions?: {
    items: readonly ComposerSuggestionOption[];
    activeIndex: number;
    onSelect: (index: number) => void;
  };
  error: string | null;
  actions: {
    busy: boolean;
    queueing: boolean;
    submitting: boolean;
    cancelling: boolean;
    sendDisabled: boolean;
    steerDisabled: boolean;
    onAttach: () => void;
    onSteer: () => void;
    onCancel?: () => void;
  };
}) {
  const { t } = useI18n();
  const activeSuggestion = suggestions?.items[suggestions.activeIndex];
  const errorInHeader = useConversationError(error);
  return (
    <>
      <div className="relative">
        {suggestions && (
          <ul
            id="composer-suggestions"
            role="listbox"
            aria-label={t("chat.composerSuggestions")}
            className="absolute inset-x-1 bottom-full z-20 mb-1 overflow-hidden rounded-md border border-border bg-popover text-popover-foreground shadow-md"
          >
            {suggestions.items.map((suggestion, index) => {
              const selected = suggestions.activeIndex === index;
              return (
                <li key={suggestion.id} role="presentation">
                  <button
                    id={`composer-suggestion-${suggestion.id}`}
                    type="button"
                    role="option"
                    aria-selected={selected}
                    onMouseDown={(event) => event.preventDefault()}
                    onClick={() => suggestions.onSelect(index)}
                    className={cn(
                      "flex w-full items-start gap-2 px-2.5 py-2 text-left transition-colors hover:bg-muted",
                      selected && "bg-muted",
                    )}
                  >
                    <span className="mt-0.5 text-muted-foreground" aria-hidden="true">
                      {suggestion.icon === "job"
                        ? <Sparkles className="size-3.5" />
                        : <Command className="size-3.5" />}
                    </span>
                    <span className="min-w-0 flex-1">
                      <span className="block text-xs font-medium">{suggestion.label}</span>
                      <span className="block truncate text-xs text-muted-foreground">{suggestion.description}</span>
                    </span>
                    {suggestion.command !== suggestion.label && (
                      <span className="mt-0.5 shrink-0 font-mono text-xs text-muted-foreground">{suggestion.command.trim()}</span>
                    )}
                  </button>
                </li>
              );
            })}
          </ul>
        )}
        <textarea
          {...textarea}
          rows={1}
          placeholder={t("chat.messagePlaceholder")}
          aria-label={t("chat.messageLabel")}
          role="combobox"
          aria-autocomplete="list"
          aria-expanded={Boolean(suggestions)}
          aria-controls={suggestions ? "composer-suggestions" : undefined}
          aria-activedescendant={activeSuggestion ? `composer-suggestion-${activeSuggestion.id}` : undefined}
          aria-invalid={Boolean(error)}
          className="max-h-[min(10rem,25dvh)] min-h-11 w-full resize-none bg-transparent px-3 pt-2.5 pb-1 text-base outline-none placeholder:text-muted-foreground sm:text-sm [@media(max-height:600px)]:max-h-[18dvh]"
        />
      </div>
      {error && !errorInHeader && (
        <p role="alert" className={`border-t border-border px-3 py-1.5 text-xs ${STATUS_TEXT['error']}`}>
          {error}
        </p>
      )}
      <div className="flex flex-wrap items-center gap-1 px-2 pb-2">
        <Button
          type="button"
          variant="ghost"
          size="icon-xs"
          className="size-11 rounded-md sm:size-9"
          aria-label={t("chat.attachFile")}
          title={t("chat.attachFile")}
          disabled={actions.submitting || actions.cancelling}
          onClick={actions.onAttach}
        >
          <Paperclip className="size-4" />
        </Button>
        <span id="composer-hint" className="sr-only sm:ml-1 sm:not-sr-only sm:text-xs sm:text-muted-foreground">
          <span className="hidden pointer-coarse:inline">{t("chat.touchHint")}</span>
          <span className="pointer-coarse:hidden">{actions.busy ? t("chat.queueHint") : t("chat.hint")}</span>
        </span>
        <div className="ml-auto flex flex-wrap items-center justify-end gap-1">
          {actions.busy && actions.onCancel && <Button type="button" variant="outline" size="xs" className="h-11 rounded-md px-2 tracking-normal normal-case sm:h-9" disabled={actions.submitting || actions.cancelling} onClick={actions.onCancel} title={t("chat.cancel")}>
            {actions.cancelling ? <Loader2 className="size-3 animate-spin" /> : <Square className="size-3" />}{t("chat.cancel")}
          </Button>}

          <Button type="submit" size={actions.queueing ? "xs" : "icon-sm"} className={cn("h-11 rounded-md tracking-normal normal-case sm:h-9", !actions.queueing && "w-11 sm:w-9")} disabled={actions.sendDisabled} aria-label={t(actions.queueing ? "chat.queue" : "chat.send")} title={t(actions.busy ? "chat.queueHint" : "chat.send")}>
            {actions.submitting ? <Loader2 className="size-4 animate-spin" /> : actions.queueing ? <ListPlus className="size-4" /> : <ArrowUp className="size-4" />}
            {actions.queueing && t("chat.queue")}
          </Button>
        </div>
      </div>
    </>
  );
}
