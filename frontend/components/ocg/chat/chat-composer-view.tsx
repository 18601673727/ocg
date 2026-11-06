"use client";

import { DismissibleNotice } from "@/components/ocg/primitives/dismissible-notice";
import { useState } from "react";
import type { ComponentPropsWithRef, ComponentPropsWithoutRef, ReactNode } from "react";
import {
  ArrowUp,
  CircleAlert,
  Command,
  Eye,
  ImagePlus,
  ListPlus,
  Loader2,
  Sparkles,
  Square,
} from "lucide-react";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { SELECTED_ROW, TEXT_TONE } from "@/components/ocg/primitives";
import { runtimeStatusDetail, useI18n, type TranslateFn } from "../i18n";
import type { RuntimeStatus } from "../types";
import { useConversationError } from "./conversation-errors";
import { MarkdownBody } from "@/components/ocg/markdown/markdown-body";

type ComposerFormProps = Pick<
  ComponentPropsWithoutRef<"form">,
  "onSubmit" | "onDragEnter" | "onDragOver" | "onDragLeave" | "onDrop"
>;

function composerStatus(t: TranslateFn, status: RuntimeStatus): string | undefined {
  switch (status.state) {
    case "connected":
      return undefined;
    case "connecting":
      return t("state.connecting");
    case "failed":
      return t("chat.unavailable");
    case "disconnected":
      return `${t("chat.unavailable")} · ${runtimeStatusDetail(t, status) ?? t("chat.unavailableFallback")}`;
  }
}

export function ChatComposerView({
  form,
  dragging,
  attachmentInput,
  children,
  runtimeStatus,
}: {
  form: ComposerFormProps;
  dragging: boolean;
  attachmentInput: ReactNode;
  children: ReactNode;
  runtimeStatus: RuntimeStatus;
}) {
  const { t } = useI18n();
  const statusMessage = composerStatus(t, runtimeStatus);
  return (
    <div className="shrink-0 bg-linear-to-b from-transparent to-background to-[1.5rem] pt-2 pb-[max(0.75rem,env(safe-area-inset-bottom))]">
      <form {...form} className="@container/composer relative mx-auto w-full">
        {dragging && (
          <div
            className="pointer-events-none absolute inset-0 z-30 flex items-center justify-center gap-2 rounded-xl border-2 border-dashed border-primary bg-background/95 text-sm font-medium text-primary"
            role="status"
          >
            <ImagePlus className="size-5" aria-hidden="true" />
            {t("chat.dropImages")}
          </div>
        )}
        {attachmentInput}
        {children}
        {statusMessage && (
          <p
            id="composer-status"
            role="status"
            className="mt-2 flex items-start gap-1.5 px-1 text-xs leading-5 text-muted-foreground"
          >
            {runtimeStatus.state === "connecting" ? (
              <Loader2
                className="mt-0.5 size-4 shrink-0 motion-safe:animate-spin"
                aria-hidden="true"
              />
            ) : (
              <CircleAlert className="mt-0.5 size-4 shrink-0" aria-hidden="true" />
            )}
            {statusMessage}
          </p>
        )}
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

type ComposerTextareaProps = Pick<
  ComponentPropsWithRef<"textarea">,
  "ref" | "readOnly" | "onChange" | "onPaste" | "onKeyDown" | "aria-describedby"
> & { value: string };

export function ChatComposerInput({
  textarea,
  suggestions,
  error,
  selectors,
  actions,
}: {
  textarea: ComposerTextareaProps;
  suggestions?: {
    items: readonly ComposerSuggestionOption[];
    activeIndex: number;
    onSelect: (index: number) => void;
  };
  error: string | null;
  selectors: ReactNode;
  actions: {
    busy: boolean;
    queueing: boolean;
    submitting: boolean;
    cancelling: boolean;
    attachDisabled: boolean;
    sendDisabled: boolean;
    steerDisabled: boolean;
    steerTitle?: string;
    onAttach: () => void;
    onSteer: () => void;
    onCancel?: () => void;
  };
}) {
  const { t } = useI18n();
  const [preview, setPreview] = useState(false);
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
                      "flex w-full items-start gap-2 px-2.5 py-2 text-left transition-colors",
                      selected ? cn(SELECTED_ROW, "bg-primary/15") : "hover:bg-muted",
                    )}
                  >
                    <span className="mt-0.5 text-muted-foreground" aria-hidden="true">
                      {suggestion.icon === "job" ? (
                        <Sparkles className="size-3.5" />
                      ) : (
                        <Command className="size-3.5" />
                      )}
                    </span>
                    <span className="min-w-0 flex-1">
                      <span className="block text-xs font-medium">{suggestion.label}</span>
                      <span className="block truncate text-xs text-muted-foreground">
                        {suggestion.description}
                      </span>
                    </span>
                    {suggestion.command !== suggestion.label && (
                      <span className="mt-0.5 shrink-0 font-mono text-xs text-muted-foreground">
                        {suggestion.command.trim()}
                      </span>
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
          aria-activedescendant={
            activeSuggestion ? `composer-suggestion-${activeSuggestion.id}` : undefined
          }
          aria-invalid={Boolean(error)}
          className="relative z-10 block max-h-[min(10rem,25dvh)] min-h-14 w-full resize-none bg-transparent px-4 py-3 text-base leading-6 text-foreground outline-none placeholder:text-muted-foreground [@media(max-height:600px)]:max-h-[18dvh]"
        />
      </div>
      {preview && (
        <div
          id="composer-preview"
          role="region"
          aria-label={t("chat.markdownPreview")}
          className="max-h-[min(10rem,25dvh)] overflow-auto border-t border-border px-3 py-2"
        >
          <MarkdownBody content={textarea.value} />
        </div>
      )}
      {error && !errorInHeader && (
        <DismissibleNotice
          message={error}
          className={`border-t border-border px-3 py-1.5 text-xs ${TEXT_TONE.red}`}
        >
          {error}
        </DismissibleNotice>
      )}
      <div className="flex flex-wrap items-center gap-x-2 gap-y-1 rounded-b-xl border-t border-border/70 bg-muted/30 p-2">
        <div className="flex w-full min-w-0 @xl/composer:w-auto @xl/composer:flex-1">
          {selectors}
        </div>
        <Button
          type="button"
          variant="ghost"
          size="icon-xs"
          className="size-11 rounded-lg text-muted-foreground pointer-fine:size-9"
          aria-label={t("chat.attachFile")}
          title={t("chat.attachFile")}
          disabled={actions.attachDisabled}
          onClick={actions.onAttach}
        >
          <ImagePlus className="size-4" />
        </Button>
        <Button
          type="button"
          variant={preview ? "selected" : "ghost"}
          size="icon-xs"
          className="size-11 rounded-lg text-muted-foreground aria-pressed:text-primary pointer-fine:size-9"
          aria-label={t("chat.markdownPreview")}
          aria-pressed={preview}
          aria-controls={preview ? "composer-preview" : undefined}
          title={t("chat.markdownPreview")}
          onClick={() => setPreview((current) => !current)}
        >
          <Eye className="size-4" />
        </Button>
        <div className="ml-auto flex flex-wrap items-center justify-end gap-1 @xl/composer:border-l @xl/composer:border-border @xl/composer:pl-2">
          {actions.busy && actions.onCancel && (
            <Button
              type="button"
              variant="outline"
              size="xs"
              className="h-11 rounded-lg px-2 tracking-normal normal-case pointer-fine:h-9"
              disabled={actions.submitting || actions.cancelling}
              onClick={actions.onCancel}
              title={t("chat.cancel")}
            >
              {actions.cancelling ? (
                <Loader2 className="size-3 animate-spin" />
              ) : (
                <Square className="size-3" />
              )}
              {t("chat.cancel")}
            </Button>
          )}

          {actions.busy && (
            <Button
              type="button"
              variant="outline"
              size="xs"
              className="h-11 rounded-lg px-2 tracking-normal normal-case pointer-fine:h-9"
              disabled={actions.steerDisabled}
              onClick={actions.onSteer}
              title={actions.steerTitle ?? t("chat.steer")}
            >
              <ArrowUp className="size-3" />
              {t("chat.steer")}
            </Button>
          )}
          <Button
            type="submit"
            size={actions.queueing ? "xs" : "icon-sm"}
            className={cn(
              "h-11 rounded-lg tracking-normal normal-case pointer-fine:h-9",
              !actions.queueing && "w-11 pointer-fine:w-9",
            )}
            disabled={actions.sendDisabled}
            aria-label={t(actions.queueing ? "chat.queue" : "chat.send")}
            title={t(actions.queueing ? "chat.queueHint" : "chat.send")}
          >
            {actions.submitting ? (
              <Loader2 className="size-4 animate-spin" />
            ) : actions.queueing ? (
              <ListPlus className="size-4" />
            ) : (
              <ArrowUp className="size-4" />
            )}
            {actions.queueing && t("chat.queue")}
          </Button>
        </div>
      </div>
    </>
  );
}
