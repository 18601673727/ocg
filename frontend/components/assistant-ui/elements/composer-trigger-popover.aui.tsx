"use client";

import { memo, type ComponentPropsWithoutRef, type FC } from "react";
import {
  ComposerPrimitive,
  defaultDirectiveFormatter,
  type DirectiveFormatter,
  type TriggerItem,
} from "@assistant-ui/react";
import { ChevronLeftIcon, ChevronRightIcon, SparklesIcon } from "lucide-react";
import { cn } from "@/lib/utils";

type IconComponent = FC<{ className?: string }>;

type DirectiveBehaviorProps = {
  /** Serializes the inserted directive. */
  formatter?: DirectiveFormatter | undefined;
  /** Runs after the composer inserts the directive. */
  onInserted?: ((item: TriggerItem) => void) | undefined;
};

type ActionBehaviorProps = {
  /** Serializes the chip retained when `removeOnExecute` is false. */
  formatter?: DirectiveFormatter | undefined;
  onExecute: (item: TriggerItem) => void;
  /** Removes the trigger text after the action runs. */
  removeOnExecute?: boolean | undefined;
};

type ComposerTriggerPopoverBaseProps = Omit<
  ComponentPropsWithoutRef<typeof ComposerPrimitive.TriggerPopover>,
  "children"
> & {
  /** Items use `metadata.icon` as the key. Categories use their ID. */
  iconMap?: Record<string, IconComponent>;
  fallbackIcon?: IconComponent;
  backLabel?: string;
  emptyCategoriesLabel?: string;
  emptyItemsLabel?: string;
  loadingLabel?: string;
};

type ComposerTriggerPopoverProps = ComposerTriggerPopoverBaseProps &
  (
    | {
        directive: DirectiveBehaviorProps;
        action?: never;
      }
    | {
        action: ActionBehaviorProps;
        directive?: never;
      }
  );

function resolveIcon(
  iconKey: string | undefined,
  iconMap: Record<string, IconComponent> | undefined,
  fallback: IconComponent,
): IconComponent {
  if (iconKey && iconMap?.[iconKey]) return iconMap[iconKey]!;
  return fallback;
}

type CategoriesProps = {
  iconMap: Record<string, IconComponent> | undefined;
  fallbackIcon: IconComponent;
  emptyLabel: string;
};

const Categories: FC<CategoriesProps> = ({ iconMap, fallbackIcon, emptyLabel }) => (
  <ComposerPrimitive.TriggerPopoverCategories>
    {(categories) => (
      <div data-slot="composer-trigger-popover-categories" className="flex flex-col py-1">
        {categories.map((cat) => {
          const Icon = resolveIcon(cat.id, iconMap, fallbackIcon);
          return (
            <ComposerPrimitive.TriggerPopoverCategoryItem
              key={cat.id}
              categoryId={cat.id}
              className="hover:bg-accent focus:bg-accent data-[highlighted]:bg-accent flex items-center justify-between gap-2 px-3 py-2 text-sm transition-colors outline-none"
            >
              <span className="flex items-center gap-2">
                <Icon className="text-muted-foreground size-4" />
                {cat.label}
              </span>
              <ChevronRightIcon className="text-muted-foreground size-4" />
            </ComposerPrimitive.TriggerPopoverCategoryItem>
          );
        })}
        {categories.length === 0 && (
          <div className="text-muted-foreground px-3 py-2 text-sm">{emptyLabel}</div>
        )}
      </div>
    )}
  </ComposerPrimitive.TriggerPopoverCategories>
);

type ItemsProps = {
  iconMap: Record<string, IconComponent> | undefined;
  fallbackIcon: IconComponent;
  backLabel: string;
  emptyLabel: string;
  loadingLabel: string;
  isLoading: boolean;
};

const Items: FC<ItemsProps> = ({
  iconMap,
  fallbackIcon,
  backLabel,
  emptyLabel,
  loadingLabel,
  isLoading,
}) => {
  return (
    <ComposerPrimitive.TriggerPopoverItems>
      {(items) => (
        <div data-slot="composer-trigger-popover-items" className="flex flex-col">
          <ComposerPrimitive.TriggerPopoverBack className="text-muted-foreground hover:bg-accent flex items-center gap-1.5 border-b px-3 py-2 text-xs tracking-wide uppercase transition-colors">
            <ChevronLeftIcon className="size-3.5" />
            {backLabel}
          </ComposerPrimitive.TriggerPopoverBack>

          <div className="py-1">
            {items.map((item, index) => {
              const iconKey =
                typeof item.metadata?.icon === "string" ? item.metadata.icon : undefined;
              const Icon = resolveIcon(iconKey, iconMap, fallbackIcon);
              return (
                <ComposerPrimitive.TriggerPopoverItem
                  key={item.id}
                  item={item}
                  index={index}
                  className="hover:bg-accent focus:bg-accent data-[highlighted]:bg-accent flex w-full flex-col items-start gap-0.5 px-3 py-2 text-start transition-colors outline-none"
                >
                  <span className="flex items-center gap-2 text-sm font-medium">
                    <Icon className="text-primary size-3.5" />
                    {item.label}
                  </span>
                  {item.description && (
                    <span className="text-muted-foreground ms-5.5 text-xs leading-tight">
                      {item.description}
                    </span>
                  )}
                </ComposerPrimitive.TriggerPopoverItem>
              );
            })}
            {items.length === 0 && (
              <div className="text-muted-foreground px-3 py-2 text-sm">
                {isLoading ? loadingLabel : emptyLabel}
              </div>
            )}
          </div>
        </div>
      )}
    </ComposerPrimitive.TriggerPopoverItems>
  );
};

const ComposerTriggerPopoverImpl: FC<ComposerTriggerPopoverProps> = ({
  iconMap,
  fallbackIcon = SparklesIcon,
  backLabel = "Back",
  emptyCategoriesLabel = "No items available",
  emptyItemsLabel = "No matching items",
  loadingLabel = "Loading…",
  isLoading = false,
  className,
  directive,
  action,
  ...props
}) => {
  return (
    <ComposerPrimitive.TriggerPopover
      data-slot="composer-trigger-popover"
      className={cn(
        "aui-composer-trigger-popover bg-popover text-popover-foreground absolute start-0 bottom-full z-50 mb-2 w-64 overflow-hidden rounded-xl border",
        className,
      )}
      isLoading={isLoading}
      {...props}
    >
      {directive ? (
        <ComposerPrimitive.TriggerPopover.Directive
          formatter={directive.formatter ?? defaultDirectiveFormatter}
          onInserted={directive.onInserted}
        />
      ) : action ? (
        <ComposerPrimitive.TriggerPopover.Action
          formatter={action.formatter ?? defaultDirectiveFormatter}
          onExecute={action.onExecute}
          removeOnExecute={action.removeOnExecute}
        />
      ) : null}
      <Categories iconMap={iconMap} fallbackIcon={fallbackIcon} emptyLabel={emptyCategoriesLabel} />
      <Items
        iconMap={iconMap}
        fallbackIcon={fallbackIcon}
        backLabel={backLabel}
        emptyLabel={emptyItemsLabel}
        loadingLabel={loadingLabel}
        isLoading={isLoading}
      />
    </ComposerPrimitive.TriggerPopover>
  );
};
ComposerTriggerPopoverImpl.displayName = "ComposerTriggerPopover";

export const ComposerTriggerPopover = memo(
  ComposerTriggerPopoverImpl,
) as FC<ComposerTriggerPopoverProps>;
