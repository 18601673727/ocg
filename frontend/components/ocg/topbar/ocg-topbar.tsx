"use client";

import {
  Bell,
  ChevronsLeft,
  ChevronsRight,
  Home,
  MoreHorizontal,
  PanelLeft,
  PanelRight,
  ScrollText,
  SlidersHorizontal,
  Settings,
  Table2,
  Workflow,
} from "lucide-react";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import type { ChatSession, RuntimeStatus } from "../types";
import { WORK_TYPE_LABEL } from "../types";
import { isDegradedSyncStatus, type RuntimeSyncStatus } from "../runtime/reconciler";
import {
  RUNTIME_CONNECTION,
  StatusDot,
  SURFACE_TONE,
  SYNC_STATUS,
  TEXT_TONE,
  syncStatusLabel,
} from "@/components/ocg/primitives";
import type { WorkspaceView } from "../layout/view-domain";

type OcgTopbarProps = {
  session: ChatSession;
  sidebarCollapsed: boolean;
  inspectorOpen: boolean;
  /** When false, the job inspector toggles are hidden (for example on the ledger view). Defaults to true. */
  inspectorControls?: boolean;
  onToggleSidebar: () => void;
  onToggleInspector: () => void;
  onOpenMobileSidebar: () => void;
  onOpenMobileInspector: () => void;
  /**
   * Opens a workspace, or closes it when it is already the active view. Omitting
   * it hides the whole shortcut group, which is how a surface renders the topbar
   * without workspace navigation.
   */
  onNavigate?: (view: WorkspaceView) => void;
  /** Current workspace, used to highlight the matching shortcut. */
  activeView?: WorkspaceView;
  runtimeStatus: RuntimeStatus;
  /** Canonical reconciler sync status. Only degraded states are surfaced. */
  syncStatus?: RuntimeSyncStatus | null;
};

/**
 * Workspace shortcuts, in bar order. The placeholder "more actions" control sat
 * between Attention and the surfaces, so the group is kept in two pieces.
 */
const SHORTCUTS_BEFORE_MENU: WorkspaceShortcut[] = [
  { view: "home", icon: Home, label: "Home" },
  { view: "attention", icon: Bell, label: "Attention" },
];

const SHORTCUTS_AFTER_MENU: WorkspaceShortcut[] = [
  { view: "ledger", icon: Table2, label: "Resource ledger" },
  { view: "control-center", icon: SlidersHorizontal, label: "Control Center" },
  { view: "job-execution", icon: Workflow, label: "Job Execution" },
  { view: "logs", icon: ScrollText, label: "Logs and diagnostics" },
  { view: "settings", icon: Settings, label: "Settings" },
];

type WorkspaceShortcut = {
  view: WorkspaceView;
  icon: typeof Home;
  label: string;
};

function Shortcut({
  shortcut,
  active,
  onNavigate,
}: {
  shortcut: WorkspaceShortcut;
  active: boolean;
  onNavigate: (view: WorkspaceView) => void;
}) {
  const Icon = shortcut.icon;
  const action = `${active ? "Close" : "Open"} ${shortcut.label}`;
  return (
    <Button
      variant={active ? "secondary" : "ghost"}
      size="icon-xs"
      onClick={() => onNavigate(shortcut.view)}
      aria-label={action}
      aria-current={active ? "page" : undefined}
      title={action}
    >
      <Icon className="size-4" />
    </Button>
  );
}

const WORK_TYPE_DOT: Record<ChatSession["workType"], string> = {
  research: "bg-sky-500",
  coding: "bg-emerald-500",
  design: "bg-violet-500",
  devops: "bg-amber-500",
};

export function OcgTopbar({
  session,
  sidebarCollapsed,
  inspectorOpen,
  inspectorControls = true,
  onToggleSidebar,
  onToggleInspector,
  onOpenMobileSidebar,
  onOpenMobileInspector,
  onNavigate,
  activeView = "chat",
  runtimeStatus,
  syncStatus,
}: OcgTopbarProps) {
  const connection = RUNTIME_CONNECTION[runtimeStatus.state];
  // Degraded sync states are the only ones the bar surfaces; the visual comes
  // from the shared scale so it matches the inspector and the ledger.
  const sync = syncStatus ? SYNC_STATUS[syncStatus] : null;
  return (
    <header className="flex h-12 shrink-0 items-center gap-1.5 border-b border-border bg-background px-2 sm:px-3">
      {/* Mobile sidebar toggle */}
      <Button
        variant="ghost"
        size="icon-xs"
        className="lg:hidden"
        onClick={onOpenMobileSidebar}
        aria-label="Open navigation"
        title="Open navigation"
      >
        <PanelLeft className="size-4" />
      </Button>
      {/* Desktop sidebar toggle */}
      <Button
        variant="ghost"
        size="icon-xs"
        className="hidden lg:inline-flex"
        onClick={onToggleSidebar}
        aria-label={sidebarCollapsed ? "Expand sidebar" : "Collapse sidebar"}
        aria-expanded={!sidebarCollapsed}
        title={sidebarCollapsed ? "Expand sidebar" : "Collapse sidebar"}
      >
        <PanelLeft className="size-4" />
      </Button>

      <div className="mx-1 h-5 w-px bg-border" aria-hidden="true" />

      <div className="flex min-w-0 flex-1 items-center gap-2">
        <span
          aria-hidden="true"
          className={cn("size-1.5 shrink-0 rounded-full", WORK_TYPE_DOT[session.workType])}
        />
        <h1 className="truncate text-[13px] font-semibold tracking-tight">
          {session.title}
        </h1>
        <span className="hidden shrink-0 rounded border border-border bg-muted/60 px-1.5 py-0.5 text-[11px] font-medium text-muted-foreground sm:inline">
          {WORK_TYPE_LABEL[session.workType]}
        </span>
      </div>

      <div
        className="hidden shrink-0 items-center gap-1.5 rounded-full border border-border bg-muted/40 px-2.5 py-1 text-[11px] text-muted-foreground md:flex"
        title={runtimeStatus.detail ?? "Local mock runtime"}
      >
        <StatusDot tone={connection.tone} pulse={connection.pulse} />
        <span className="font-medium">local mock</span>
        <span aria-hidden="true">·</span>
        <span>{runtimeStatus.state}</span>
      </div>

      {syncStatus && sync && isDegradedSyncStatus(syncStatus) && (
        <div
          className={cn(
            "hidden shrink-0 items-center gap-1.5 rounded-full border px-2 py-0.5 text-[11px] md:flex",
            SURFACE_TONE[sync.tone],
            TEXT_TONE[sync.tone],
          )}
          title={`Runtime synchronization: ${syncStatus}`}
        >
          <StatusDot tone={sync.tone} pulse={sync.pulse} />
          <span className="font-medium">{syncStatusLabel(syncStatus)}</span>
        </div>
      )}

      {onNavigate &&
        SHORTCUTS_BEFORE_MENU.map((shortcut) => (
          <Shortcut
            key={shortcut.view}
            shortcut={shortcut}
            active={activeView === shortcut.view}
            onNavigate={onNavigate}
          />
        ))}

      <Button
        variant="ghost"
        size="icon-xs"
        aria-label="More actions (placeholder)"
        title="More actions (placeholder)"
      >
        <MoreHorizontal className="size-4" />
      </Button>

      {onNavigate &&
        SHORTCUTS_AFTER_MENU.map((shortcut) => (
          <Shortcut
            key={shortcut.view}
            shortcut={shortcut}
            active={activeView === shortcut.view}
            onNavigate={onNavigate}
          />
        ))}

      {inspectorControls && (
        <>
          {/* Mobile job inspector toggle */}
          <Button
            variant="ghost"
            size="icon-xs"
            className="lg:hidden"
            onClick={onOpenMobileInspector}
            aria-label="Open job inspector"
            title="Open job inspector"
          >
            <PanelRight className="size-4" />
          </Button>
          {/* Desktop job inspector toggle */}
          <Button
            variant={!inspectorOpen ? "secondary" : "ghost"}
            size="icon-xs"
            className="hidden lg:inline-flex"
            onClick={onToggleInspector}
            aria-label={inspectorOpen ? "Collapse job inspector" : "Expand job inspector"}
            aria-expanded={inspectorOpen}
            title={inspectorOpen ? "Collapse job inspector" : "Expand job inspector"}
          >
            {inspectorOpen ? (
              <ChevronsRight className="size-4" />
            ) : (
              <ChevronsLeft className="size-4" />
            )}
          </Button>
        </>
      )}
    </header>
  );
}
