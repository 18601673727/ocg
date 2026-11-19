import { createContext, useContext } from "solid-js";
import { type ElementProps, handleEvent, splitProps } from "@/lib/ui-state";
export const WorkspaceNavigation = createContext<((href: string) => void) | null>(null);

type WorkspaceLinkProps = ElementProps<"a"> & { onNavigate?: () => void };

export function WorkspaceLink(viewProps: WorkspaceLinkProps) {
  const [, props] = splitProps(viewProps, ["onClick", "onNavigate"]);

  const navigate = useContext(WorkspaceNavigation);
  return (
    <a
      {...props}
      onClick={(event) => {
        handleEvent(viewProps.onClick, event);
        const link = event.currentTarget;
        if (
          event.defaultPrevented ||
          event.button !== 0 ||
          event.altKey ||
          event.ctrlKey ||
          event.metaKey ||
          event.shiftKey ||
          (link.target && link.target !== "_self") ||
          link.hasAttribute("download") ||
          link.origin !== window.location.origin
        ) {
          return;
        }
        if (viewProps.onNavigate) {
          event.preventDefault();
          viewProps.onNavigate();
        } else if (navigate) {
          event.preventDefault();
          navigate(link.href);
        }
      }}
    />
  );
}
