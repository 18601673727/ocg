import { createContext, useContext } from "solid-js";
import { type View } from "@/lib/ui-state";

const RemoteLogout = createContext<(() => Promise<void>) | null>(null);
export function SessionActions(props: { onSignOut: () => Promise<void>; children: View }) {
  return <RemoteLogout value={props.onSignOut}>{props.children}</RemoteLogout>;
}
export function useRemoteLogout() {
  return useContext(RemoteLogout);
}
export function useAccountEmail(): string | null {
  return null;
}
