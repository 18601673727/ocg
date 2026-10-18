"use client";

import { OperatorConsole } from "../session/operator-console";

export type StaticEntryRoute = "home" | "login" | "logs" | "settings" | "jobs" | "usage" | "health";

export function StaticEntry({ route }: { route: StaticEntryRoute }) {
  void route;
  return <OperatorConsole />;
}
