"use client";

import { createContext, useContext } from "react";

export const JobDetailsContext = createContext<((job: string) => void) | null>(null);

export function useOpenJobDetails() {
  return useContext(JobDetailsContext);
}
