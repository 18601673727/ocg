import { createContext, useContext } from "solid-js";
export const JobDetailsContext = createContext<((job: string) => void) | null>(null);

export function useOpenJobDetails() {
  return useContext(JobDetailsContext);
}
