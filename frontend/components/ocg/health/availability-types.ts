export type AvailabilityStatus = "available" | "unavailable" | "unknown" | "in-progress";

export type AvailabilityEntry = {
  provider: string;
  model: string;
  effort: string | null;
  status: AvailabilityStatus;
  updatedAt?: number | null;
  jobId?: string;
  jobState?: string;
  failure?: { code: string; message?: string | null } | null;
  stale?: boolean;
};

export type AvailabilityMatrix = {
  groups: readonly { provider: string; entries: readonly AvailabilityEntry[] }[];
  summary: { total: number; available: number; unavailable: number; unknown: number; inProgress: number };
};
