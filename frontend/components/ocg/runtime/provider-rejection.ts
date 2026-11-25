import type { TranslateFn } from "../i18n/dictionaries";

// A confirmed provider rejection: the provider returned a complete HTTP refusal.
// Only the facts the backend recorded are presented. Billing is never inferred.
export type ProviderRejection = {
  status: number | null;
  providerCode: string | null;
  requestId: string | null;
};

export function providerRejectionOf(output: {
  code?: string | null;
  value?: unknown;
}): ProviderRejection | null {
  if (output.code !== "provider_rejected") return null;
  const value =
    output.value && typeof output.value === "object" && !Array.isArray(output.value)
      ? (output.value as Record<string, unknown>)
      : {};
  return {
    status: typeof value.status_code === "number" ? value.status_code : null,
    providerCode: typeof value.provider_code === "string" ? value.provider_code : null,
    requestId: typeof value.request_id === "string" ? value.request_id : null,
  };
}

export function providerRejectionText(t: TranslateFn, rejection: ProviderRejection): string {
  const lines = [
    rejection.status === null
      ? t("provider.rejected")
      : t("provider.rejectedStatus", { status: rejection.status }),
  ];
  if (rejection.providerCode !== null)
    lines.push(t("provider.code", { code: rejection.providerCode }));
  if (rejection.requestId !== null)
    lines.push(t("provider.requestId", { id: rejection.requestId }));
  return lines.join("\n");
}
