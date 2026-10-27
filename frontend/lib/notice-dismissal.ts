const STORAGE_KEY = "ocg.dismissed-notices.v1";
const dismissed = new Set<string>();

export async function noticeIdentity(scope: string, message: string): Promise<string> {
  const value = JSON.stringify([scope, message]);
  try {
    const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(value));
    return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join(
      "",
    );
  } catch {
    return `volatile:${value}`;
  }
}

export function isNoticeDismissed(identity: string): boolean {
  try {
    const stored: unknown = JSON.parse(localStorage.getItem(STORAGE_KEY) ?? "[]");
    if (Array.isArray(stored)) {
      for (const entry of stored) {
        if (typeof entry === "string" && /^[a-f0-9]{64}$/.test(entry)) dismissed.add(entry);
      }
    }
  } catch {
    // Browser storage can be unavailable; dismissals still last for this page.
  }
  return dismissed.has(identity);
}

export function dismissNotice(identity: string): void {
  isNoticeDismissed(identity);
  dismissed.add(identity);
  try {
    localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify([...dismissed].filter((entry) => /^[a-f0-9]{64}$/.test(entry))),
    );
  } catch {
    // Keep the in-memory dismissal when browser storage is unavailable.
  }
}
