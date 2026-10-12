import type { Metadata } from "next";
import { StaticEntry } from "@/components/ocg/bootstrap/static-entry";

export const metadata: Metadata = {
  title: "Resource Ledger - OCG Workspace",
};

export default function ResourceLedgerPage() {
  return <StaticEntry route="resource-ledger" />;
}
