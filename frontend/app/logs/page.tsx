import type { Metadata } from "next";
import { StaticEntry } from "@/components/ocg/bootstrap/static-entry";

export const metadata: Metadata = {
  title: "Logs - OCG Workspace",
};

export default function LogsPage() {
  return <StaticEntry route="logs" />;
}
