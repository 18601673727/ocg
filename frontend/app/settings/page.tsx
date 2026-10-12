import type { Metadata } from "next";
import { StaticEntry } from "@/components/ocg/bootstrap/static-entry";

export const metadata: Metadata = {
  title: "Settings - OCG Workspace",
};

export default function SettingsPage() {
  return <StaticEntry route="settings" />;
}
