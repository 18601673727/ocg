import type { Metadata } from "next";
import { StaticEntry } from "@/components/ocg/bootstrap/static-entry";
import { SETTINGS_SECTIONS } from "@/components/ocg/settings/settings-sections";

export const metadata: Metadata = { title: "Settings - OCG Workspace" };
export const dynamicParams = false;

export function generateStaticParams() {
  return SETTINGS_SECTIONS.map((section) => ({ section: section.id }));
}

export default function SettingsSectionPage() {
  return <StaticEntry route="settings" />;
}
