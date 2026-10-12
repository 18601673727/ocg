import type { Metadata } from "next";
import { StaticEntry } from "@/components/ocg/bootstrap/static-entry";

export const metadata: Metadata = {
  title: "OCG Control - OCG Workspace",
};

export default function CanonicalPage() {
  return <StaticEntry route="canonical" />;
}
