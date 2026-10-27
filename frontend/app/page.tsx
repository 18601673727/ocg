import type { Metadata } from "next";
import { StaticEntry } from "@/components/ocg/bootstrap/static-entry";

export const metadata: Metadata = {
  title: "Home - OCG Workspace",
};

export default function Home() {
  return <StaticEntry route="home" />;
}
