import type { Metadata, Viewport } from "next";
import type { ReactNode } from "react";
import "./globals.css";

export const metadata: Metadata = {
  title: "OCG",
  description: "OCG local operator console.",
};

export const viewport: Viewport = {
  width: "device-width",
  initialScale: 1,
};

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en-US" className="h-full antialiased font-sans">
      <body className="flex min-h-full flex-col bg-background text-foreground">{children}</body>
    </html>
  );
}
