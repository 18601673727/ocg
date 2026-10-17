import type { Metadata, Viewport } from "next";
import type { ReactNode } from "react";
import "./globals.css";
import { ThemeProvider } from "@/components/ocg/appearance/theme-provider";
import { THEME_BOOTSTRAP_SCRIPT } from "@/components/ocg/appearance/theme-bootstrap";

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
    <html lang="en-US" suppressHydrationWarning className="h-full antialiased font-sans">
      <head>
        {/* Applies the stored theme before first paint; see theme-bootstrap. */}
        <script dangerouslySetInnerHTML={{ __html: THEME_BOOTSTRAP_SCRIPT }} />
      </head>
      <body className="flex min-h-full flex-col bg-background text-foreground">
        <ThemeProvider>{children}</ThemeProvider>
      </body>
    </html>
  );
}
