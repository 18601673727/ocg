import type { Metadata, Viewport } from "next";
import type { ReactNode } from "react";
import "./globals.css";
import "katex/dist/katex.min.css";
import { ThemeProvider } from "@/components/ocg/appearance/theme-provider";
import { THEME_BOOTSTRAP_SCRIPT } from "@/components/ocg/appearance/theme-bootstrap";
import { I18nProvider, LOCALE_BOOTSTRAP_SCRIPT } from "@/components/ocg/i18n";

export const metadata: Metadata = {
  title: "OCG Workspace",
  description: "A self-hosted workspace for AI conversations, coding agents, and job history.",
};

export const viewport: Viewport = {
  width: "device-width",
  initialScale: 1,
  viewportFit: "cover",
  interactiveWidget: "resizes-content",
};

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en-US" suppressHydrationWarning className="h-full font-sans antialiased">
      <head>
        {/* Applies the stored theme before first paint; see theme-bootstrap. */}
        <script dangerouslySetInnerHTML={{ __html: THEME_BOOTSTRAP_SCRIPT }} />
        {/* Applies the stored locale before first paint; see locale-bootstrap. */}
        <script dangerouslySetInnerHTML={{ __html: LOCALE_BOOTSTRAP_SCRIPT }} />
      </head>
      <body className="flex min-h-full flex-col">
        <ThemeProvider>
          <I18nProvider>{children}</I18nProvider>
        </ThemeProvider>
      </body>
    </html>
  );
}
