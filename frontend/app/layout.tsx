import type { Metadata, Viewport } from "next";
import type { ReactNode } from "react";
import { Geist_Mono, Noto_Sans, Noto_Sans_SC, Playfair_Display } from "next/font/google";
import "./globals.css";
import { cn } from "@/lib/utils";
import { ThemeProvider } from "@/components/ocg/appearance/theme-provider";
import { THEME_BOOTSTRAP_SCRIPT } from "@/components/ocg/appearance/theme-bootstrap";
import { I18nProvider, LOCALE_BOOTSTRAP_SCRIPT } from "@/components/ocg/i18n";

const playfairDisplayHeading = Playfair_Display({subsets:['latin'],variable:'--font-heading'});

const notoSans = Noto_Sans({subsets:['latin'],variable:'--font-sans'});

const notoSansSc = Noto_Sans_SC({subsets:['latin'],variable:'--font-sans-sc', weight: ['400','500','700']});

const geistMono = Geist_Mono({
  variable: "--font-geist-mono",
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: "OCG Workspace",
  description: "OCG desktop AI engineering environment — application shell (mock state, Phase 1).",
};

export const viewport: Viewport = {
  width: "device-width",
  initialScale: 1,
  viewportFit: "cover",
  interactiveWidget: "resizes-content",
};

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html
      lang="en-US"
      suppressHydrationWarning
      className={cn("h-full", "antialiased", geistMono.variable, "font-sans", notoSans.variable, notoSansSc.variable, playfairDisplayHeading.variable)}
    >
      <head>
        {/* Applies the stored theme before first paint; see theme-bootstrap. */}
        <script dangerouslySetInnerHTML={{ __html: THEME_BOOTSTRAP_SCRIPT }} />
        {/* Applies the stored locale before first paint; see locale-bootstrap. */}
        <script dangerouslySetInnerHTML={{ __html: LOCALE_BOOTSTRAP_SCRIPT }} />
      </head>
      <body className="flex min-h-full flex-col"><ThemeProvider><I18nProvider>{children}</I18nProvider></ThemeProvider></body>
    </html>
  );
}
