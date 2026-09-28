import type { Metadata } from "next";
import type { ReactNode } from "react";
import { Geist, Geist_Mono, Noto_Sans, Playfair_Display } from "next/font/google";
import "./globals.css";
import { cn } from "@/lib/utils";
import { ThemeProvider } from "@/components/ocg/appearance/theme-provider";

const playfairDisplayHeading = Playfair_Display({subsets:['latin'],variable:'--font-heading'});

const notoSans = Noto_Sans({subsets:['latin'],variable:'--font-sans'});

const geistSans = Geist({
  variable: "--font-geist-sans",
  subsets: ["latin"],
});

const geistMono = Geist_Mono({
  variable: "--font-geist-mono",
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: "OCG Workspace",
  description: "OCG desktop AI engineering environment — application shell (mock state, Phase 1).",
};

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html
      lang="en"
      suppressHydrationWarning
      className={cn("h-full", "antialiased", geistSans.variable, geistMono.variable, "font-sans", notoSans.variable, playfairDisplayHeading.variable)}
    >
      <head>
        <script
          dangerouslySetInnerHTML={{
            __html: `(function(){try{var key="ocg.appearance.v1";var raw=window.localStorage.getItem(key);var mode="system";if(raw){var value=JSON.parse(raw);if(value&&("system"===value.theme||"light"===value.theme||"dark"===value.theme)){mode=value.theme}}var dark="dark"===mode||("system"===mode&&window.matchMedia("(prefers-color-scheme: dark)").matches);var root=document.documentElement;root.classList.toggle("dark",dark);root.dataset.theme=dark?"dark":"light";if(raw){var parsed=JSON.parse(raw);root.dataset.density=parsed.density||"comfortable";root.dataset.accent=parsed.accent||"ochre"}}catch(error){document.documentElement.dataset.theme="light"}})();`,
          }}
        />
      </head>
      <body className="flex min-h-full flex-col"><ThemeProvider>{children}</ThemeProvider></body>
    </html>
  );
}
