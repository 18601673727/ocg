import { cn } from "@/lib/utils";

export function OcgLogo({ compact = false, className }: { compact?: boolean; className?: string }) {
  return (
    <span
      role="img"
      aria-label="OCG"
      data-ocg-logo=""
      className={cn(
        "inline-block align-middle leading-none font-bold tracking-tight",
        compact ? "text-lg" : "text-2xl",
        className,
      )}
    >
      OCG
    </span>
  );
}
