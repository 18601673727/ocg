import { cn } from "@/lib/utils";

export function OcgLogo({ compact = false, className }: { compact?: boolean; className?: string }) {
  return (
    <span
      role="img"
      aria-label="OCG"
      data-ocg-logo=""
      className={cn(
        "inline-block origin-left scale-y-75 font-sans leading-none font-black tracking-[-0.06em]",
        compact ? "text-[18px]" : "text-[26px]",
        className,
      )}
    >
      OCG
    </span>
  );
}
