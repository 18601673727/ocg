"use client";

import ThinkingDots from "@/components/react-bits/thinking-dots";
import { cn } from "@/lib/utils";

type ActivityPulseProps = {
  className?: string;
  paused?: boolean;
  label?: string;
};

/**
 * Restrained React Bits usage: a small monochrome activity dot field.
 * Used for the mock worker presence and the empty-state backdrop so the
 * shell stays quiet while still demonstrating the Pro primitive.
 */
export function ActivityPulse({ className, paused = false, label }: ActivityPulseProps) {
  return (
    <div
      role="img"
      aria-label={label ?? "Background activity indicator"}
      aria-hidden={label ? undefined : true}
      className={cn("overflow-hidden rounded-full border border-border bg-muted", className)}
    >
      <ThinkingDots
        speed={0.7}
        lobes={3}
        spacing={0.16}
        dotSize={0.05}
        dotGain={0.1}
        dotSoftness={0.55}
        pulse={0.12}
        pulseRate={1.2}
        pulseGlow={0.15}
        drift={0.18}
        cloudScale={0.9}
        turbulence={0.18}
        ambient={0.28}
        intensity={0.6}
        color="#8a8478"
        accentColor="#3f3b35"
        backgroundColor="transparent"
        opacity={0.9}
        glow={0.02}
        grain={0.02}
        vignette={0}
        cursorInteraction={false}
        paused={paused}
        adaptiveQuality
        targetFps={30}
        dpr={1}
        className="h-full w-full"
      />
    </div>
  );
}
