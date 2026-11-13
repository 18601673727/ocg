import { useId } from "react";

import { cn } from "@/lib/utils";

export function WorkspaceIllustration({ className }: { className?: string }) {
  const id = useId();

  return (
    <svg
      viewBox="0 0 520 330"
      fill="none"
      aria-hidden="true"
      focusable="false"
      className={cn("workspace-illustration", className)}
      strokeLinejoin="round"
      strokeLinecap="round"
    >
      <defs>
        <pattern id={id + "grid"} width="32" height="16" patternUnits="userSpaceOnUse">
          <path d="m0 0 32 16m0-16L0 16" stroke="var(--drawing-line)" strokeWidth=".6" />
        </pattern>
        <radialGradient id={id + "fade"}>
          <stop offset=".4" stopColor="white" />
          <stop offset="1" stopColor="white" stopOpacity="0" />
        </radialGradient>
        <mask id={id + "mask"}>
          <ellipse cx="260" cy="226" rx="265" ry="104" fill={`url(#${id}fade)`} />
        </mask>
        <g id={id + "plate"} stroke="var(--drawing-ink)" strokeWidth="1.6">
          <path d="m260 43 113 56-113 57-113-57Z" fill="var(--drawing-surface)" />
          <path d="m147 99 113 57v10l-113-56Z" fill="var(--drawing-side)" />
          <path d="m260 156 113-57v11l-113 56Z" fill="var(--drawing-edge)" />
        </g>
        <g id={id + "terminal"} transform="matrix(1 .5 -1 .5 260 68)">
          <rect width="65" height="65" rx="8" fill="var(--drawing-ink)" />
          <path d="m15 22 13 10-13 10m21 1h15" stroke="var(--drawing-highlight)" strokeWidth="5" />
        </g>
      </defs>

      <path d="M0 122h520v208H0z" fill={`url(#${id}grid)`} mask={`url(#${id}mask)`} />
      <g stroke="var(--drawing-line)" strokeWidth="1.3">
        <path d="m52 202 43 21 35-17 75 37m-133-52 42 21 18-9 84 41m144-46 37 18 57-28m-167 77 48 24 64-32" />
        <g fill="var(--drawing-surface)">
          <ellipse cx="50" cy="201" rx="4.5" ry="2.4" />
          <ellipse cx="69" cy="189.5" rx="4.5" ry="2.4" />
          <ellipse cx="459" cy="186" rx="4.5" ry="2.4" />
          <ellipse cx="404" cy="254" rx="4.5" ry="2.4" />
        </g>
      </g>

      <path d="m260 159 126 63-126 63-126-63Z" fill="var(--drawing-shadow)" />
      <use href={`#${id}plate`} y="100" />
      <path d="m260 153 91 46-91 46-91-46Z" stroke="var(--drawing-line)" strokeDasharray="3 4" />
      <g stroke="var(--drawing-line)" strokeWidth="1.2" strokeDasharray="4 4">
        <path d="M157 102v90m206-90v90m-103-38v89m0-201v97" />
      </g>

      <use href={`#${id}plate`} y="52" />
      <g transform="matrix(1 .5 -1 .5 260 102)" stroke="var(--drawing-line)" strokeWidth="1.7">
        <rect x="25" y="25" width="48" height="48" rx="3" fill="var(--drawing-side)" />
        <path d="M32 25V10m9 15V8m10 17V13m11 12V9M25 36H9m16 9H7m18 10H12m13 10H7m67-29h14m-14 9h19m-19 10h15m-15 10h12M33 74v12m10-12v19m10-19v12m10-12v17" />
      </g>
      <use href={`#${id}plate`} />
      <path d="m260 49 100 50-100 50-100-50Z" stroke="var(--drawing-line)" />
      <use href={`#${id}terminal`} />
      <g fill="var(--drawing-ink)">
        <ellipse cx="260" cy="56" rx="2.7" ry="1.4" />
        <ellipse cx="173" cy="99" rx="2.7" ry="1.4" />
        <ellipse cx="347" cy="99" rx="2.7" ry="1.4" />
        <ellipse cx="260" cy="142" rx="2.7" ry="1.4" />
      </g>
      <g stroke="var(--drawing-ink)" strokeWidth="2.5">
        <path d="m162 112 23 11m-23 41 23 11m-23 37 23 11" />
        <path d="m337 126 4-2m4-2 4-2m4-2 4-2" />
      </g>
    </svg>
  );
}
