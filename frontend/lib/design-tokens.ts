/**
 * OCG Design Token System
 * 
 * This file defines the canonical design tokens for typography, spacing,
 * and visual hierarchy across the OCG frontend.
 * 
 * DO NOT use arbitrary values (text-[14px], p-[12px]) in components.
 * Reference these semantic tokens instead.
 */

// ============================================================================
// TYPOGRAPHY SCALE
// ============================================================================

/**
 * Semantic typography scale mapped to Tailwind utilities.
 * 
 * Usage:
 * - Use the semantic name (TYPOGRAPHY_SCALE.body) in documentation
 * - Apply the Tailwind class directly in JSX
 * - Never use arbitrary text-[Xpx] values
 */
export const TYPOGRAPHY_SCALE = {
  /** 12px - Captions, metadata, timestamps */
  caption: 'text-xs',
  
  /** 13px - Secondary body text, table cells, compact labels */
  body_sm: 'text-[13px]',
  
  /** 14px - Primary body text, descriptions, form inputs */
  body: 'text-sm',
  
  /** 15px - Emphasized body text, important labels */
  body_lg: 'text-[15px]',
  
  /** 16px - Section labels, card titles, navigation */
  label: 'text-base',
  
  /** 18px - Section headings, emphasized titles */
  heading_sm: 'text-lg',
  
  /** 20px - Major section headings */
  heading: 'text-xl',
  
  /** 24px - Page titles, hero headings */
  heading_lg: 'text-2xl',
  
  /** 30px - Display text (rare) */
  display: 'text-[30px]',
} as const;

/**
 * Line height scale for typography
 */
export const LINE_HEIGHT = {
  tight: 'leading-tight',    // 1.25
  snug: 'leading-snug',      // 1.375
  normal: 'leading-normal',  // 1.5
  relaxed: 'leading-relaxed', // 1.625
} as const;

/**
 * Font weight scale
 */
export const FONT_WEIGHT = {
  normal: 'font-normal',      // 400
  medium: 'font-medium',      // 500
  semibold: 'font-semibold',  // 600
  bold: 'font-bold',          // 700
} as const;

// ============================================================================
// SPACING SCALE
// ============================================================================

/**
 * Container padding patterns for different surface types
 */
export const CONTAINER_PADDING = {
  /** Default padding for page surfaces: p-4 sm:p-6 lg:p-8 */
  page: 'p-4 sm:p-6 lg:p-8',
  
  /** Compact padding for panels and dense UI: p-3 */
  panel: 'p-3',
  
  /** Dense padding for tables and lists: p-2 */
  dense: 'p-2',
  
  /** Spacious padding for hero sections: p-6 */
  spacious: 'p-6',
  
  /** Inline padding for buttons/pills: px-3 py-1.5 */
  inline: 'px-3 py-1.5',
  
  /** Compact inline: px-2 py-1 */
  inline_compact: 'px-2 py-1',
} as const;

/**
 * Gap scale for flex/grid layouts
 */
export const GAP_SCALE = {
  /** 4px - Tight grouping (e.g., icon + text) */
  tight: 'gap-1',
  
  /** 8px - Default spacing between related items */
  default: 'gap-2',
  
  /** 12px - Comfortable spacing for lists */
  comfortable: 'gap-3',
  
  /** 16px - Section separation within a component */
  loose: 'gap-4',
  
  /** 24px - Major section separation */
  section: 'gap-6',
} as const;

/**
 * Vertical spacing (margin/padding) scale
 */
export const VERTICAL_SPACING = {
  /** 8px - Tight vertical rhythm */
  tight: 'space-y-2',
  
  /** 12px - Default vertical rhythm */
  default: 'space-y-3',
  
  /** 16px - Comfortable vertical rhythm */
  comfortable: 'space-y-4',
  
  /** 24px - Section separation */
  section: 'space-y-6',
} as const;

// ============================================================================
// BORDER RADIUS
// ============================================================================

/**
 * Border radius semantic scale
 */
export const BORDER_RADIUS = {
  /** Interactive elements: buttons, inputs, pills */
  interactive: 'rounded-md',
  
  /** Containers: panels, cards, surfaces */
  container: 'rounded-lg',
  
  /** Small decorative elements: badges, dots */
  small: 'rounded',
  
  /** Full round: avatars, icon buttons */
  full: 'rounded-full',
} as const;

// ============================================================================
// BORDER OPACITY
// ============================================================================

/**
 * Border opacity semantic scale
 */
export const BORDER_OPACITY = {
  /** Primary structural borders */
  primary: 'border-border',
  
  /** Secondary dividers */
  secondary: 'border-border/70',
  
  /** Tertiary subtle separators */
  tertiary: 'border-border/40',
  
  /** Faint decorative borders */
  faint: 'border-border/20',
} as const;

// ============================================================================
// COMPONENT HEIGHT
// ============================================================================

/**
 * Standard heights for interactive components
 */
export const COMPONENT_HEIGHT = {
  /** Compact buttons/inputs: h-8 */
  compact: 'h-8',
  
  /** Default buttons/inputs: h-9 */
  default: 'h-9',
  
  /** Large buttons/inputs: h-10 */
  large: 'h-10',
  
  /** Extra large: h-11 */
  xlarge: 'h-11',
} as const;

// ============================================================================
// USAGE EXAMPLES
// ============================================================================

/*
BEFORE:
<div className="p-2.5 px-2.5 py-2">
  <h2 className="text-[14px] font-medium">Title</h2>
  <p className="text-[13px] text-muted-foreground">Description</p>
</div>

AFTER:
<div className={CONTAINER_PADDING.panel}>
  <h2 className={`${TYPOGRAPHY_SCALE.body} ${FONT_WEIGHT.medium}`}>Title</h2>
  <p className={`${TYPOGRAPHY_SCALE.body_sm} text-muted-foreground`}>Description</p>
</div>

BEST (with cn helper):
<div className={cn(CONTAINER_PADDING.panel)}>
  <h2 className={cn(TYPOGRAPHY_SCALE.body, FONT_WEIGHT.medium)}>Title</h2>
  <p className={cn(TYPOGRAPHY_SCALE.body_sm, "text-muted-foreground")}>Description</p>
</div>
*/
