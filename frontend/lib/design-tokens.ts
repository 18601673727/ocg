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
