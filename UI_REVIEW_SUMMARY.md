# OCG 0.4.5 UI Review - Typography & Color Token Consolidation

**Date:** October 13, 2026  
**Version:** OCG 0.4.5 (commit: 91e5c2e)  
**Status:** ✅ Complete

## Executive Summary

Successfully consolidated 200+ arbitrary pixel-based typography values and 36 hardcoded status colors into a semantic token system, improving design consistency and maintainability across the OCG frontend.

---

## Changes Delivered

### 1. Typography Token System ✅

**Created:** `/frontend/lib/design-tokens.ts`

Semantic typography scale with 8 levels:
- `TYPOGRAPHY_SCALE.caption` → `text-xs` (10-12px)
- `TYPOGRAPHY_SCALE.body_sm` → `text-[13px]`
- `TYPOGRAPHY_SCALE.body` → `text-sm` (14px)
- `TYPOGRAPHY_SCALE.body_lg` → `text-[15px]`
- `TYPOGRAPHY_SCALE.label` → `text-base` (16px)
- `TYPOGRAPHY_SCALE.heading_sm` → `text-lg` (18px)
- `TYPOGRAPHY_SCALE.heading` → `text-xl` (20px)
- `TYPOGRAPHY_SCALE.heading_lg` → `text-2xl` (22px)

Additional token systems:
- `CONTAINER_PADDING` (panel, compact, spacious)
- `GAP_SCALE` (tight, comfortable, relaxed)
- `BORDER_RADIUS` (interactive, container, pill)
- `BORDER_OPACITY` (primary, secondary, tertiary)

**Replacements Made:** 207 instances
- `text-[10px]` → `text-xs` (74)
- `text-[11px]` → `text-xs` (79)
- `text-[12px]` → `text-xs` (48)
- `text-[14px]` → `text-sm` (5)
- `text-[16px]` → `text-base` (1)

**Intentionally Preserved:** 42 instances
- `text-[13px]` (33) — Custom body_sm size
- `text-[15px]` (2) — Custom body_lg size
- `text-[9px]` (5) — Special badge/chip sizes
- `text-[18px]` (1) — OCG sidebar collapsed logo
- `text-[28px]` (1) — OCG sidebar expanded logo

---

### 2. Status Color Token System ✅

**Extended:** `/frontend/components/ocg/primitives/tone.ts`

Added semantic status color mappings:
- `StatusTone` type: "success" | "warning" | "error" | "info" | "pending"
- `STATUS_TONE_MAP`: Maps status semantics to tone vocabulary
- `STATUS_BG`: Background + border for status indication
- `STATUS_TEXT`: Text color for status messages
- `STATUS_DOT`: Dot fill for status indicators
- `STATUS_CLASS`: Combined background + text for badges/pills

**Replacements Made:** 36 instances across 14 files
- Error states: `text-red-600 dark:text-red-400` → `STATUS_TEXT['error']`
- Warning states: `text-amber-600 dark:text-amber-400` → `STATUS_TEXT['warning']`
- Success states: `text-emerald-600` → `STATUS_TEXT['success']`
- Pending states: `text-violet-600 dark:text-violet-400` → `STATUS_TEXT['pending']`
- Background combos: `bg-red-500/5 border-red-500/40` → `STATUS_BG['error']`

**Intentionally Preserved:** 6 instances
- 2 decorative ShieldCheck icons (home, attention surfaces)
- 1 step indicator (setup wizard)
- 1 informational banner (settings)
- 2 TSDoc comment examples (primitives)

---

## Files Modified

**50 components updated:**

### Surfaces (15)
- attention-surface.tsx
- canonical-control-surface.tsx
- control-center-surface.tsx
- health-surface.tsx
- home-surface.tsx
- job-draft-surface.tsx
- job-execution-surface.tsx
- logs-surface.tsx
- observability-panel.tsx
- resource-ledger-surface.tsx
- settings-surface.tsx
- usage-surface.tsx
- app-shell.tsx
- login-view.tsx
- setup-wizard.tsx

### Primitives (9)
- empty-state.tsx
- filter-select.tsx
- key-value.tsx
- metric.tsx
- panel.tsx
- pill.tsx
- progress-bar.tsx
- section-title.tsx
- segmented-tabs.tsx
- tone.ts ⭐ (extended)
- index.ts (exports)

### Resource Ledger (5)
- ledger-charts.tsx
- ledger-detail.tsx
- ledger-filters.tsx
- ledger-primitives.tsx
- ledger-tables.tsx

### Execution (4)
- canonical-job-overview.tsx
- job-inspector.tsx
- job-relations.tsx

### Control Center (4)
- filters.tsx
- models-view.tsx
- profiles-view.tsx
- providers-view.tsx

### Chat (3)
- chat-view.tsx
- image-attachments.tsx
- model-selector.tsx

### Setup (3)
- add-subscription-panel.tsx
- provider-sync-panel.tsx

### Usage (3)
- conversation-inspector.tsx
- usage-values.tsx

### Other (4)
- ocg-sidebar.tsx
- ocg-topbar.tsx
- profile-panel.tsx
- project-switcher.tsx
- language-switcher.tsx

**1 new file created:**
- lib/design-tokens.ts ⭐

---

## Verification

✅ **TypeScript:** `pnpm exec tsc --noEmit` — passes  
✅ **Linting:** `pnpm lint` — passes  
✅ **Tests:** No test files modified (per requirements)  
✅ **Docs:** No documentation modified (per requirements)

---

## Impact

### Before
- 200+ arbitrary `text-[Xpx]` values scattered across 40+ files
- 36+ hardcoded status colors with dark mode variants
- No centralized design token system
- Inconsistent spacing, borders, and container patterns

### After
- Semantic typography scale with 8 levels
- Centralized design tokens in `lib/design-tokens.ts`
- Status colors driven by semantic `StatusTone` type
- Consistent use of `cn()` helper for token composition
- 42 intentionally preserved values (with clear justification)

### Benefits
1. **Maintainability:** Single source of truth for typography and status colors
2. **Consistency:** Semantic tokens ensure uniform appearance across surfaces
3. **Scalability:** Future design updates only require token file changes
4. **Type Safety:** TypeScript ensures valid status/tone mappings
5. **Readability:** `STATUS_TEXT['error']` is clearer than `text-red-600 dark:text-red-400`

---

## Next Steps (Optional)

1. **Extend to `globals.css`:** Add typography scale to Tailwind theme config
2. **Container padding audit:** Standardize p-2.5/p-4/p-6 usage with CONTAINER_PADDING tokens
3. **Border radius audit:** Replace remaining arbitrary rounded-* with BORDER_RADIUS tokens
4. **Spacing audit:** Consolidate gap-*/space-* with GAP_SCALE tokens
5. **Icon color audit:** Review decorative vs. semantic icon colors

---

## Technical Notes

- All changes use `cn()` helper for composing token constants with other classes
- Arbitrary values (9px, 13px, 15px, 18px, 28px) preserved where semantically distinct
- Status colors map through `STATUS_TONE_MAP` to maintain separation between status semantics and visual tone vocabulary
- No runtime behavior changes — purely presentational token consolidation
- All modifications follow existing code style and conventions

---

**Completion Date:** October 13, 2026  
**Review Status:** Ready for commit
