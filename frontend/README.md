# OCG console

The operator console for OCG, a Next.js static export.

## Runtime

Every route renders the same operator console. It talks to the Control
Plane at the page's own origin. Run it the way the Control Plane serves it:
`pnpm build`, then start OCG with `OCG_UI_DIR` set to `frontend/out` (see the
repository README). Served this way, the console signs in without a code;
protect a public deployment in front of OCG (for example with Cloudflare Zero
Trust). There is no `?scenario=` parameter.

## Checks

```bash
pnpm install --frozen-lockfile
pnpm exec tsc --noEmit
pnpm lint
pnpm build
```
