This is a [Next.js](https://nextjs.org) project bootstrapped with [`create-next-app`](https://nextjs.org/docs/app/api-reference/cli/create-next-app).

## Getting Started

First, run the development server:

```bash
npm run dev
# or
yarn dev
# or
pnpm dev
# or
bun dev
```

Open [http://localhost:3000](http://localhost:3000) with your browser to see the result.

## Runtime scenarios

The UI currently runs against a local OCG-owned mock runtime. Inspect a fixture with
`?scenario=`; unknown values fall back to `normal-chat`:

```text
normal-chat · long-stream · tool-heavy · worker-parallel
build-failed · retry-success · mission-complete · budget-exhausted
runtime-disconnected · runtime-connecting · runtime-failed · permission-required
observability-live · resource-ledger
```

Entry/setup fixtures:

```text
local-ready · local-first-run
remote-unauthenticated · remote-session-expired · remote-denied
remote-authenticated-ready · remote-authenticated-first-run
onboarding-resume · onboarding-migration · onboarding-recovery
onboarding-invalid-configuration · onboarding-auth-required
onboarding-connection-failure · onboarding-discovery · onboarding-ready
```

The bootstrap scenarios drive the entry gate: local scenarios open the workspace
directly, remote scenarios without access show the login surface, and pending
setup opens the seven-stage onboarding wizard. `?scenario=` also works on
`/login` and `/onboarding`; explicit local scenarios still resolve to the
workspace. Cloudflare Access is represented as a mock handoff button, and no
credential is entered or stored.

For example: `http://localhost:3000/?scenario=worker-parallel`.

## Resource ledger

The `resource-ledger` scenario adds a cross-mission usage and cost ledger on top
of the workspace. Open it directly at `/resource-ledger`, which defaults to the
ledger scenario, or via the ledger action in the topbar. A scenario can still be
selected explicitly with `?scenario=`, for example
`http://localhost:3000/resource-ledger?scenario=resource-ledger`. Other
scenarios have no ledger fixture and the surface shows an explicit empty state
instead of inventing data.

You can start editing the page by modifying `app/page.tsx`. The page auto-updates as you edit the file.

This project uses [`next/font`](https://nextjs.org/docs/app/building-your-application/optimizing/fonts) to automatically optimize and load [Geist](https://vercel.com/font), a new font family for Vercel.

## Learn More

To learn more about Next.js, take a look at the following resources:

- [Next.js Documentation](https://nextjs.org/docs) - learn about Next.js features and API.
- [Learn Next.js](https://nextjs.org/learn) - an interactive Next.js tutorial.

You can check out [the Next.js GitHub repository](https://github.com/vercel/next.js) - your feedback and contributions are welcome!

## Deploy on Vercel

The easiest way to deploy your Next.js app is to use the [Vercel Platform](https://vercel.com/new?utm_medium=default-template&filter=next.js&utm_source=create-next-app&utm_campaign=create-next-app-readme) from the creators of Next.js.

Check out our [Next.js deployment documentation](https://nextjs.org/docs/app/building-your-application/deploying) for more details.
