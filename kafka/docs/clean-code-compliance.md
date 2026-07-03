# Clean Code Compliance — Audit & Checklist

Date: 2025-11-25

This document maps the project's `Clean Code TypeScript` guidance to actual findings in the repository and provides a prioritized checklist for remediation. It was generated from an automated scan and manual review of the codebase.

Summary

- Scan targets: `src/**`, `examples/**`, tests. Looked for `any` usage, `console.*` in library code, eslint disables, promise chains, `TODO/FIXME`, and idioms like `||` defaults.
- Overall: codebase is well-structured, strongly typed, and mostly compliant. A few targeted improvements will reduce risk and align more closely with the guide.

Findings (concrete)

- `any` usages and casts (recommended: replace with `unknown`/generics or tighten types)

  - `src/client/consumers/consume-with-dlq.ts` — `handler: (payload: any, ctx: ...)` (eslint disabled for explicit any)
  - `src/client/core/kafka.ts` — `public get topics(): Readonly<Record<string, any>> | undefined`
  - `src/types/kafka.ts` — logger meta typed as `meta?: any`, `createPartitioner?: () => (args: any) => number`
  - `src/config/topics/update-configs.ts` — `(c as any).configName`, `(c as any).configValue`
  - `src/client/producers/event.factory.ts` — `(payload as any)` casts and `CreatePublisherOptions<TSchema = any>`
  - `src/client/core/error-classifier.ts` and `connect-with-retry.ts` — `err: any` in catch handlers
  - `src/schemas/dlq.schema.ts` — `originalValue: z.any()` (intentional to preserve arbitrary payloads)

- `console.*` usage

  - `examples/express-app/src/client.ts` — demo logger uses `console.log/warn/error` (acceptable in examples; not in library core)
  - Some `console.log` snippets in doc-comments (no runtime impact)

- `eslint-disable` and suppressions

  - `src/client/consumers/consume-with-dlq.ts` — single `// eslint-disable-next-line @typescript-eslint/no-explicit-any` prior to handler typing

- Promise chaining / `.catch`

  - Top-level example usage: `initKafka().catch(...)`, `startServer().catch(...)` — acceptable for scripts/examples
  - `void this.connect().catch(...)` in `src/client/core/kafka.ts` used intentionally to avoid unhandled rejections but could be logged explicitly

- `TODO` / `FIXME` markers
  - None found

Assessment vs Guide

- Async/await: predominantly used; aligns with guidance.
- Naming & functions: generally good and consistent.
- Side effects & immutability: mostly OK (default metadata uses spreads; acceptable patterns used).
- Error handling: acceptable; some catches use `any` which could be improved.
- Testing: e2e tests and examples present — good practice.

Prioritized Remediation Checklist (recommended order)

1. Replace `any` with `unknown` / generics / typed alternatives (High)

- [ ] `src/client/consumers/consume-with-dlq.ts` — change handler typing to `ConsumeHandler<T = unknown>` or `handler: (payload: unknown, ctx: ConsumeHandlerContext) => Promise<void>` and infer/validate via Zod when schema present.
- [ ] `src/types/kafka.ts` — change `meta?: any` to `meta?: Record<string, unknown> | unknown` and tighten `createPartitioner` signature.
- [ ] `src/client/core/kafka.ts` — change `Record<string, any>` to `Record<string, unknown>` or a typed `TopicMap` interface.
- [ ] `src/config/topics/update-configs.ts` — remove `as any` casts by narrowing config entry types or adding a small type guard.
- [ ] `src/client/producers/event.factory.ts` — replace `TSchema = any` with `TSchema = z.ZodTypeAny | unknown` as appropriate and reduce `(payload as any)` casts.
- [ ] `src/client/core/error-classifier.ts` / `connect-with-retry.ts` — avoid `err: any`; use `unknown` and narrow (e.g., `if (err && typeof err === 'object' && 'message' in err)`).

2. Document and minimize `eslint-disable` usage (Medium)

- [ ] Add inline comment explaining why the `no-explicit-any` suppression exists (if keeping). Prefer switching to `unknown`/generic handler instead.

3. Example app: replace demo `console.*` with `Logger` interface or add comment (Low)

- [ ] `examples/express-app/src/client.ts` — either use the library `Logger` type or mark clearly as demo-only.

4. CI / Lint changes (Low–Medium)

- [ ] Add `pnpm lint` to CI or tighten ESLint rule `no-explicit-any` and allow exceptions only for `examples/**` and test fixtures.

5. Optional: top-level rejection handling (Low)

- [ ] Consider consistently logging `void this.connect().catch(err => logger.error(...))` instead of silently swallowing errors.

Quick PR candidates (I can prepare)

- PR 1 (high impact, small): Replace `any` -> `unknown`/generics in `consume-with-dlq.ts` and update types in `types/kafka.ts` for logger meta. (Safer, minimal API surface changes)
- PR 2 (demo): Replace `console.*` in `examples/express-app` with reference logger or comment.
- PR 3 (CI): Add `pnpm lint` to CI and an ESLint rule tweak for `examples/**`.

Automation & Tracking

- Suggested branch naming: `canary/clean-code/any-to-unknown` or `canary/clean-code/ci-lint`.
- Suggested checklist file (this doc) should be kept updated as PRs are merged.

How I can help next

- Create PR 1 now (replace `any` in `consume-with-dlq.ts` and tighten `types/kafka.ts`) and run `pnpm build` / `pnpm test` to verify. (Recommended)
- Or open smaller PRs for each change above so each is reviewable and revertible.

Appendix — quick references

- Files with `any` casts found (for convenience):
  - `src/client/consumers/consume-with-dlq.ts`
  - `src/client/core/kafka.ts`
  - `src/types/kafka.ts`
  - `src/config/topics/update-configs.ts`
  - `src/client/producers/event.factory.ts`
  - `src/client/core/error-classifier.ts`
  - `src/client/core/connect-with-retry.ts`
  - `src/schemas/dlq.schema.ts` (intentional `z.any()`)
- Example console usage:
  - `examples/express-app/src/client.ts`

---

If you want, I will now implement PR 1 (safe, focused): update `consume-with-dlq.ts` to use `unknown`/generic handler type and adjust `types/kafka.ts` logger meta typing. Tell me to proceed and I'll create the patch and run the relevant local checks. If you'd rather a different first step, mention which one.
