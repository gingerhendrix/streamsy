# Lint policy

The repository runs two lint policies. They are separate because Effect is an
architectural choice made per package, not a repository-wide style.

## General policy

`.oxlintrc.json` holds the general TypeScript, Unicorn, Oxc, Oxlint core, and
vendored anti-slop policy with type-aware rules enabled. It applies to every
live source and test file in the repository, including the Effect-owned areas.
`parked/**` is excluded from both general lint and formatting.

```bash
bun run lint:general
```

## Effect policy

`.oxlintrc.effect.json` extends the `@effect/tsgo` recommended preset and
enables the `effecttsgo` plugin plus the Effect-specific vendored anti-slop
plugin. It runs against an explicit path allow-list, held in the `lint:effect`
script:

- `packages/core-next`, `packages/serve`
- `packages/views`
- `examples/fold-agent`
- `examples/hackernews-newest-stream`

```bash
bun run lint:effect
```

The old core and its dependent packages remain under the general policy during
the Step 1 rebuild. The old `streams` and `projection` packages still depend on
Effect, but are outside the live Effect allow-list pending their removal in
Batch 6. The general probe stays in `packages/core/src`; the Effect probe now
lives in `packages/serve/src`.

Add a path to the allow-list only when that area intentionally adopts Effect,
which means its manifest declares `effect` and its sources import it. Do not
instead add per-rule suppressions to non-Effect files.

Oxlint always enables its built-in `eslint` rules and they cannot be disabled at
plugin level, so `.oxlintrc.effect.json` disables the ones it would otherwise
duplicate. The general policy owns those rules for the whole repository.

## Aggregate and verification

```bash
bun run lint          # general policy, then Effect policy
bun run lint:fix      # the same split, with fixes applied
bun run lint:policy   # asserts the boundary between the two policies
```

Type-aware rules resolve workspace imports through each package's build output,
so run `bun run build` before measuring an inventory. Linting a tree with no
`dist` directories reports extra findings that disappear once the packages are
built.

`bun run lint:policy` writes a temporary probe file into a non-Effect package
and into an Effect-owned package, lints both policies, asserts that each policy
reports exactly the violations it owns, and removes the probes. No lint fixtures
are committed.

## Perimeter verification

`bun run check:perimeter` rejects references to the parked example paths in live
code and root configuration, retired package names, and Vitest imports outside
conformance tests, the explicitly reported old-package exceptions, and the two
existing Hacker News tests (until Batch 5). Parked
files and maintainer docs are reference material; site content is deferred to
Batch 7. The temporary runner exceptions preserve the old core until Batch 6.

Batch 2 applies Effect lint to the private storage tier in `packages/core-next`. Its source perimeter rejects Promise bridges, async functions, abort plumbing, timers, and object cloning; test files alone are excluded. Existing runner exceptions do not include this package.
