# Lint policy

The repository runs two lint policies. They are separate because Effect is an
architectural choice made per package, not a repository-wide style.

## General policy

`.oxlintrc.json` holds the general TypeScript, Unicorn, Oxc, and Oxlint core
policy with type-aware rules enabled. It applies to every source and test file
in the repository, including the Effect-owned areas.

```bash
bun run lint:general
```

## Effect policy

`.oxlintrc.effect.json` extends the `@effect/tsgo` recommended preset and
enables only the `effecttsgo` plugin. It runs against an explicit path
allow-list, held in the `lint:effect` script:

- `packages/experimental`
- `examples/causal-counter`
- `examples/hackernews-newest-stream`
- `examples/issue-tracker-projections`
- `examples/risk-demo`

```bash
bun run lint:effect
```

Every other package exposes a dependency-light API and declares no dependency on
`effect`. Applying Effect rules there would report findings that can only be
resolved by changing those packages' architecture and dependency boundaries.

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
