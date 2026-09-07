# Lint policy

`bun run lint` runs the general policy and then the Effect policy. General lint
(`.oxlintrc.json`) covers all live source and tests; parked examples are excluded
from lint and format. Effect lint (`.oxlintrc.effect.json`) covers core, views,
serve, Fold agent and Hacker News. Its preset owns Effect diagnostics; the general
policy owns the remaining rules, avoiding duplicate built-in diagnostics.

Build first so type-aware lint resolves workspace exports. `bun run lint:policy`
writes and removes temporary probes in conformance-tests (general) and serve
(Effect), asserting both policies' scope. `lint:fix` uses the same split.
Suppressions require a focused reason; perimeter output inventories them.

`bun run check:perimeter` rejects retired imports/APIs, temporary aliases,
unsupported runtime and Promise bridges, abort plumbing in library production
code, forbidden storage seams and SQL in core. Runtime conversion belongs to the
Bun host and tests, including the two existing published Bun contract kits.
These test entry points must import `bun:test`; no live library runtime exception
is added. Web-boundary tests retain abort controls to prove cancellation.

All authored tests use Bun. Only the official memory and SQLite suite registration
files in conformance-tests may import Vitest. Historical parked files and maintainer docs may discuss retired
names. The temporary alias scan has no exclusions. Site content is included
in retired-name scans; no new live source exclusion is allowed.

## Accepted executable-example warning baseline

The three executable documentation edges intentionally retain these warnings:

| Source                               | Diagnostic                         | Reason                                                                            |
| ------------------------------------ | ---------------------------------- | --------------------------------------------------------------------------------- |
| `packages/core/test/readme.ts`       | `effecttsgo/strict-effect-provide` | The top-level executable provides its one memory Layer at its owned runtime edge. |
| `packages/serve/test/basic-usage.ts` | `effecttsgo/strict-effect-provide` | The top-level executable provides its one memory Layer at its owned runtime edge. |
| `packages/serve/test/host.ts`        | `effecttsgo/global-fetch`          | A native HTTP request verifies the Bun host at an executable Web boundary.        |

Expected baseline: three warnings, zero errors; no suppressions are added for
these examples. `site:validate` compiles and executes all three, each with a
15-second process limit. Additional warnings require review rather than silently
expanding this baseline.
