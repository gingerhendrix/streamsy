# Lint policy

`bun run lint` runs the general policy and then the Effect policy. General lint
(`.oxlintrc.json`) covers all live source and tests; parked examples are excluded
from lint and format. Effect lint (`.oxlintrc.effect.json`) covers core, views,
serve, hosted, Fold agent and Hacker News. Its preset owns Effect diagnostics; the general
policy owns the remaining rules, avoiding duplicate built-in diagnostics.

Build first so type-aware lint resolves workspace exports. `bun run lint:policy`
writes and removes temporary probes in conformance-tests (general), serve and
hosted (Effect), asserting both policies' scope. `lint:fix` uses the same split.
Suppressions require a focused reason; perimeter output inventories them.

`bun run check:perimeter` rejects retired imports/APIs, temporary aliases,
unsupported runtime and Promise bridges, abort plumbing in library production
code, forbidden storage seams and SQL in core. Runtime conversion belongs to the
Bun host and tests, including the two existing published Bun contract kits.
These test entry points must import `bun:test`; no live library runtime exception
is added. Web-boundary tests retain abort controls to prove cancellation.

All authored tests use Bun. The official memory, SQLite and workerd suite
registration files in conformance-tests may import Vitest. Hosted uses Effect
directly and does not author an integration adapter. The all-input Effect-Vitest
token scan retains root/site/source/docs coverage and excludes only generated
`hosted/bun.lock`, because Alchemy's transitive graph records that peer. Direct
and aliased manifest declarations remain forbidden outside the official
conformance manifest, which owns the plain `vitest` runner declarations. Only
the generated `hosted/bun.lock` is exempt from the Effect-Vitest token scan;
authored hosted suppressions are inventoried with focused reasons while its
dependency/build directories remain outside authored-input scope. Historical
parked files and maintainer docs may discuss retired names. The temporary alias
scan has no exclusions. Site content is
included in retired-name scans; no new live source exclusion is allowed.

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
