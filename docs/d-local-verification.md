# D-local verification ledger

This is the repository-local record for the D correction. It is a local
verification ledger, not hosted evidence and not authorization to deploy. The
correction code was verified from `7497d7173b7995979b448058aca835de397fd951`,
the tip after the F1/F2 ownership corrections and their lint repairs. The
documentation-only follow-up keeps that code and artifact identity unchanged.
The coordinator's stream result records the final documentation tip and private
scratch receipt; this ledger keeps the reproducible facts available in the
repository.

## Boundary and preparation

The accepted C base is
`e09213c1f88e1f68f3e1c3f8a556baa442fb430b`. No dependency or lockfile change
was made in this correction. A fresh environment may prepare registries with
scripts disabled, after inspecting lifecycle hooks, then use frozen installs:

```sh
bun install --lockfile-only --ignore-scripts
bun install --cwd hosted --lockfile-only --ignore-scripts
bun install --frozen-lockfile --ignore-scripts
bun install --cwd hosted --frozen-lockfile --ignore-scripts
bun install --cwd site --frozen-lockfile --ignore-scripts
bun run prepare
```

Those are optional registry-preparation commands, not validation. No registry
preparation was needed for this correction. No Alchemy module was evaluated,
and no Cloudflare account, remote target, deployment, destroy, metadata query,
or hosted measurement was used.

For the local gate, create an existing absolute scratch directory outside
owned test roots and set:

```sh
export STREAMSY_STORAGE_SCRATCH=/home/gareth/Documents/Personal/scratch/2026-09-08-step-3-batch-d-fixes/storage
export CLOUDFLARE_CF_FETCH_ENABLED=false
```

The site process chain inherits the Cloudflare-fetch setting. The excerpt
checker gives every executed source its own temporary root and process group;
the usage example gives Miniflare its own `tmpdir()` root and retains that root
if disposal cannot be confirmed.

## Gate commands and observed results

The following commands were run with the environment above, using installed
executables and no network-capable fallback. The first unit invocation exposed
one transient B14 alarm failure; its unchanged package test rerun passed. The
recorded gate run is the passing rerun.

```sh
bun run build
bun run typecheck
bun run hosted:check
bun run test:unit
bun run test:conformance
bun run test:sql-boundary
bun run measure:bundle
bun run lint
bun run lint:policy
bun run format:check
bun run check:perimeter
bun run pack:dry-run
bun run site:validate
git status --porcelain=v1 --untracked-files=all
git diff --check e09213c1f88e1f68f3e1c3f8a556baa442fb430b..HEAD
```

Observed local counts:

| Check                                   | Result                                                                                                                                                                        |
| --------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `hosted:check`                          | 37 passed, 0 failed; 167 expectations                                                                                                                                         |
| Unit: core / storage / views            | 305 passed, 5 skipped / 53 passed, 2 skipped / 44 passed, 0 skipped                                                                                                           |
| Unit: serve / Fold / Hacker News        | 143 passed, 3 skipped / 47 passed, 0 skipped / 12 passed, 0 skipped                                                                                                           |
| Official memory, SQLite, workerd        | 332 passed, 6 skipped independently on each backend                                                                                                                           |
| Workerd ownership                       | 8 passed, 0 failed                                                                                                                                                            |
| SQL boundary                            | 2 passed, 0 failed                                                                                                                                                            |
| Lint                                    | pass; 139 warning lines, unchanged inventory                                                                                                                                  |
| Lint policy / format / perimeter / pack | pass / 361 files / pass / four public package dry-runs                                                                                                                        |
| Site validation                         | 0 stale-term hits across 40 authored files; 7 excerpts (5 executed, 2 typechecked-only); 11 rendered routes; 161 internal links/images/anchors; checker process group stopped |

The usage excerpt is a local smoke check and the official workerd result is the
single-object-chain profile. The default `byStream()` profile's nine
chain-lifecycle divergences remain a topology distinction. Cross-object copy
tests remain separate. None of these local results is hosted evidence.

## Artifact and status

`measure:bundle` produced one regular `worker.js` with no Alchemy token:

| Metric                              | Value                                                              |
| ----------------------------------- | ------------------------------------------------------------------ |
| Worker SHA-256                      | `2b32a617c8129a4f805754c398e67da963935d5c9cbb58d3f7849ef760c5e898` |
| Raw / minified bytes                | 534,562 / 252,866                                                  |
| CLI pathname / CLI stdin / Bun gzip | 81,496 / 81,486 / 81,887                                           |
| Input / output modules              | 116 / 1                                                            |
| Bun / gzip                          | 1.4.2 / `gzip 1.14-modified`                                       |
| Compatibility                       | 2026-07-30, `nodejs_compat`; conformance long poll 1,500 ms        |

These figures are attributed to the correction code SHA above and sit beside,
without replacing, the accepted Batch B 81,574 B gzip signal and unchanged
27,160 B proposal. The 542.85 ms first-object p95 proposal remains unmeasured.

Alchemy deployment for Step 3 is authorized. Hosted execution remains disabled
in this local package pending independently reviewed live adapters and a
reconciled run plan, including destroy/cleanup and required query scope. Hosted
acceptance still requires hosted evidence, the uploaded-compressed-byte/startup-
CPU policy, and Gareth's budget/topology decision. No live adapter or evidence
path was enabled by this correction.
