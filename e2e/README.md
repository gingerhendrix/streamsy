# e2e

End-to-end tests against a real Cloudflare deployment. This private package
is outside the root Bun workspace. It pins Alchemy v2, so install it on its
own:

```bash
bun install --cwd e2e
```

## Deployed conformance run

```bash
bun run test:e2e:deployed          # from the repository root: build, then the run below
bun run --cwd e2e test:deployed    # build the worker, deploy, test, destroy
```

One run does these steps in order:

1. Builds `packages/conformance-tests/dist/worker/worker.js`.
2. Picks a new stage such as `c-tm1n7l-89babe`, and deploys `alchemy.run.ts`
   to the Worker `streamsy-conf-<stage>` on `workers.dev`.
3. Waits until `GET /` answers `400 Stream path required` five times in a row.
4. Runs `test:deployed` in `packages/conformance-tests`. That is the official
   Durable Streams suite with `SERVER_BASE_URL` set to the Worker URL. The run
   passes only with 332 passed and 6 skipped.
5. Destroys the stage twice, waits for `404`, and checks that Alchemy's local
   state for the stage holds no resources.
6. Writes `reports/<stage>.json` and `reports/<stage>.vitest.json`. It does this
   even when a step fails or the run is interrupted.

`src/run.ts` owns this order and the cleanup. `src/live.ts` holds the real
adapters. The run needs Node on `PATH`, because the Alchemy v2 CLI runs under
Node. It also needs a Cloudflare login in the default Alchemy profile. If deploy
reports missing OAuth scopes, run this once:

```bash
node e2e/node_modules/alchemy/bin/cli.js profile refresh --profile default --provider Cloudflare
```

The run does not query Cloudflare for Worker size or startup CPU. Those report
fields say `unavailable`.

If a run is killed hard, remove its stage by hand:

```bash
bun run --cwd e2e destroy --stage <stage> --yes
```

## Checks without Cloudflare

```bash
bun run e2e:check    # typecheck and bun tests; CI runs this
```

The tests use fakes. `test/live.test.ts` runs the real adapters against a fake
Alchemy CLI and a fake suite script. None of the tests deploy.

`alchemy.source.run.ts` declares the same stack from the Effect-native source
in `src/source-worker.ts`. It is only typechecked.
