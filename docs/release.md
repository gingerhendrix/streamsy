# Streamsy npm release runbook

Streamsy publishes four public npm packages:

- `@streamsy/core`
- `@streamsy/storage-memory`
- `@streamsy/storage-sqlite`
- `@streamsy/storage-durable-object`

`@streamsy/conformance-tests` and everything under `examples/*` remain private and are not published.

## Prerequisites

- npm CLI `11.5.1` or newer and Node.js `22.14.0` or newer for provenance/trusted publishing.
- Bun installed for workspace install/build/test.
- npm account access for the `@streamsy` scope.
- GitHub repository: `gingerhendrix/streamsy`.
- Clean checkout of the commit being released.

Check the local toolchain:

```bash
node --version
npm --version
bun --version
npm whoami
```

## Pre-release verification

From the repository root:

```bash
bun install --frozen-lockfile
bun run build
bun run typecheck
bun run lint
bun run format:check
bun run test:unit
bun run test:conformance:memory
bun run test:conformance:sqlite
bun run pack:dry-run
```

The Durable Object conformance suite requires Cloudflare/Alchemy credentials and can be run when appropriate:

```bash
bun run test:conformance:do
```

## Manual first publish for new packages

npm trusted publishing can only be configured after each new package exists on npm. Do the first publish manually from a clean checkout. Do **not** publish `@streamsy/conformance-tests` or `examples/*`.

1. Log in to npm in an interactive shell:

   ```bash
   npm login
   npm whoami
   ```

2. Build and verify the exact packages:

   ```bash
   bun install --frozen-lockfile
   bun run typecheck
   bun run lint
   bun run format:check
   bun run build
   bun run pack:dry-run
   ```

3. Publish each scoped package publicly with provenance:

   ```bash
   cd packages/core
   npm publish --provenance --access public

   cd ../storage-memory
   npm publish --provenance --access public

   cd ../storage-sqlite
   npm publish --provenance --access public

   cd ../storage-durable-object
   npm publish --provenance --access public
   ```

4. Confirm the packages are visible:

   ```bash
   npm view @streamsy/core version repository
   npm view @streamsy/storage-memory version repository
   npm view @streamsy/storage-sqlite version repository
   npm view @streamsy/storage-durable-object version repository
   ```

## Configure npm trusted publishing

After the first manual publish, configure trusted publishing on npmjs.com for each of the four public packages.

For each package, open the package settings on npm and add a trusted publisher with:

- Publisher type: GitHub Actions
- Organization/user: `gingerhendrix`
- Repository: `streamsy`
- Workflow filename: `publish.yml`
- Environment: leave empty unless the workflow is later changed to use a GitHub Environment
- Allowed action: `npm publish`

The repository workflow is `.github/workflows/publish.yml`. It uses GitHub-hosted runners, grants `id-token: write`, sets up npm with the npm registry, and runs `npm publish --provenance --access public` without an `NPM_TOKEN`.

## Automated releases after trusted publishing is configured

1. Update package versions in all four public package manifests and update internal `@streamsy/core` dependency versions where needed.
2. Run the pre-release verification commands.
3. Commit and push the version changes.
4. Create and publish a GitHub Release for the tag/commit to trigger `.github/workflows/publish.yml`.
5. Verify package publication and provenance on npm.

The workflow can also be started manually from GitHub Actions via `workflow_dispatch` if a release needs to be retried from the selected ref.
