# Streamsy npm release runbook

Streamsy publishes four public npm packages:

- `@streamsy/core`
- `@streamsy/storage-memory`
- `@streamsy/storage-sqlite`
- `@streamsy/storage-durable-object`

`@streamsy/conformance-tests` and everything under `examples/*` remain private and are not published.

## Prerequisites

- npm CLI `11.5.1` or newer and Node.js `22.14.0` or newer for provenance/trusted publishing.
- Bun installed for workspace install/build/test and package tarball creation.
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

`bun run pack:dry-run` uses `bun pm pack --dry-run` in each public package directory. This matches the publish workflow, which creates package tarballs with Bun and publishes those tarballs with npm.

The Durable Object conformance suite requires Cloudflare/Alchemy credentials and can be run when appropriate:

```bash
bun run test:conformance:do
```

## Manual first publish for new packages

npm trusted publishing can only be configured after each new package exists on npm. Do the first publish manually from a clean checkout. Do **not** publish `@streamsy/conformance-tests` or `examples/*`.

Local/manual publishes cannot create npm provenance attestations because provenance requires a supported cloud CI/CD runner with OIDC. The first manual publish should therefore use normal npm authentication and `--access public`; provenance starts with the trusted-publishing workflow after the packages exist and trusted publishers are configured.

1. Log in to npm in an interactive shell:

   ```bash
   npm login
   npm whoami
   ```

2. Build, test, and dry-run the exact packages:

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

3. Publish each scoped package publicly by packing with Bun, then publishing the generated tarball with npm. Do not pass `--provenance` for this local/manual first publish:

   ```bash
   set -euo pipefail

   for pkg in \
     packages/core \
     packages/storage-memory \
     packages/storage-sqlite \
     packages/storage-durable-object
   do
     echo "Publishing $pkg..."
     cd "$pkg"
     TARBALL=$(bun pm pack --quiet | tail -n 1)
     npm publish "$TARBALL" --access public
     rm -f "$TARBALL"
     cd - >/dev/null
   done
   ```

4. Confirm the packages are visible:

   ```bash
   npm view @streamsy/core version repository
   npm view @streamsy/storage-memory version repository
   npm view @streamsy/storage-sqlite version repository
   npm view @streamsy/storage-durable-object version repository
   ```

## Commands for the first `0.0.1` release

Use these commands from a clean `main` worktree after reviewing the release-prep commits. They push the repository changes, manually publish the initial npm packages with Bun-created tarballs, and then optionally push the `v0.0.1` tag so GitHub has a release marker for the first version.

```bash
cd /home/gareth/Documents/Personal/repos/streamsy/main

git status --short --branch
git push origin main

npm login
npm whoami

bun install --frozen-lockfile
bun run build
bun run typecheck
bun run lint
bun run format:check
bun run test:unit
bun run test:conformance:memory
bun run test:conformance:sqlite
bun run pack:dry-run

set -euo pipefail
for pkg in \
  packages/core \
  packages/storage-memory \
  packages/storage-sqlite \
  packages/storage-durable-object
do
  echo "Publishing $pkg..."
  cd "$pkg"
  TARBALL=$(bun pm pack --quiet | tail -n 1)
  npm publish "$TARBALL" --access public
  rm -f "$TARBALL"
  cd "$OLDPWD"
done

npm view @streamsy/core version repository
npm view @streamsy/storage-memory version repository
npm view @streamsy/storage-sqlite version repository
npm view @streamsy/storage-durable-object version repository

# Optional after the manual npm publish succeeds:
# push the first version tag so GitHub has a release marker.
# The tag workflow skips packages that are already published.
git tag v0.0.1
git push origin v0.0.1
```

After the manual publish, configure trusted publishing and use later tags for automated provenance publishes. The `v0.0.1` npm packages were manually published, so they will not have provenance attestations.

## Configure npm trusted publishing

After the first manual publish, configure trusted publishing on npmjs.com for each of the four public packages.

For each package, open the package settings on npm and add a trusted publisher with:

- Publisher type: GitHub Actions
- Organization/user: `gingerhendrix`
- Repository: `streamsy`
- Workflow filename: `publish.yml`
- Environment: leave empty unless the workflow is later changed to use a GitHub Environment
- Allowed action: `npm publish`

The repository workflow is `.github/workflows/publish.yml`. It follows the Tooee release pattern: a `v*` tag triggers the workflow, Bun creates each package tarball with `bun pm pack --quiet`, and npm publishes the tarball with `--provenance --access public` using GitHub Actions OIDC/trusted publishing.

## Automated releases after trusted publishing is configured

1. Update package versions in all four public package manifests and update internal `@streamsy/core` dependency versions where needed.
2. Run the pre-release verification commands.
3. Commit and push the version changes.
4. Create and push a matching tag, for example `v0.0.2`, to trigger `.github/workflows/publish.yml`.
5. Verify package publication, provenance, and the generated GitHub Release.
