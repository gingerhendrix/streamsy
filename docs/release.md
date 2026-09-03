# Streamsy npm release runbook

Streamsy uses a single release-train version. The private root `package.json` is not published, but its `version` is the source of truth for the release version and must be bumped in the release commit before tagging.

Current public npm packages:

- `@streamsy/core` (`packages/core`)
- `@streamsy/http-client` (`packages/http-client`)
- `@streamsy/streams` (`packages/streams`)
- `@streamsy/projection` (`packages/projection`)
- `@streamsy/state` (`packages/state`)
- `@streamsy/storage` (`packages/storage`) — subpaths `/fs`, `/sqlite`, `/durable-object`

`@streamsy/conformance-tests` and everything under `examples/*` remain private and are not published.

`@streamsy/storage-memory` was removed from the repo after `0.0.2`; memory storage now lives in `@streamsy/core`. Do not publish a new `@streamsy/storage-memory` version. Deprecate the old npm package only when the replacement release and migration wording require it.

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

## Release version policy

1. Bump the private root `package.json` version.
2. Bump every public package manifest to the same version.
3. Bump internal runtime dependency pins between public `@streamsy/*` packages to the same version.
4. Commit the version changes.
5. Create the tag from the committed root package version:

   ```bash
   VERSION=$(node -p "require('./package.json').version")
   git tag "v${VERSION}"
   ```

The publish workflow reads `VERSION` from the root `package.json`, verifies the pushed tag is exactly `v${VERSION}`, and verifies every public package manifest matches `VERSION` before publishing.

## Pre-release verification

From the repository root:

```bash
bun install --frozen-lockfile
bun run build
bun run typecheck
bun run lint
bun run format:check
bun run test:unit
bun run test:conformance:client
bun run test:conformance:memory
bun run test:conformance:sqlite
bun run test:conformance:fs
bun run pack:dry-run
```

`bun run pack:dry-run` uses `bun pm pack --dry-run` in each public package directory. This matches the publish workflow, which creates package tarballs with Bun and publishes those tarballs with npm.

The Durable Object conformance suite requires Cloudflare/Alchemy credentials. Run it before release when the credentials/environment are available; otherwise record that it was skipped:

```bash
bun run test:conformance:do
```

## Manual first publish for new packages

npm trusted publishing is configured per package after that package exists on npm. For a package name that is new to npm, do the first publish manually from the exact release commit. Do **not** publish `@streamsy/conformance-tests` or `examples/*`.

Local/manual publishes cannot create npm provenance attestations because provenance requires a supported cloud CI/CD runner with OIDC. The first manual publish should therefore use normal npm authentication and `--access public`; provenance starts with the trusted-publishing workflow after the package exists and trusted publishing is configured.

### First publish after the HTTP client rename

`@streamsy/client` was first published with Streamsy `0.2.0`. The package was renamed to `@streamsy/http-client` after `0.2.1` because Streamsy's transport-neutral client contracts and direct client already live in `@streamsy/core`. The new name identifies this package as the HTTP adapter over `@durable-streams/client`.

First-publish `@streamsy/http-client` manually from the reviewed release commit. Then configure npm trusted publishing for the new package name before pushing the release tag. Deprecate `@streamsy/client` only after the replacement version is available.

The tag workflow skips package versions that are already present on npm. It should skip the manually published HTTP client and publish the other package versions that are absent.

```bash
cd /home/gareth/Documents/Personal/repos/streamsy/main

# Confirm you are on the reviewed release commit and all versions match.
git status --short --branch
VERSION=$(node -p "require('./package.json').version")
echo "Releasing ${VERSION}"
node - <<'NODE'
const paths = [
  'package.json',
  'packages/core/package.json',
  'packages/http-client/package.json',
  'packages/streams/package.json',
  'packages/projection/package.json',
  'packages/state/package.json',
  'packages/storage/package.json',
];
const root = require('./package.json').version;
for (const path of paths) {
  const pkg = require(`./${path}`);
  if (pkg.version !== root) throw new Error(`${path} is ${pkg.version}, expected ${root}`);
}
NODE

npm login
npm whoami

bun install --frozen-lockfile
bun run build
bun run typecheck
bun run lint
bun run format:check
bun run test:unit
bun run test:conformance:client
bun run test:conformance:memory
bun run test:conformance:sqlite
bun run test:conformance:fs
bun run pack:dry-run

set -euo pipefail
(
  cd packages/http-client
  TARBALL=$(bun pm pack --quiet | tail -n 1)
  npm publish "$TARBALL" --access public
  rm -f "$TARBALL"
)

npm view "@streamsy/http-client@${VERSION}" version repository
```

After the first publish succeeds, configure trusted publishing for `@streamsy/http-client` using the settings below. Then push `main` and the release tag.

## Configure npm trusted publishing

After the first manual publish for any new package, configure trusted publishing on npmjs.com for that package.

For each public package, open the package settings on npm and add a trusted publisher with:

- Publisher type: GitHub Actions
- Organization/user: `gingerhendrix`
- Repository: `streamsy`
- Workflow filename: `publish.yml`
- Environment: leave empty unless the workflow is later changed to use a GitHub Environment
- Allowed action: `npm publish`

The repository workflow is `.github/workflows/publish.yml`. A `v*` tag triggers the workflow, Bun creates each package tarball with `bun pm pack --quiet`, and npm publishes the tarball with `--provenance --access public` using GitHub Actions OIDC/trusted publishing.

## Automated release after trusted publishing is configured

Use this after new packages have been first-published and trusted publishing has been configured. For the HTTP client rename, this means after the release version of `@streamsy/http-client` exists on npm and has trusted publishing configured.

```bash
cd /home/gareth/Documents/Personal/repos/streamsy/main

git status --short --branch
VERSION=$(node -p "require('./package.json').version")
echo "Releasing ${VERSION}"

bun install --frozen-lockfile
bun run build
bun run typecheck
bun run lint
bun run format:check
bun run test:unit
bun run test:conformance:client
bun run test:conformance:memory
bun run test:conformance:sqlite
bun run test:conformance:fs
bun run pack:dry-run

git push origin main
git tag "v${VERSION}"
git push origin "v${VERSION}"
```

Watch the GitHub Actions publish workflow. It should verify the committed root/package versions, skip packages that already have the exact version on npm, publish missing packages with provenance when trusted publishing permits it, and create the GitHub Release.

Verify publication:

```bash
VERSION=$(node -p "require('./package.json').version")
for name in \
  @streamsy/core \
  @streamsy/http-client \
  @streamsy/streams \
  @streamsy/projection \
  @streamsy/state \
  @streamsy/storage
do
  npm view "${name}@${VERSION}" version repository dist.integrity
  npm view "${name}@${VERSION}" dist.attestations --json
done

gh release view "v${VERSION}" --repo gingerhendrix/streamsy
```

## Deprecate `@streamsy/client`

After the first `@streamsy/http-client` release is available and its migration wording is confirmed, deprecate the old package instead of unpublishing it:

```bash
npm deprecate '@streamsy/client@*' 'This package moved to @streamsy/http-client. Install and import @streamsy/http-client instead.'
```

Verify the deprecation message:

```bash
npm view @streamsy/client deprecated
```

## Deprecate `@streamsy/experimental`

`@streamsy/experimental@0.2.1` is still live on npm. The package was dissolved after `0.2.1` into `@streamsy/streams` (Effect stream capabilities, bindings, stream identity, causal coverage) and `@streamsy/projection` (the `StateProjection` facade and the framework-private `/mesh` incubation area).

Both replacement names are new to npm, so they follow the `@streamsy/http-client` pattern:

1. First-publish `@streamsy/streams` and `@streamsy/projection` manually from the reviewed release commit.
2. Configure npm trusted publishing for both new package names.
3. Only then deprecate the old name.

```bash
npm deprecate '@streamsy/experimental@*' 'This package was split into @streamsy/streams and @streamsy/projection. Install and import those instead.'
```

Verify the deprecation message:

```bash
npm view @streamsy/experimental deprecated
```

## Deprecate `@streamsy/storage-memory`

After the replacement release is available and the public API/migration wording is confirmed, deprecate the old package instead of unpublishing it:

```bash
npm deprecate '@streamsy/storage-memory@*' 'Memory storage moved into @streamsy/core as of Streamsy 0.1.0. Install/use @streamsy/core instead.'
```

Verify the deprecation message:

```bash
npm view @streamsy/storage-memory deprecated
```
