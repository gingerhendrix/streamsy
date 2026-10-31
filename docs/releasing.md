# Releasing Streamsy

Streamsy uses [Tegami](https://tegami.fuma-nama.dev) to version and publish its five public packages: `@streamsy/core`, `@streamsy/storage`, `@streamsy/projection`, `@streamsy/outbox` and `@streamsy/serve`. They share one version and one `v<version>` tag. The config is in `scripts/tegami.mts`.

## Release notes

Every user-facing change needs a release note under `.tegami/`. Create one interactively with:

```bash
bun run tegami
```

Or add a file such as `.tegami/fix-read-cursor.md`:

```md
---
packages:
  "@streamsy/core": patch
---

## Keep the read cursor after a reconnect

The remote client now resumes from the last offset it received.
```

Use `patch`, `minor` or `major` in the frontmatter. The four packages share a version, so a note for one package bumps all four. Target `"group:streamsy"` when the note is about every package.

Do not edit package versions or create release tags by hand.

## How a release happens

1. A change with a release note reaches `main`.
2. `.github/workflows/publish.yml` runs the full gate, then `bun run tegami ci`. Tegami opens or updates the **Version Packages** pull request on the `tegami/version-packages` branch.
3. When you merge that pull request, the same workflow publishes the packages through npm trusted publishing. It then creates the `v<version>` tag and the GitHub release.

Tegami keeps `.tegami/publish-lock.yaml` after a release. If a publish fails, run the workflow again and Tegami retries the packages that are not on npm.

Keep the workflow file name `publish.yml`. npm trusted publishing is tied to it. The repo setting that lets GitHub Actions create pull requests must be on, or Tegami fails with HTTP 403.

The private workspaces (examples, tools and `@streamsy/conformance-tests`) get a patch bump in each Version Packages pull request because they depend on `@streamsy/core`. They never publish, so the bump does no harm.

## New packages

npm trusted publishing works only for a package that already exists on npm. Before you merge a Version Packages pull request that contains a new public package, create the package and its trusted publisher once from your own machine. `npm trust` needs npm 11.10.0 or newer, and your npm account needs two-factor authentication.

Start after the push to `main` that adds the package has updated the pull request:

```bash
npm login
git fetch origin tegami/version-packages
git worktree add -B tegami/version-packages version-packages origin/tegami/version-packages
cd version-packages
bun install
bun run tegami npm pretrust --dry-run
bun run tegami npm pretrust
```

For each new package, `pretrust` publishes `0.0.0-tegami-trusted-publish-setup` under the `temp` dist-tag and runs `npm trust github <package> --repo gingerhendrix/streamsy --file publish.yml`. It also writes `npm:mark-latest` entries into `.tegami/publish-lock.yaml`. Commit that change to the Version Packages branch, push it, then merge the pull request.

For the 0.4.0 release, `@streamsy/storage`, `@streamsy/projection` and `@streamsy/serve` are new on npm. `@streamsy/core` exists already. Check on npmjs.com that its trusted publisher names `gingerhendrix/streamsy` and `publish.yml`.
