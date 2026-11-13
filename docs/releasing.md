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

Use `patch`, `minor` or `major` in the frontmatter. The five packages share a version, so a note for one package bumps all five. Target `"group:streamsy"` when the note is about every package.

### Write one note for each user-facing change

Tegami adds each note as its own section under the next release in the changelog of every package. Write a note for each change that a user of the released packages will see, not for each commit.

- A branch that lands as one merge gets one note for each feature it adds. When a later commit on the branch changes the feature, edit that note.
- Do not add a note for a fix to behaviour that has not been released yet. Users never saw the bug. Update the feature's note if the fix changes what it says.
- Write for a developer who uses Streamsy. Say what they can do now, which API they call, and what they must change in their code. Mark a breaking change with `**Breaking change:**`. Leave out internal names and implementation steps unless a user must act on them, and link the docs page.

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

Start after the push to `main` that adds the package has updated the pull request. Run these commands from the repository root, `repos/streamsy/`. If a `version-packages` worktree exists from an earlier release, remove it first with `git worktree remove version-packages`. Otherwise `git worktree add` fails, because that worktree has the branch checked out.

```bash
npm login
git fetch origin tegami/version-packages
git worktree add -B tegami/version-packages version-packages origin/tegami/version-packages
cd version-packages
bun install
bun run tegami npm pretrust --dry-run
bun run tegami npm pretrust
git checkout -- .tegami/publish-lock.yaml
git status --short
```

The dry run lists the packages that are not on npm. If it lists a package that you did not add, stop and check it.

For each new package, `pretrust` publishes `0.0.0-tegami-trusted-publish-setup` under the `temp` dist-tag and runs `npm trust github <package> --repo gingerhendrix/streamsy --file publish.yml`. Check on npmjs.com that the trusted publisher names `gingerhendrix/streamsy` and `publish.yml`.

`pretrust` also adds an `npm:mark-latest` entry for each new package to `.tegami/publish-lock.yaml`. Do not commit that entry. `git checkout` discards it, and `git status --short` then prints nothing. With the entry, Tegami 1.5.0 runs `npm dist-tag add <package>@<version> latest` after the publish. npm rejects that command with E401 on the Actions runner, because trusted publishing authorizes `npm publish` only. The release job then fails before it creates the GitHub release and deploys the docs. This happened in 0.4.0 and 0.8.0. The entry is not needed. The normal `npm publish` already sets `latest` on the new package.

Pretrust leaves no change to commit, so merge the pull request next. Remove the worktree after the release with `git worktree remove version-packages`.

If a release job fails with `Failed to mark <package> as latest`, the packages are already on npm. Rerun the failed job with `gh run rerun <run-id> --failed`. Tegami skips versions that are on npm, so the rerun skips the `dist-tag` step and finishes the GitHub release and the docs deploys.
