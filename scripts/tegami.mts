import { Array } from "effect";
import { tegami } from "tegami";
import type { TegamiPlugin } from "tegami";
import { runCli } from "tegami/cli";
import { github } from "tegami/plugins/github";

/** Keep Streamsy's shared v<version> tag and GitHub release. */
const sharedReleaseTag = (): TegamiPlugin => ({
  enforce: "post",
  initPublishPlan({ plan }) {
    const versions = new Set<string>();

    for (const [id, packagePlan] of plan.packages) {
      const pkg = this.graph.get(id);

      if (pkg?.group?.name !== "streamsy" || pkg.version === undefined) {
        continue;
      }

      versions.add(pkg.version);
      packagePlan.git ??= {};
      packagePlan.git.tag = `v${pkg.version}`;
    }

    if (versions.size > 1) {
      throw new Error(
        `Streamsy release packages have mismatched versions: ${Array.join([...versions], ", ")}`,
      );
    }
  },
  name: "streamsy-shared-release-tag",
});

// Tegami matches each pattern against the package name and its internal id
// (`npm:<name>`). Use a list of names: a negative regex removes every package.
const paper = tegami({
  groups: {
    streamsy: {
      syncBump: true,
      syncGitTag: true,
    },
  },
  ignore: [
    "durable-streams",
    "@streamsy/conformance-tests",
    "@streamsy/docs-snippets",
    "@streamsy/issue-tracker-demo-contract",
    "@streamsy/test-support",
    "bugsy",
    "fold-agent-example",
    "hackernews-newest-stream",
    "hex-board",
    "issue-tracker-demo",
    "issue-tracker-demo-event-sourced",
    "memory-server",
    "platform-matrix",
  ],
  npm: {
    client: "bun",
    trustedPublish: {
      provider: "github",
      workflow: "publish.yml",
    },
  },
  packages: {
    "@streamsy/core": { group: "streamsy" },
    "@streamsy/projection": { group: "streamsy" },
    "@streamsy/serve": { group: "streamsy" },
    "@streamsy/storage": { group: "streamsy" },
  },
  plugins: [
    github({
      repo: "gingerhendrix/streamsy",
      versionPr: {
        base: "main",
      },
    }),
    sharedReleaseTag(),
  ],
});

await runCli(paper);
