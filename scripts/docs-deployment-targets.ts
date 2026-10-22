/**
 * Documentation infrastructure identities; selectors are aliases, not resource renames.
 *
 * A target with a `stage` is a shared target. Its Alchemy stage is pinned, its
 * Worker name is explicit, and its state lives in the remote Cloudflare state
 * store, so a CI runner and a local shell update the same Worker. A target
 * without a `stage` keeps the default local state and the `$USER` stage.
 */
export interface DocsDeploymentTarget {
  readonly appId: string;
  readonly resourceId: string;
  readonly name: string;
  readonly domains: ReadonlyArray<string>;
  readonly stage: string | undefined;
}

export function docsDeploymentTarget(selector = "production"): DocsDeploymentTarget {
  switch (selector) {
    case "production":
      return {
        appId: "streamsy-docs",
        resourceId: "streamsy-docs",
        // The Worker that local `gareth` stage deploys created. Pinned so a
        // deploy from another stage or machine updates it in place.
        name: "streamsy-docs-streamsy-docs-gareth",
        domains: ["streamsy.gandrew.com", "streamsy.dev"],
        stage: "production",
      };
    case "next":
      return {
        appId: "streamsy-docs",
        resourceId: "streamsy-docs",
        name: "streamsy-docs-next",
        domains: ["next.streamsy.dev"],
        stage: "next",
      };
    case "preview":
    case "experimental":
      return {
        appId: "streamsy-docs-experimental",
        resourceId: "streamsy-docs-experimental",
        name: "streamsy-docs-experimental",
        domains: ["experimental.streamsy.dev"],
        stage: undefined,
      };
    default:
      throw new Error(`Unknown STREAMSY_DOCS_DEPLOYMENT value: ${selector}`);
  }
}
