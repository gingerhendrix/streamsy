/** Existing documentation infrastructure identities; selectors are aliases, not resource renames. */
export function docsDeploymentTarget(selector = "production") {
  switch (selector) {
    case "production":
      return {
        appId: "streamsy-docs",
        resourceId: "streamsy-docs",
        name: undefined,
        domains: ["streamsy.gandrew.com", "streamsy.dev"],
      };
    case "preview":
    case "experimental":
      return {
        appId: "streamsy-docs-experimental",
        resourceId: "streamsy-docs-experimental",
        name: "streamsy-docs-experimental",
        domains: ["experimental.streamsy.dev"],
      };
    default:
      throw new Error(`Unknown STREAMSY_DOCS_DEPLOYMENT value: ${selector}`);
  }
}
