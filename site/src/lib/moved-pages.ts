/**
 * Docs pages that moved: the old slug path under `/docs`, and the slug path of the page
 * that replaced it. The docs route answers an old path with a 308 redirect, and
 * `check:links` checks each entry.
 */
export const movedPages = new Map([
  ["demos", "examples"],
  ["demos/memory-server", "examples/memory-server"],
  ["demos/todo", "examples/todo"],
  ["demos/hackernews-newest-stream", "examples/hackernews-newest-stream"],
  ["demos/issue-tracker", "examples/issue-tracker"],
  ["demos/issue-tracker/event-sourced", "examples/issue-tracker/event-sourced"],
  ["demos/issue-tracker/foldkit", "examples/issue-tracker/foldkit"],
  ["demos/fold-agent", "examples/fold-agent"],
  ["demos/hex-board", "examples/hex-board"],
  ["demos/bugsy", "examples/bugsy"],
  ["demos/folder-sync", "examples/folder-sync"],
  ["demos/issue-tracker-demo", "examples/issue-tracker"],
  ["demos/issue-tracker-event-sourced", "examples/issue-tracker/event-sourced"],
  ["demos/issue-tracker-projection", "examples/bugsy"],
  ["user/durable-state", "streams/declare"],
  ["http", "servers"],
  ["http/fetch", "servers/remote"],
  ["http/routes", "servers/protocol"],
  ["http/serve", "servers/serve"],
  ["http/remote", "servers/remote"],
  ["hosting", "storage"],
  ["hosting/bun", "servers/process"],
  ["hosting/cloudflare", "servers/cloudflare"],
  ["hosting/host-contract", "servers/process"],
  ["hosting/storage-adapter", "storage/adapter"],
  ["tutorial", "examples"],
]);

/**
 * Sections that moved to another page or another heading when their page moved. A browser
 * keeps the fragment of a link across the 308 redirect of a moved page, so an old link
 * such as `/docs/hosting/bun#sqlite-storage` arrives at `/docs/servers/process#sqlite-storage`.
 * The key is that arrival, as a slug path and a fragment. The value is the section that
 * holds the content now. The docs page replaces the URL with the value on load, because
 * a server never sees a fragment. `check:links` checks that no key is an id of its page,
 * and that each value exists.
 */
export const movedSections = new Map([
  // http/serve
  ["servers/serve#long-running-hosts", "servers/process#clean-shutdown"],
  // http/remote
  ["servers/remote#limits", "servers/remote#projections"],
  // hosting
  ["storage#commit-with-your-own-tables", "storage/sqlite#commit-with-your-own-tables"],
  ["storage#protocol-tier", "storage/adapter#protocol-tier"],
  // hosting/bun and hosting/host-contract
  ["servers/process#sqlite-storage", "storage/sqlite#bun"],
  ["servers/process#several-processes", "storage/sqlite#several-processes"],
  ["servers/process#projection-services", "storage/sqlite#projection-services"],
  ["servers/process#shared-parts", "servers/process#listener"],
  ["servers/process#sqlite-on-node-and-deno", "storage/sqlite#sqlite-on-node-and-deno"],
  // hosting/cloudflare
  ["servers/cloudflare#wakeups", "servers/cloudflare/alarms#deadlines"],
  ["servers/cloudflare#projections", "servers/cloudflare/alarms#projections"],
  ["servers/cloudflare#streams-on-other-objects", "servers/cloudflare/objects#owner-and-caller"],
  ["servers/cloudflare#projection-services", "storage/sqlite#projection-services"],
]);

/** The section that replaced `fragment` on the page at `slug`, as a slug path and a fragment. */
export const movedSection = (
  slug: string,
  fragment: string,
): { readonly slug: string; readonly hash: string } | undefined => {
  const target = movedSections.get(`${slug}#${fragment}`);

  if (target === undefined) return undefined;
  const [page = "", hash = ""] = target.split("#");

  return { slug: page, hash };
};

/**
 * The decoded fragment of a URL hash such as `location.hash`, without its `#`. A hash
 * with malformed percent-encoding, such as `#100%`, has no fragment, so this returns
 * `undefined` where `decodeURIComponent` throws a `URIError`.
 */
export const hashFragment = (hash: string): string | undefined => {
  const encoded = hash.startsWith("#") ? hash.slice(1) : hash;

  try {
    return decodeURIComponent(encoded);
  } catch (error) {
    if (error instanceof URIError) return undefined;
    throw error;
  }
};

/**
 * The section that replaced the fragment of `hash` on the page at `slug`. `hash` is the
 * raw `location.hash`. A malformed hash names no section.
 */
export const forwardedSection = (
  slug: string,
  hash: string,
): { readonly slug: string; readonly hash: string } | undefined => {
  const fragment = hashFragment(hash);

  return fragment === undefined ? undefined : movedSection(slug, fragment);
};
