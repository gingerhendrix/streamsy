/** Prefix helpers shared by protocol and host routers. */
export const streamPath = (pathPrefix = "/") => {
  const prefix = pathPrefix.endsWith("/") ? pathPrefix : `${pathPrefix}/`;
  const regex = new RegExp(`^${prefix.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}`);
  const strip = (pathname: string): string => pathname.replace(regex, "");
  return {
    requiredPathPattern: (): string => `${prefix}{path}`,
    strip,
    // A client that does not know the mount prefix names the source by its
    // path below the prefix. Both forms resolve to the same stream id.
    canonicalizeForkSource: (source: string): string =>
      regex.test(source) ? strip(source) : source.replace(/^\//, ""),
  };
};
