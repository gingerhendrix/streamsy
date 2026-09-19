/** Prefix helpers shared by protocol and host routers. */
export const streamPath = (pathPrefix = "/") => {
  const prefix = pathPrefix.endsWith("/") ? pathPrefix : `${pathPrefix}/`;
  const regex = new RegExp(`^${prefix.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}`);
  const strip = (pathname: string): string => pathname.replace(regex, "");
  return {
    requiredPathPattern: (): string => `${prefix}{path}`,
    strip,
    canonicalizeForkSource: strip,
  };
};
