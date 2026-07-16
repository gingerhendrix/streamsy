/**
 * Safe path helper for Streamsy's path-prefix handler. It normalizes a trailing
 * slash, preserves `/` as an id segment separator, percent-encodes each
 * segment, and rejects empty, `.`, and `..` segments. It deliberately does not
 * use `new URL(streamId, base)` resolution, which would treat ids as relative
 * references.
 */
export function protocolPathUrl(baseUrl: string | URL, streamId: string): URL {
  const segments = streamId.split("/");
  if (
    segments.length === 0 ||
    segments.some((segment) => segment.length === 0 || segment === "." || segment === "..")
  ) {
    throw new TypeError("Stream id must contain only non-empty, non-relative path segments");
  }
  const url = new URL(baseUrl);
  const prefix = url.pathname.endsWith("/") ? url.pathname : `${url.pathname}/`;
  url.pathname = `${prefix}${segments.map(encodeURIComponent).join("/")}`;
  return url;
}
