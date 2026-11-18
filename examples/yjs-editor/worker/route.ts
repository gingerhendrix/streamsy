import { Array } from "effect";

export interface DocumentRoute {
  readonly service: string;
  readonly docPath: string;
}

export const documentId = (route: DocumentRoute) => `yjs/${route.service}/docs/${route.docPath}`;

/** Match the pinned upstream path grammar, including encoded traversal rejection. */
export const parseDocumentRoute = (pathname: string): DocumentRoute | null => {
  const match = /^\/v1\/yjs\/([^/]+)\/docs\/(.+)$/.exec(pathname);

  if (!match?.[1] || !match[2]) return null;
  let path: string;

  try {
    path = decodeURIComponent(match[2]).replace(/\/+/g, "/");
  } catch {
    return null;
  }

  if (
    !/^[a-zA-Z0-9_\-/]+$/.test(path) ||
    path.length > 256 ||
    Array.some(path.split("/"), (segment) => segment === "." || segment === "..")
  )
    return null;

  return { service: match[1], docPath: path };
};
