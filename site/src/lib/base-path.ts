const rawBasePath = process.env.SITE_BASE_PATH ?? "";

export const basePath = normalizeBasePath(rawBasePath);

export function withBasePath(path: `/${string}`) {
  if (!basePath) return path;
  if (path === "/") return basePath;
  return `${basePath}${path}` as const;
}

function normalizeBasePath(path: string) {
  const trimmed = path.trim().replace(/^\/+|\/+$/g, "");
  return trimmed ? `/${trimmed}` : "";
}
