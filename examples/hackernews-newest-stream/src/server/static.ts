const contentTypes: Record<string, string> = {
  ".css": "text/css; charset=utf-8",
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".map": "application/json; charset=utf-8",
  ".svg": "image/svg+xml",
};

function contentType(pathname: string): string | undefined {
  const extension = pathname.match(/\.[^.]+$/)?.[0];
  return extension ? contentTypes[extension] : undefined;
}

async function fileResponse(pathname: string, candidate: string): Promise<Response | null> {
  const file = Bun.file(candidate);
  if (!(await file.exists())) return null;
  const type = contentType(pathname);
  return new Response(file, type ? { headers: { "content-type": type } } : undefined);
}

export async function serveStatic(url: URL): Promise<Response> {
  const pathname = url.pathname === "/" ? "/index.html" : url.pathname;
  const direct = await fileResponse(pathname, `${import.meta.dir}/../../dist${pathname}`);
  if (direct) return direct;

  if (!pathname.includes(".")) {
    const fallback = await fileResponse("/index.html", `${import.meta.dir}/../../dist/index.html`);
    if (fallback) return fallback;
  }

  return new Response(
    "Hacker News newest stream API is running. Run `bun run build` in examples/hackernews-newest-stream to serve the React app from this Bun server.",
    { status: 200, headers: { "content-type": "text/plain" } },
  );
}
