export async function serveStatic(url: URL): Promise<Response> {
  const pathname = url.pathname === "/" ? "/index.html" : url.pathname;
  const candidate = `${import.meta.dir}/../../dist${pathname}`;
  const file = Bun.file(candidate);
  if (await file.exists()) return new Response(file);

  if (!pathname.includes(".")) {
    const index = Bun.file(`${import.meta.dir}/../../dist/index.html`);
    if (await index.exists()) return new Response(index);
  }

  return new Response(
    "Hacker News newest stream API is running. Run `bun run build` in examples/hackernews-newest-stream to serve the React app from this Bun server, or run `bun run dev:web` for Vite dev.",
    { status: 200, headers: { "content-type": "text/plain" } },
  );
}
