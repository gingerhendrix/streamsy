import { spawn } from "node:child_process";
import { access, readdir, readFile } from "node:fs/promises";
import { createServer } from "node:net";
import { join, relative } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { z } from "zod";

const site = new URL("..", import.meta.url).pathname;
const routes = new Set(["/", "/articles"]);
async function inventory(directory: string, prefix: string): Promise<void> {
  const entries = await readdir(directory, { withFileTypes: true });
  for (const entry of entries) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) await inventory(path, prefix);
    else if (/\.mdx?$/.test(entry.name)) {
      const slug = relative(join(site, "content", prefix), path)
        .replace(/\.mdx?$/, "")
        .replace(/(^|\/)index$/, "");
      routes.add(`/${prefix}/${slug}`.replace(/\/$/, ""));
    } else if (entry.name === "meta.json") {
      const meta: { pages?: string[] } = JSON.parse(await readFile(path, "utf8"));
      for (const page of meta.pages ?? []) {
        if (page === "..." || page.startsWith("---") || page.startsWith("!")) continue;
        if (
          !entries.some(
            (candidate) =>
              candidate.name === page ||
              candidate.name === `${page}.mdx` ||
              candidate.name === `${page}.md`,
          )
        ) {
          throw new Error(`Broken navigation entry: ${path} → ${page}`);
        }
      }
    }
  }
}
await inventory(join(site, "content/docs"), "docs");
await inventory(join(site, "content/articles"), "articles");
const reservation = createServer();
await new Promise<void>((resolve, reject) => {
  reservation.once("error", reject);
  reservation.listen(0, "127.0.0.1", resolve);
});
const { port } = z.object({ port: z.number().int().positive() }).parse(reservation.address());
await new Promise<void>((resolve, reject) =>
  reservation.close((error) => (error ? reject(error) : resolve())),
);
const origin = `http://127.0.0.1:${port}`;
const child = spawn(
  process.execPath,
  ["run", "dev", "--host", "127.0.0.1", "--port", String(port), "--strictPort"],
  {
    cwd: site,
    detached: true,
    env: { ...process.env, SITE_BASE_PATH: "", PORT: String(port), NO_COLOR: "1" },
    stdio: ["ignore", "pipe", "pipe"],
  },
);
let output = "";
child.stdout.on("data", (chunk) => {
  output += String(chunk);
});
child.stderr.on("data", (chunk) => {
  output += String(chunk);
});
// Own the complete local process group, including the development emulator.
function signalGroup(signal: NodeJS.Signals): void {
  if (child.pid === undefined) return;
  try {
    process.kill(-child.pid, signal);
  } catch (error) {
    if (!(error instanceof Error && "code" in error && error.code === "ESRCH")) throw error;
  }
}
const stopped = new Promise<void>((resolve, reject) => {
  child.once("error", reject);
  child.once("close", () => resolve());
});
const decode = (value: string) =>
  value
    .replace(/&amp;/g, "&")
    .replace(/&#(?:x([\da-f]+)|(\d+));/gi, (_, hex: string | undefined, decimal: string) =>
      String.fromCodePoint(Number.parseInt(hex ?? decimal, hex ? 16 : 10)),
    );
const attributes = (html: string, attribute: string): string[] =>
  [...html.matchAll(new RegExp(`\\b${attribute}=["']([^"']*)["']`, "g"))].map((match) =>
    decode(match[1]),
  );
const pages = new Map<string, string>();
let checked = 0;
try {
  let ready = false;
  for (let attempt = 0; attempt < 120; attempt++) {
    if (child.exitCode !== null) throw new Error(`Site server exited: ${child.exitCode}`);
    try {
      const response = await fetch(origin, { signal: AbortSignal.timeout(1000) });
      if (response.ok) {
        ready = true;
        break;
      }
    } catch {
      /* The listener and route compiler are still starting. */
    }
    await delay(250);
  }
  if (!ready) throw new Error("Site server did not become ready");
  for (const route of routes) {
    const response = await fetch(origin + route);
    if (!response.ok) throw new Error(`Broken page ${route}: ${response.status}`);
    const html = await response.text();
    if (!html.includes("<h1")) throw new Error(`No rendered heading: ${route}`);
    pages.set(route, html);
  }
  for (const [route, html] of pages) {
    const tags = html.match(/<(?:a|img)\b[^>]*>/g) ?? [];
    for (const tag of tags) {
      for (const href of [...attributes(tag, "href"), ...attributes(tag, "src")]) {
        const url = new URL(href, origin + route);
        const sourcePrefix =
          "https://github.com/gingerhendrix/streamsy/blob/effect-first-live-perimeter/";
        if (url.href.startsWith(sourcePrefix)) {
          await access(
            new URL(
              url.pathname.split("/blob/effect-first-live-perimeter/")[1],
              new URL("../../", import.meta.url),
            ),
          );
        }
        if (url.origin !== origin) continue;
        const path = url.pathname.replace(/\/$/, "") || "/";
        let target = pages.get(path);
        if (target === undefined) {
          const response = await fetch(url);
          if (!response.ok) throw new Error(`Broken link: ${route} → ${href} (${response.status})`);
          target = await response.text();
          pages.set(path, target);
        }
        if (url.hash && !attributes(target, "id").includes(decodeURIComponent(url.hash.slice(1)))) {
          throw new Error(`Broken anchor: ${route} → ${href}`);
        }
        checked++;
      }
    }
  }
  console.log(
    `Site links: ${routes.size} rendered routes, ${checked} internal links/images and anchors checked.`,
  );
} catch (error) {
  console.error(output);
  throw error;
} finally {
  signalGroup("SIGTERM");
  const forced = setTimeout(() => signalGroup("SIGKILL"), 5000);
  await stopped;
  clearTimeout(forced);
  signalGroup("SIGKILL");
  console.log("Isolated link-check server stopped.");
}
