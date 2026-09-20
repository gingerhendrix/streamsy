import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

export const packageDir = resolve(import.meta.dir, "..");
export const scratchDirectory = async (name: string) => {
  const directory = await mkdtemp(join(tmpdir(), `${name}-`));
  return {
    directory,
    database: join(directory, "tracker.sqlite"),
    remove: () => rm(directory, { recursive: true, force: true }),
  };
};
export const startServer = (port: number, database: string) =>
  Bun.spawn(["bun", "server/index.ts"], {
    cwd: packageDir,
    env: {
      ...process.env,
      PORT: String(port),
      ISSUE_TRACKER_DB: database,
      ISSUE_TRACKER_WORKSPACES: "acme,live",
    },
    stdout: "pipe",
    stderr: "pipe",
  });
export const stopServer = async (process: ReturnType<typeof startServer>) => {
  process.kill("SIGTERM");
  await process.exited;
};
export const waitForServer = async (baseUrl: string) => {
  const deadline = Date.now() + 10_000;
  while (Date.now() < deadline) {
    try {
      if ((await fetch(baseUrl)).ok) return;
    } catch {
      /* server is starting */
    }
    await Bun.sleep(50);
  }
  throw new Error("server did not become ready");
};
export const requestJson = async <A>(
  baseUrl: string,
  path: string,
  init?: RequestInit,
): Promise<A> => {
  const response = await fetch(`${baseUrl}${path}`, init);
  const text = await response.text();
  if (!response.ok) throw new Error(`${init?.method ?? "GET"} ${path}: ${response.status} ${text}`);
  // SAFETY: each evidence call names a concrete response shape and immediately
  // asserts the fields that matter to that scenario.
  return JSON.parse(text) as A;
};
type JsonValue =
  | string
  | number
  | boolean
  | null
  | ReadonlyArray<JsonValue>
  | { readonly [key: string]: JsonValue };
export const post = <A>(baseUrl: string, path: string, body: JsonValue) =>
  requestJson<A>(baseUrl, path, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
