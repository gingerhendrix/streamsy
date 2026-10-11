/* oxlint-disable effecttsgo/async-function -- Native Workers fetch/scheduled transport boundary. */
import { Placement, rule } from "@streamsy/serve/cloudflare";
import { DateTime, Schema } from "effect";
import { CreateFolder } from "../server/domain.ts";
import { Files, Folder } from "../server/streams.ts";
import type { ScheduledController } from "@cloudflare/workers-types";
import type { Env } from "./env.ts";

export { FolderObject, IndexObject } from "./objects.ts";

// Placement decodes the internal hex stream segment back to the opaque id. Object names use the id itself.
export const placement = Placement.byRoute([
  rule({ stream: Files, owner: ({ folderId }) => folderId }),
]);

export const folderOwner = (folderId: string) => placement.name(Files.at({ folderId }).id);

const index = (env: Env) => env.INDEX.get(env.INDEX.idFromName("index"));

const folder = (env: Env, folderId: string) =>
  env.FOLDERS.get(env.FOLDERS.idFromName(folderOwner(folderId)));

const json = (error: string, status: number) =>
  new Response(JSON.stringify({ error }), {
    status,
    headers: { "content-type": "application/json" },
  });

/** Equal-length credentials use the Workers constant-time primitive. Only length can short-circuit. */
const authorized = (request: Request, env: Env) => {
  const expected = new TextEncoder().encode(`Bearer ${env.WRITER_TOKEN}`);
  const supplied = new TextEncoder().encode(request.headers.get("authorization") ?? "");

  const equal =
    supplied.length === expected.length && crypto.subtle.timingSafeEqual(supplied, expected);

  return Boolean(env.WRITER_TOKEN) && equal;
};

/** One scheduled pass, also called by the authenticated local trigger. A failed folder does not stop the other folders. */
export const scheduledDrain = async (env: Env) => {
  const response = await index(env).fetch("https://internal/__registry");

  if (!response.ok) throw new Error(`Registry unavailable: ${response.status}`);
  const folders = Schema.decodeUnknownSync(Schema.Array(Folder))(await response.json());

  for (const row of folders) {
    try {
      const delivered = await folder(env, row.folderId).fetch("https://internal/__drain", {
        method: "POST",
      });

      if (!delivered.ok) throw new Error(`Drain answered ${delivered.status}`);
    } catch (error) {
      // oxlint-disable-next-line effecttsgo/global-console -- Platform scheduled boundary reports deferred delivery for the next cron pass.
      console.error(`Scheduled drain failed for ${row.folderId}`, error);
    }
  }
};

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const write = !["GET", "HEAD", "OPTIONS"].includes(request.method);

    if (write && !authorized(request, env)) return json("Writer authorization required", 401);

    // Internal object routes are not exposed through the public router.
    if (url.pathname === "/api/admin/drain" && request.method === "POST") {
      try {
        await scheduledDrain(env);

        return new Response("drained");
      } catch {
        return json("Scheduled drain failed", 503);
      }
    }

    if (
      url.pathname === "/api/admin/test-intake" &&
      request.method === "POST" &&
      env.TEST_HOOKS === "true"
    )
      return index(env).fetch("https://internal/__test/intake", {
        method: "POST",
        body: await request.text(),
      });

    if (
      url.pathname === "/api/admin/pending" &&
      request.method === "GET" &&
      env.TEST_HOOKS === "true" &&
      authorized(request, env)
    )
      return folder(env, url.searchParams.get("folder") ?? "").fetch("https://internal/__pending");

    if (url.pathname === "/api/folders" && request.method === "POST") {
      if (
        request.headers.get("content-type")?.split(";")[0]?.trim().toLowerCase() !==
        "application/json"
      )
        return json("Expected application/json", 415);
      let payload: { folderId: string; title: string };

      try {
        payload = Schema.decodeUnknownSync(CreateFolder)(await request.json());
      } catch (error) {
        return json(String(error), 400);
      }

      if (!payload.folderId) return json("Invalid folder", 400);

      // Register before creating the folder so a crash cannot orphan it from the cron registry.
      const registration = await index(env).fetch("https://internal/__register", {
        method: "POST",
        body: JSON.stringify({
          ...payload,
          created: Schema.encodeSync(Schema.DateTimeUtcFromString)(DateTime.nowUnsafe()),
        }),
      });

      if (!registration.ok) return json("Registry unavailable; retry", 503);

      return folder(env, payload.folderId).fetch(request.url, {
        method: "POST",
        headers: request.headers,
        body: JSON.stringify(payload),
      });
    }

    const match = /^\/api\/folders\/([^/]+)\/(?:files|history)\//.exec(url.pathname);

    if (match?.[1]) {
      try {
        return await folder(env, decodeURIComponent(match[1])).fetch(request);
      } catch {
        return json("Folder unavailable; retry", 503);
      }
    }

    if (url.pathname.startsWith("/api/blobs/")) {
      // The file API's immutable blob handler shares the R2 layer in any folder object.
      return folder(env, "__blob-reader").fetch(request.url, {
        method: request.method,
        headers: request.headers,
      });
    }

    if (
      ["/api/ls", "/api/search", "/streams/changes", "/streams/manifest", "/health"].includes(
        url.pathname,
      )
    )
      return index(env).fetch(request.url, { method: request.method, headers: request.headers });

    if (
      url.pathname.startsWith("/api/") ||
      url.pathname.startsWith("/streams/") ||
      url.pathname.startsWith("/__")
    )
      return json("Not found", 404);

    return env.ASSETS.fetch(request);
  },
  async scheduled(_event: ScheduledController, env: Env): Promise<void> {
    await scheduledDrain(env);
  },
};
