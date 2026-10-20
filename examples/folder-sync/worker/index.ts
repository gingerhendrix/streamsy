/* oxlint-disable effecttsgo/async-function -- Native Workers fetch/scheduled transport boundary. */
import { Placement, rule } from "@streamsy/serve/cloudflare";
import { Schema } from "effect";
import { Http } from "@streamsy/core";
import { CreateFolder, FolderId } from "../server/domain.ts";
import { JSON_BODY_LIMIT } from "../server/api.ts";
import { Files, FolderKey } from "../server/streams.ts";
import { handleBlobRequest } from "./blob-api.ts";
import type { ScheduledController } from "@cloudflare/workers-types";
import type { Env } from "./env.ts";

export { FolderObject } from "./objects.ts";

export { IndexObject } from "./index-object.ts";

/** Placement decodes the hex URL segment. Object names use the opaque folder id. */
export const placement = Placement.byRoute([
  rule({ stream: Files, owner: ({ folderId }) => folderId }),
]);

export const folderOwner = (folderId: string) => placement.name(Files.at({ folderId }).id);

const index = (env: Env) => env.INDEX.get(env.INDEX.idFromName("index"));

const folder = (env: Env, folderId: string) =>
  env.FOLDERS.get(env.FOLDERS.idFromName(folderOwner(folderId)));

const json = (error: string, status: number) => Response.json({ error }, { status });

/** Equal-length credentials use the Workers constant-time primitive. */
const authorized = (request: Request, env: Env) => {
  const expected = new TextEncoder().encode(`Bearer ${env.WRITER_TOKEN}`);
  const supplied = new TextEncoder().encode(request.headers.get("authorization") ?? "");

  return (
    Boolean(env.WRITER_TOKEN) &&
    supplied.length === expected.length &&
    crypto.subtle.timingSafeEqual(supplied, expected)
  );
};

/** The hourly backstop checks source tails from every stored folder cursor. */
export const scheduledWake = async (env: Env) => {
  const response = await index(env).fetch("https://internal/__catchup", { method: "POST" });

  if (!response.ok) throw new Error(`Index catch-up answered ${response.status}`);
  await response.text();
};

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    // Stream endpoints are always read-only, including unauthenticated write attempts.
    if (url.pathname.startsWith("/streams/") && !["GET", "HEAD"].includes(request.method))
      return new Response(null, {
        status: 405,
        headers: { ...Http.securityHeaders, allow: "GET, HEAD" },
      });
    const write = !["GET", "HEAD", "OPTIONS"].includes(request.method);

    if (write && !authorized(request, env)) return json("Unauthorized", 401);

    if (url.pathname === "/api/admin/wake" && request.method === "POST") {
      try {
        await scheduledWake(env);

        return new Response("awake");
      } catch {
        return json("Index catch-up failed", 503);
      }
    }

    if (env.TEST_HOOKS === "true" && request.method === "POST") {
      if (url.pathname === "/api/admin/test-index")
        return index(env).fetch("https://internal/__test/index", {
          method: "POST",
          body: request.body,
        });

      if (url.pathname === "/api/admin/test-head")
        return folder(env, url.searchParams.get("folder") ?? "").fetch(
          "https://internal/__test/head",
          {
            method: "POST",
            body: request.body,
          },
        );
    }

    if (url.pathname === "/api/folders" && request.method === "POST") {
      if (
        request.headers.get("content-type")?.split(";")[0]?.trim().toLowerCase() !==
        "application/json"
      )
        return json("Expected application/json", 415);

      if (Number(request.headers.get("content-length")) > JSON_BODY_LIMIT)
        return json("JSON write bodies must be at most 512 KiB", 413);
      // Bound chunked bodies before parsing or making either object call.
      const reader = request.body?.getReader();
      const chunks: Uint8Array[] = [];
      let size = 0;

      if (reader) {
        try {
          while (true) {
            const part = await reader.read();

            if (part.done) break;
            size += part.value.length;

            if (size > JSON_BODY_LIMIT) {
              await reader.cancel();

              return json("JSON write bodies must be at most 512 KiB", 413);
            }

            chunks.push(part.value);
          }
        } finally {
          reader.releaseLock();
        }
      }

      const bytes = new Uint8Array(size);
      let offset = 0;

      for (const chunk of chunks) {
        bytes.set(chunk, offset);
        offset += chunk.length;
      }

      let payload: typeof CreateFolder.Type;

      try {
        payload = Schema.decodeUnknownSync(CreateFolder)(
          JSON.parse(new TextDecoder().decode(bytes)),
        );
      } catch {
        return json("Invalid request", 400);
      }

      const body = JSON.stringify(payload);

      // Registry first: a crash leaves a discoverable folder that a retry can finish creating.
      const registration = await index(env).fetch("https://internal/__register", {
        method: "POST",
        body,
      });

      if (!registration.ok) return registration;
      await registration.text();

      const created = await folder(env, payload.folderId).fetch("https://internal/__create", {
        method: "POST",
        body,
      });

      return created;
    }

    const stream = /^\/streams\/folders\/([^/]+)\/files$/.exec(url.pathname);

    if (stream?.[1]) {
      let folderId: string;

      try {
        folderId = Schema.decodeSync(FolderKey)(stream[1]);
      } catch {
        return json("Invalid folder stream", 400);
      }

      try {
        return await folder(env, folderId).fetch(request);
      } catch {
        return json("Folder unavailable; retry", 503);
      }
    }

    const file = /^\/api\/folders\/([^/]+)\/files\//.exec(url.pathname);

    if (file?.[1]) {
      let folderId: string;

      try {
        folderId = decodeURIComponent(file[1]);
      } catch {
        // Bun's router rejects malformed URI segments before dispatching the API.
        return new Response(null, { status: 404 });
      }

      if (!Schema.is(FolderId)(folderId)) return json("Invalid request", 400);

      try {
        return await folder(env, folderId).fetch(request);
      } catch {
        return json("Folder unavailable; retry", 503);
      }
    }

    if (url.pathname.startsWith("/api/blobs/")) return handleBlobRequest(request, env);

    if (["/api/ls", "/api/search", "/streams/folders", "/health"].includes(url.pathname))
      return index(env).fetch(request);

    if (
      url.pathname.startsWith("/api/") ||
      url.pathname.startsWith("/streams/") ||
      url.pathname.startsWith("/__")
    )
      return json("Not found", 404);

    return env.ASSETS.fetch(request);
  },
  async scheduled(_event: ScheduledController, env: Env): Promise<void> {
    await scheduledWake(env);
  },
};
