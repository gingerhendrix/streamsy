/* oxlint-disable effecttsgo/async-function -- Native Workers fetch/scheduled transport boundary. */
import { Option, Schema } from "effect";
import { Http } from "@streamsy/core";
import { bearerMatches } from "../server/auth.ts";
import { FolderId } from "../server/domain.ts";
import { FolderKey } from "../server/streams.ts";
import { handleBlobRequest } from "./blob-api.ts";
import { folderObject, indexObject } from "./placement.ts";
import type { ScheduledController } from "@cloudflare/workers-types";
import type { Env } from "./env.ts";

export { FolderObject } from "./objects.ts";

export { IndexObject } from "./index-object.ts";

export { folderOwner, placement } from "./placement.ts";

const json = (error: string, status: number) => Response.json({ error }, { status });

/** The hourly backstop checks source tails from every stored folder cursor. */
export const scheduledWake = async (env: Env) => {
  const response = await indexObject(env).fetch("https://internal/__catchup", { method: "POST" });

  if (!response.ok) throw new Error(`Index catch-up answered ${response.status}`);
  await response.text();
};

/** Forward to a Durable Object. A failed call means the object could not start. */
const forward = async (
  object: { fetch: (request: Request) => Promise<Response> },
  request: Request,
) => {
  try {
    return await object.fetch(request);
  } catch {
    return json("Folder unavailable; retry", 503);
  }
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

    if (write && !bearerMatches(request.headers.get("authorization"), env.WRITER_TOKEN))
      return json("Unauthorized", 401);

    if (url.pathname === "/api/admin/wake" && request.method === "POST") {
      try {
        await scheduledWake(env);

        return new Response("awake");
      } catch {
        return json("Index catch-up failed", 503);
      }
    }

    // The index object owns the registry. It validates the body, registers the
    // folder, then creates the stream in the folder object.
    if (url.pathname === "/api/folders" && request.method === "POST")
      return forward(indexObject(env), request);

    const stream = /^\/streams\/folders\/([^/]+)\/files$/.exec(url.pathname);

    if (stream?.[1]) {
      return Option.match(Schema.decodeOption(FolderKey)(stream[1]), {
        onNone: () => json("Invalid folder stream", 400),
        onSome: (folderId) => forward(folderObject(env, folderId), request),
      });
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

      return forward(folderObject(env, folderId), request);
    }

    if (url.pathname.startsWith("/api/blobs/")) return handleBlobRequest(request, env);

    if (["/api/ls", "/api/search", "/streams/folders", "/health"].includes(url.pathname))
      return indexObject(env).fetch(request);

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
