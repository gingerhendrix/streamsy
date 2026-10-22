/* oxlint-disable effecttsgo/async-function -- Native Workers fetch boundary of the test-only entry. */
import { Effect } from "effect";
import { bearerMatches } from "../../server/auth.ts";
import worker, {
  FolderObject as ProductionFolderObject,
  IndexObject as ProductionIndexObject,
} from "../../worker/index.ts";
import type { Env } from "../../worker/env.ts";
import { folderObject, indexObject } from "../../worker/placement.ts";

/**
 * The production folder object plus one switch: while `createDelay` is set, the
 * internal create waits that many milliseconds before it creates the stream.
 * Tests use it to widen the gap between the two steps of a Worker folder create.
 */
export class FolderObject extends ProductionFolderObject {
  override async fetch(request: Request): Promise<Response> {
    const { pathname } = new URL(request.url);

    if (pathname === "/__test/folder" && request.method === "POST") {
      const { createDelay } = await request.json<{ createDelay: number }>();
      await this.ctx.storage.put("createDelay", createDelay);

      return new Response("configured");
    }

    if (pathname === "/__create") {
      const delay = (await this.ctx.storage.get<number>("createDelay")) ?? 0;

      if (delay > 0) await Effect.runPromise(Effect.sleep(delay));
    }

    return super.fetch(request);
  }
}

/**
 * The production index object plus one switch: while `dropPings` is set, a
 * folder ping answers at once and indexes nothing. Tests use it to prove that
 * the hourly catch-up finds writes whose ping was lost.
 */
export class IndexObject extends ProductionIndexObject {
  override async fetch(request: Request): Promise<Response> {
    const { pathname } = new URL(request.url);

    if (pathname === "/__test/index" && request.method === "POST") {
      const { dropPings } = await request.json<{ dropPings: boolean }>();
      await this.ctx.storage.put("dropPings", dropPings === true);

      return new Response("configured");
    }

    if (pathname === "/__moved" && (await this.ctx.storage.get<boolean>("dropPings")) === true)
      return new Response("indexed");

    return super.fetch(request);
  }
}

/**
 * The production entry, plus two admin routes for the switches:
 * `POST /api/admin/test-index` and `POST /api/admin/test-folder`.
 */
export default {
  ...worker,
  async fetch(request: Request, env: Env): Promise<Response> {
    const { pathname } = new URL(request.url);

    if (
      request.method !== "POST" ||
      !["/api/admin/test-index", "/api/admin/test-folder"].includes(pathname)
    )
      return worker.fetch(request, env);

    if (!bearerMatches(request.headers.get("authorization"), env.WRITER_TOKEN))
      return Response.json({ error: "Unauthorized" }, { status: 401 });

    if (pathname === "/api/admin/test-index")
      return indexObject(env).fetch("https://internal/__test/index", {
        method: "POST",
        body: request.body,
      });

    const { folderId, createDelay } = await request.json<{
      folderId: string;
      createDelay: number;
    }>();

    return folderObject(env, folderId).fetch("https://internal/__test/folder", {
      method: "POST",
      body: JSON.stringify({ createDelay }),
    });
  },
};
