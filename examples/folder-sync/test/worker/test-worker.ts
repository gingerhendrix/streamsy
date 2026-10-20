/* oxlint-disable effecttsgo/async-function -- Native Workers fetch boundary of the test-only entry. */
import { bearerMatches } from "../../server/auth.ts";
import worker, { FolderObject, IndexObject as ProductionIndexObject } from "../../worker/index.ts";
import type { Env } from "../../worker/env.ts";
import { indexObject } from "../../worker/placement.ts";

export { FolderObject };

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

/** The production entry, plus `POST /api/admin/test-index` to set the switch. */
export default {
  ...worker,
  async fetch(request: Request, env: Env): Promise<Response> {
    const { pathname } = new URL(request.url);

    if (pathname !== "/api/admin/test-index" || request.method !== "POST")
      return worker.fetch(request, env);

    if (!bearerMatches(request.headers.get("authorization"), env.WRITER_TOKEN))
      return Response.json({ error: "Unauthorized" }, { status: 401 });

    return indexObject(env).fetch("https://internal/__test/index", {
      method: "POST",
      body: request.body,
    });
  },
};
