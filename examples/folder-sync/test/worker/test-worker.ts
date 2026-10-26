/* oxlint-disable effecttsgo/async-function -- Native Workers fetch boundary of the test-only entry. */
import { Effect, Layer, Schema } from "effect";
import { Http, Streamsy } from "@streamsy/core";
import { StreamsyObject } from "@streamsy/serve/cloudflare";
import { fileResponse, folderConfiguration } from "../../worker/objects.ts";
import { Files, FolderKey, FolderWake } from "../../server/streams.ts";
import { bearerMatches } from "../../server/auth.ts";
import worker, { IndexObject as ProductionIndexObject } from "../../worker/index.ts";
import type { Env } from "../../worker/env.ts";
import { folderObject, indexObject } from "../../worker/placement.ts";

/**
 * The production folder object plus two switches. While `createDelay` is set,
 * the internal create waits that many milliseconds before it creates the stream.
 * Tests use it to widen the gap between the two steps of a Worker folder create.
 */
export class FolderObject extends StreamsyObject.make<
  Env,
  Layer.Success<ReturnType<typeof folderConfiguration.layer>>,
  FolderWake
>({
  ...folderConfiguration,
  app: Layer.merge(
    folderConfiguration.app,
    Http.routes(Streamsy.group(Files), { prefix: "/__test/streams", access: "full" }),
  ),
}) {
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

    return fileResponse(request, await super.fetch(request), (work) => this.ctx.waitUntil(work));
  }
}

/** Suppress index alarms to leave durable wake rows unprocessed until restart. */
export class IndexObject extends ProductionIndexObject {
  override async fetch(request: Request): Promise<Response> {
    if (new URL(request.url).pathname === "/__test/index") {
      const { pauseIndex } = await request.json<{ pauseIndex: boolean }>();
      await this.ctx.storage.put("pauseIndex", pauseIndex);

      return new Response("configured");
    }

    return super.fetch(request);
  }

  override async alarm(): Promise<void> {
    if (await this.ctx.storage.get<boolean>("pauseIndex")) return;

    return super.alarm();
  }
}

/** Test-only controls are authenticated and never included in the deployed entry. */
export default {
  ...worker,
  async fetch(request: Request, env: Env): Promise<Response> {
    const { pathname } = new URL(request.url);

    if (
      request.method !== "POST" ||
      ![
        "/api/admin/test-index",
        "/api/admin/test-folder",
        "/api/admin/test-remove-folder",
      ].includes(pathname)
    )
      return worker.fetch(request, env);

    if (!bearerMatches(request.headers.get("authorization"), env.WRITER_TOKEN))
      return Response.json({ error: "Unauthorized" }, { status: 401 });

    if (pathname === "/api/admin/test-index")
      return indexObject(env).fetch("https://internal/__test/index", {
        method: "POST",
        body: request.body,
      });

    if (pathname === "/api/admin/test-remove-folder") {
      const { folderId } = await request.json<{ folderId: string }>();
      const key = await Effect.runPromise(Schema.encodeEffect(FolderKey)(folderId));

      return folderObject(env, folderId).fetch(
        `https://internal/__test/streams/folders/${key}/files`,
        { method: "DELETE" },
      );
    }

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
