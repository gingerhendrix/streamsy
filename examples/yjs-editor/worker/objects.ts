/* oxlint-disable effecttsgo/async-function -- Native Workers adapter boundary delegates stream operations to Streamsy. */
import type { DurableObjectState } from "@cloudflare/workers-types";
import { Http, Streamsy } from "@streamsy/core";
import { StreamsReader, StreamsWriter } from "@streamsy/core/protocol";
import { CheckpointStore, Projection, Transaction } from "@streamsy/projection";
import { StreamsyObject } from "@streamsy/serve/cloudflare";
import { DurableObjectStorage } from "@streamsy/storage/durable-object";
import { Digest, SecureRandom } from "effect/crypto";
import { Array, Clock, Effect, Layer } from "effect";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/http";
import { compact, documentFold } from "./compaction.ts";
import type { Env } from "./env.ts";

const streams = Streamsy.group(Streamsy.Wildcard);

const internal = HttpRouter.add(
  "POST",
  "/__fold",
  Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const url = new URL(request.url, "https://internal");
    const document = url.searchParams.get("document")!;

    if (url.searchParams.has("forget")) yield* Projection.forget(documentFold(document));
    else yield* compact(document, Number(url.searchParams.get("threshold")));

    return HttpServerResponse.empty({ status: 204 });
  }).pipe(
    Effect.catchCause((cause) =>
      Effect.logError("Yjs fold failed", cause).pipe(
        Effect.as(HttpServerResponse.empty({ status: 500 })),
      ),
    ),
  ),
);

const storage = (state: DurableObjectState) =>
  Layer.mergeAll(
    CheckpointStore.layer,
    Transaction.layer,
    Layer.effect(
      StreamsReader,
      Effect.map(Streamsy.Wildcard, (service) => service.wire),
    ),
    Layer.effect(
      StreamsWriter,
      Effect.map(Streamsy.Wildcard, (service) => service.wire),
    ),
  ).pipe(
    Layer.provideMerge(DurableObjectStorage.layer(streams, { client: { storage: state.storage } })),
    Layer.provideMerge(Layer.merge(Digest.layerWebCrypto, SecureRandom.layerWebCrypto)),
  );

export const protocolError = (code: string, message: string, status: number) =>
  Response.json({ error: { code, message } }, { status });

const missingDocument = () => protocolError("DOCUMENT_NOT_FOUND", "Document does not exist", 404);

const binary = "application/octet-stream";

const rawPrefix = "/v1/stream/";

export interface DocumentRoute {
  readonly service: string;
  readonly docPath: string;
}

export const documentId = (route: DocumentRoute) => `yjs/${route.service}/docs/${route.docPath}`;

/** One object owns every stream and checkpoint belonging to a document. */
export class DocumentObject extends StreamsyObject.make({
  app: Layer.merge(
    Http.routes(streams, { prefix: "/v1/stream", access: "full", maxMessageSize: 4 * 1024 * 1024 }),
    internal,
  ),
  layer: (state: DurableObjectState, _env: Env) => storage(state),
}) {
  // Only scheduling lives in memory. All fold values, counters, indexes and streams are durable.
  #mutations: Promise<void> = Promise.resolve();

  override fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const route = parseDocumentRoute(url.pathname);

    const work = () =>
      route === null ? super.fetch(request) : this.#document(request, url, route);

    // Never hold a mutation permit for an SSE or long-poll read.
    if (["GET", "HEAD"].includes(request.method) && url.searchParams.get("offset") !== "snapshot")
      return work();
    const pending = this.#mutations.then(work);
    this.#mutations = pending.then(
      () => undefined,
      () => undefined,
    );

    return pending;
  }

  #raw(id: string, init: RequestInit = {}, query = "") {
    return super.fetch(new Request(`https://internal${rawPrefix}${id}${query}`, init));
  }

  async #forward(request: Request, id: string, query = "", body?: Uint8Array) {
    // The Yjs endpoint accepts updates, not base-protocol stream closure. The
    // stock provider calls producer.close() on disconnect; forwarding Stream-Closed
    // would stop every other peer. Match the reference server's write header policy.
    const headers = request.method === "POST" ? new Headers() : new Headers(request.headers);

    if (request.method === "POST") {
      for (const name of [
        "producer-id",
        "producer-epoch",
        "producer-seq",
        "stream-producer-id",
        "stream-producer-epoch",
        "stream-producer-seq",
      ]) {
        const value = request.headers.get(name);

        if (value !== null) headers.set(name, value);
      }
    }

    headers.delete("host");

    if (request.method === "POST" || !headers.has("content-type"))
      headers.set("content-type", binary);

    return this.#raw(id, { method: request.method, headers, body, signal: request.signal }, query);
  }

  async #fold(id: string, forget = false) {
    const params = new URLSearchParams({
      document: id,
      threshold: this.env.COMPACTION_THRESHOLD || "1048576",
    });

    if (forget) params.set("forget", "true");

    const result = await super.fetch(
      new Request(`https://internal/__fold?${params.toString()}`, { method: "POST" }),
    );

    if (!result.ok) throw new Error("Compaction failed");
  }

  async #createAwareness(id: string, name: string) {
    const result = await this.#raw(`${id}/.awareness/${name}`, {
      method: "PUT",
      headers: { "content-type": binary, "stream-ttl": "3600" },
    });

    if (!result.ok) throw new Error(`Awareness create failed: ${result.status}`);

    if (result.status === 201 && name !== "default")
      await this.#appendJson(`${id}/.awareness-index`, {
        name,
        createdAt: await Effect.runPromise(Clock.currentTimeMillis),
      });

    return result;
  }

  async #appendJson(id: string, entry: { name: string; createdAt: number }) {
    const headers = { "content-type": "application/json" };
    const created = await this.#raw(id, { method: "PUT", headers });

    if (!created.ok) throw new Error("Registry create failed");

    const appended = await this.#raw(id, {
      method: "POST",
      headers,
      body: JSON.stringify([entry]),
    });

    if (!appended.ok) throw new Error("Registry append failed");
  }

  async #entries(id: string): Promise<Array<{ name?: string; snapshotOffset?: string }>> {
    const response = await this.#raw(id, {}, "?offset=-1");

    if (response.status === 404) return [];

    if (!response.ok) throw new Error(`Index read failed: ${response.status}`);

    return response.json();
  }

  async #document(request: Request, url: URL, route: DocumentRoute): Promise<Response> {
    const id = documentId(route);
    const updates = `${id}/.updates`;
    const method = request.method;

    // Buffer writes once: retries for an expired awareness stream reuse these bytes.
    const body = ["PUT", "POST"].includes(method)
      ? new Uint8Array(await request.arrayBuffer())
      : undefined;

    if (!["GET", "HEAD", "PUT", "POST", "DELETE"].includes(method))
      return protocolError("INVALID_REQUEST", "Method not allowed", 405);
    const awareness = url.searchParams.get("awareness");

    if (awareness !== null) {
      if (!/^[a-zA-Z0-9_.-]{1,256}$/.test(awareness) || [".", "..", ".index"].includes(awareness))
        return protocolError("INVALID_REQUEST", "Invalid awareness name", 400);
      const target = `${id}/.awareness/${awareness}`;

      if (method === "PUT" || method === "POST") {
        if ((await this.#raw(updates, { method: "HEAD" })).status === 404) return missingDocument();

        if (method === "PUT") {
          const created = await this.#createAwareness(id, awareness);

          return new Response(null, { status: created.status });
        }

        let result = await this.#forward(request, target, "", body);

        if (result.status === 404) {
          await result.arrayBuffer();
          await this.#createAwareness(id, awareness);
          result = await this.#forward(request, target, "", body);
        }

        return result;
      }

      if (method === "DELETE") {
        const result = await this.#raw(target, { method });

        return result.status === 404
          ? protocolError("STREAM_NOT_FOUND", "Awareness stream not found", 404)
          : result;
      }

      const query = new URLSearchParams(url.searchParams);
      query.delete("awareness");

      if (query.get("live") === "true") query.set("live", "sse");

      return this.#forward(request, target, `?${query.toString()}`);
    }

    const offset = url.searchParams.get("offset");

    if (method === "GET" && offset === "snapshot") {
      if ((await this.#raw(updates, { method: "HEAD" })).status === 404) return missingDocument();
      await this.#fold(id);
      const latest = (await this.#entries(`${id}/.index`)).at(-1)?.snapshotOffset;
      url.searchParams.set("offset", latest ? `${latest}_snapshot` : "-1");

      return new Response(null, {
        status: 307,
        headers: {
          location: `${url.pathname}${url.search}`,
          "cache-control": "private, max-age=5",
        },
      });
    }

    if (method === "GET" && offset?.endsWith("_snapshot")) {
      if (!/^\d{16}_\d{16}_snapshot$/.test(offset))
        return protocolError("SNAPSHOT_NOT_FOUND", "Snapshot not found", 404);
      const result = await this.#raw(`${id}/.snapshots/${offset}`, {}, "?offset=-1");

      if (result.status === 404)
        return protocolError("SNAPSHOT_NOT_FOUND", "Snapshot not found", 404);

      if (!result.ok) return result;
      const base = offset.slice(0, -"_snapshot".length);
      // Upstream Yjs protocol uses the following sequence position. Streamsy advances
      // its first component for each message, so this remains before the next update.
      const next = `${base.slice(0, 17)}${String(BigInt(base.slice(17)) + 1n).padStart(16, "0")}`;

      return new Response(result.body, {
        headers: { "content-type": binary, "stream-next-offset": next },
      });
    }

    if (method === "DELETE") {
      const deleted = await this.#raw(updates, { method });

      if (deleted.status === 404) return missingDocument();

      if (!deleted.ok) return deleted;

      const paths = new Set([
        `${id}/.index`,
        `${id}/.awareness/default`,
        `${id}/.awareness-index`,
        `${id}/.awareness/.index`,
      ]);

      for (const entry of await this.#entries(`${id}/.index`))
        if (entry.snapshotOffset) paths.add(`${id}/.snapshots/${entry.snapshotOffset}_snapshot`);

      for (const registry of [".awareness-index", ".awareness/.index"])
        for (const entry of await this.#entries(`${id}/${registry}`))
          if (entry.name) paths.add(`${id}/.awareness/${entry.name}`);

      for (const path of paths) {
        const removed = await this.#raw(path, { method: "DELETE" });

        if (!removed.ok && removed.status !== 404)
          throw new Error(`Cascade delete failed: ${removed.status}`);
      }

      await this.#fold(id, true);

      return new Response(null, { status: 204 });
    }

    const query = new URLSearchParams(url.searchParams);

    if (query.get("live") === "true") query.set("live", "long-poll");

    const result = await this.#forward(
      request,
      updates,
      method === "GET" ? `?${query.toString()}` : "",
      body,
    );

    if (result.ok && method === "PUT") await this.#createAwareness(id, "default");

    if (result.ok && (method === "POST" || (method === "PUT" && body?.length)))
      await this.#fold(id);

    return result;
  }
}

/** Match the pinned upstream path grammar, including encoded traversal rejection. */
export const parseDocumentRoute = (pathname: string): DocumentRoute | null => {
  const match = /^\/v1\/yjs\/([^/]+)\/docs\/(.+)$/.exec(pathname);

  if (!match?.[1] || !match[2]) return null;
  let path: string;

  try {
    path = decodeURIComponent(match[2]).replace(/\/+/g, "/");
  } catch {
    return null;
  }

  if (
    !/^[a-zA-Z0-9_\-/]+$/.test(path) ||
    path.length > 256 ||
    Array.some(path.split("/"), (segment) => segment === "." || segment === "..")
  )
    return null;

  return { service: match[1], docPath: path };
};
