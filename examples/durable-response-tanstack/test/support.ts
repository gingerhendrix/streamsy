/* oxlint-disable effecttsgo/async-function, effecttsgo/global-fetch, effecttsgo/crypto-random-uuid -- Black-box harness owns real HTTP and native framework connections. */
import { Effect, ManagedRuntime, Schema } from "effect";
import { stream } from "@durable-streams/client";
import type { StreamChunk } from "@tanstack/ai";
import { server } from "../src/server.ts";
import { connection } from "../src/connection.ts";

export const boot = async (paceMs = 8, database = ":memory:") => {
  const reservation = Bun.serve({ port: 0, fetch: () => new Response() });
  const port = reservation.port;

  if (port === undefined) throw Error("No port");
  await reservation.stop(true);
  const runtime = ManagedRuntime.make(server({ port, database, paceMs }));
  await runtime.runPromise(Effect.void);
  const base = `http://127.0.0.1:${port}`;

  const post = (path: string, data: Schema.Json = {}) =>
    fetch(`${base}${path}`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(data),
    });

  const prepare = (id: string) => post(`/api/chat/${id}/prepare`);

  const start = async (id: string = crypto.randomUUID()) => {
    const prepared = await prepare(id);

    if (prepared.status !== 201) throw Error(`Prepare failed: ${prepared.status}`);

    const response = await post(`/api/chat/${id}`, {
      messages: [{ role: "user", parts: [{ type: "text", content: "Explain durable streams" }] }],
    });

    if (response.status !== 201)
      throw Error(`Start failed: ${response.status} ${await response.text()}`);

    return id;
  };

  const native = (id: string) => connection(id, base);
  const replay = (id: string) => Array.fromAsync<StreamChunk>(native(id).subscribe());

  const raw = async (id: string, offset = "-1") => {
    const response = await fetch(`${base}/responses/${id}?offset=${encodeURIComponent(offset)}`);

    return {
      status: response.status,
      body: await response.text(),
      offset: response.headers.get("stream-next-offset"),
      closed: response.headers.get("stream-closed"),
    };
  };

  const stats = () => fetch(`${base}/debug/model-calls`).then((r) => r.json());

  return {
    base,
    post,
    prepare,
    start,
    native,
    replay,
    raw,
    stats,
    dispose: () => runtime.dispose(),
  };
};

export const read = async (url: string, offset = "-1", take = Infinity) => {
  const response = await stream<StreamChunk>({
    url,
    offset,
    live: true,
    json: true,
    warnOnHttp: false,
  });

  const items: StreamChunk[] = [];
  let next = offset;
  let closed = false;
  const done = Promise.withResolvers<void>();
  response.subscribeJson<StreamChunk>((batch) => {
    items.push(...batch.items);
    next = batch.offset;
    closed = batch.streamClosed;

    if (items.length >= take) {
      response.cancel();
      done.resolve();
    }

    if (closed) done.resolve();
  });
  void response.closed.catch(done.reject);
  await done.promise;

  return { items, offset: next, closed };
};
