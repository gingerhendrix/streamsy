/* oxlint-disable effecttsgo/async-function, effecttsgo/global-fetch, effecttsgo/new-promise, effecttsgo/crypto-random-uuid -- Black-box HTTP proofs use the native client/fetch and await real paced network records. */
import { afterEach, describe, expect, it } from "vitest";
import { Array, Effect, ManagedRuntime, Schema, Stream } from "effect";
import { stream } from "@durable-streams/client";
import {
  ResponseEntry,
  finish,
  foldResponse,
  emptyView,
  streamReader,
} from "@streamsy/durable-response-contract";
import { server } from "../src/server.ts";
import { chunks } from "../src/model.ts";

let dispose = async () => {};

afterEach(async () => {
  await dispose();
});

const boot = async (paceMs = 30) => {
  const reservation = Bun.serve({ port: 0, fetch: () => new Response() });
  const port = reservation.port;

  if (port === undefined) throw Error("Missing reserved port");
  await reservation.stop(true);
  const runtime = ManagedRuntime.make(server({ database: ":memory:", port, paceMs }));
  dispose = () => runtime.dispose();
  await runtime.runPromise(Effect.void);
  const base = `http://127.0.0.1:${port}`;

  const post = (path: string, body: Schema.Json) =>
    fetch(`${base}${path}`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });

  const start = async (chatId: string = crypto.randomUUID()) => {
    const response = await post("/chat", { prompt: "Explain durable streams", chatId });
    expect(response.status).toBe(201);

    const created = Schema.decodeUnknownSync(Schema.Struct({ id: Schema.String }))(
      await response.json(),
    );

    expect(response.headers.get("location")).toBe(`/responses/${created.id}`);

    return created.id;
  };

  return { base, post, start };
};

const read = async (base: string, id: string, offset = "-1", take = Infinity) => {
  const response = await stream({
    url: `${base}/responses/${id}`,
    offset,
    live: true,
    json: true,
    warnOnHttp: false,
  });

  const parts: unknown[] = [];
  let next = offset;
  let closed = false;
  const done = Promise.withResolvers<void>();

  response.subscribeJson((batch) => {
    parts.push(...batch.items);
    next = batch.offset;
    closed = batch.streamClosed;

    if (parts.length >= take) {
      response.cancel();
      done.resolve();
    }

    if (closed) done.resolve();
  });
  // Network closure can precede body consumption; wait for the decoded batch.
  void response.closed.catch(done.reject);
  await done.promise;

  return { parts, offset: next, closed };
};

const raw = async (base: string, id: string, offset = "-1") => {
  const response = await fetch(`${base}/responses/${id}?offset=${encodeURIComponent(offset)}`);
  expect(response.status).toBe(200);

  return {
    body: await response.text(),
    offset: response.headers.get("stream-next-offset"),
    closed: response.headers.get("stream-closed"),
  };
};

const parts = (values: readonly unknown[]) =>
  Array.map(values, (part) => Schema.decodeUnknownSync(ResponseEntry)(part));

describe("SQLite Bun HTTP durable response", () => {
  it("drops after three paced records and resumes identical encoded bytes with one model call", async () => {
    const { base, start, post } = await boot();
    const id = await start();
    const first = await read(base, id, "-1", 3);
    expect(first.parts).toHaveLength(3);
    expect(first.closed).toBe(false);
    const rest = await read(base, id, first.offset);
    expect(rest.closed).toBe(true);
    const second = await read(base, id);
    expect([...first.parts, ...rest.parts]).toEqual(second.parts);

    // Exercise the shared page adapter against an already-closed response as well.
    const replay = await Effect.runPromise(
      streamReader(id, first.offset, base).pipe(Stream.runCollect, Effect.timeout("2 seconds")),
    );

    expect(Array.flatMap(replay, (batch) => batch.items)).toEqual(rest.parts);
    expect(replay.at(-1)?.closed).toBe(true);
    const full = await raw(base, id);
    const suffix = await raw(base, id, first.offset);
    const prefixBytes = JSON.stringify(first.parts).slice(0, -1);
    expect(new TextEncoder().encode(`${prefixBytes},${suffix.body.slice(1)}`)).toEqual(
      new TextEncoder().encode(full.body),
    );
    const view = Array.reduce(parts(second.parts), emptyView, foldResponse);
    expect(view.text).toBe(Array.join(chunks, ""));
    expect(view.status).toBe("Finished");
    expect(await fetch(`${base}/debug/model-calls`).then((r) => r.json())).toEqual({
      calls: 1,
      interruptions: 0,
    });
    const closed = await read(base, id, rest.offset);
    expect(closed).toEqual({ parts: [], offset: rest.offset, closed: true });

    // A natural finish winning the stop race is an idempotent success too.
    const afterFinish = await post(`/responses/${id}/stop`, {
      by: "late-stop",
      reason: "Already finished",
    });

    expect(afterFinish.status).toBe(200);
    expect(await afterFinish.json()).toEqual({ alreadyClosed: true });
    expect(await raw(base, id)).toEqual(full);
  });

  it("two readers and a late reader see the same stopped record; provider interruption and repeated stop", async () => {
    const { base, post, start } = await boot();
    const id = await start("shared-chat");
    const probe = await fetch(`${base}/chat/shared-chat/stream`);
    expect(probe.status).toBe(200);
    expect(probe.headers.get("location")).toBe(`/responses/${id}`);
    const other = read(base, id);
    const early = await read(base, id, "-1", 3);
    const stopped = await post(`/responses/${id}/stop`, { by: "reader-a", reason: "Enough" });
    expect(stopped.status).toBe(200);
    expect(await stopped.json()).toEqual({ alreadyClosed: false });
    const resumed = await read(base, id, early.offset);
    const readerB = await other;
    const late = await read(base, id);
    expect([...early.parts, ...resumed.parts]).toEqual(readerB.parts);
    expect(late).toEqual(readerB);
    const terminal = Array.filter(parts(late.parts), (p) => p.type === "finish");
    expect(terminal).toHaveLength(1);
    expect(terminal[0]?.metadata.stopped).toMatchObject({ by: "reader-a", reason: "Enough" });
    expect(late.closed).toBe(true);
    expect(await fetch(`${base}/debug/model-calls`).then((r) => r.json())).toEqual({
      calls: 1,
      interruptions: 1,
    });
    const before = await raw(base, id);

    const repeated = await Promise.all(
      Array.makeBy(4, () => post(`/responses/${id}/stop`, { by: "reader-b", reason: "Again" })),
    );

    for (const response of repeated) {
      expect(response.status).toBe(200);
      expect(await response.json()).toEqual({ alreadyClosed: true });
    }

    expect(await raw(base, id)).toEqual(before);
    expect((await fetch(`${base}/chat/shared-chat/stream`)).status).toBe(204);
  });

  it("finish and competing stops have one terminal winner", async () => {
    const { base, post, start } = await boot(0);

    for (let i = 0; i < 12; i++) {
      const id = await start();

      const attempts = await Promise.all([
        post(`/responses/${id}/stop`, { by: "a", reason: "Race" }),
        post(`/responses/${id}/stop`, { by: "b", reason: "Race" }),
      ]);

      const result = await read(base, id);
      expect(result.closed).toBe(true);
      expect(
        Array.filter(parts(result.parts), (p) => p.type === "finish" || p.type === "error"),
      ).toHaveLength(1);

      for (const response of attempts) expect(response.status).toBe(200);
      const last = parts(result.parts).at(-1);
      expect(last?.type).toBe("finish");
    }

    // The same native schema decodes the chosen stop representation.
    expect(
      Schema.decodeSync(ResponseEntry)(
        Schema.encodeSync(ResponseEntry)(finish({ by: "a", reason: "Race", at: "now" })),
      ).type,
    ).toBe("finish");
  });

  it("rejects invalid bodies/ids and missing responses without calling the model", async () => {
    const { base, post } = await boot();

    const invalidBodies: ReadonlyArray<Schema.Json> = [
      {},
      { prompt: " ", chatId: "chat" },
      { prompt: "ok", chatId: "bad.id" },
    ];

    for (const body of invalidBodies) expect((await post("/chat", body)).status).toBe(400);
    expect((await post("/responses/bad.id/stop", { by: "a", reason: "stop" })).status).toBe(400);
    expect((await post("/responses/missing/stop", { by: "a", reason: "stop" })).status).toBe(404);
    expect((await post("/responses/missing/stop", {})).status).toBe(400);
    expect((await fetch(`${base}/responses/missing?offset=-1`)).status).toBe(404);
    expect((await fetch(`${base}/responses/bad.id?offset=-1`)).status).toBe(400);
    expect((await fetch(`${base}/chat/idle/stream`)).status).toBe(204);
    expect(await fetch(`${base}/debug/model-calls`).then((r) => r.json())).toEqual({
      calls: 0,
      interruptions: 0,
    });
  });
});
