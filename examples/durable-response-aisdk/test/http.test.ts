/* oxlint-disable effecttsgo/async-function, effecttsgo/global-fetch, effecttsgo/new-promise -- Integration proofs exercise the unchanged published transport against real paced HTTP. */
import { afterEach, describe, expect, it } from "vitest";
import { Array, Number } from "effect";
import { stream } from "@durable-streams/client";
import type { UIMessage } from "ai";
import { Chat } from "@ai-sdk/react";
import { chunks } from "../src/model.ts";
import { boot } from "./support.ts";

let dispose = async () => {};

afterEach(async () => {
  await dispose();
});

const read = async (url: string, offset = "-1", take = Infinity) => {
  const response = await stream({ url, offset, live: true, json: true, warnOnHttp: false });
  const items: object[] = [];
  let next = offset;
  let closed = false;
  const done = Promise.withResolvers<void>();
  response.subscribeJson<object>((batch) => {
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

const nativeReplay = async (app: Awaited<ReturnType<typeof boot>>, id: string) => {
  const chat = new Chat({ id, transport: app.transport(id) });
  await chat.resumeStream();

  return chat;
};

const text = (chat: Chat<UIMessage>) =>
  Array.join(
    Array.flatMap(chat.messages, (m) =>
      Array.flatMap(m.parts, (p) => (p.type === "text" ? [p.text] : [])),
    ),
    "",
  );

describe("published AI SDK transport + SQLite Serve.layer", () => {
  it("resumes at a saved protocol offset and native replay returns one full message with one model call", async () => {
    const app = await boot();
    dispose = app.dispose;
    const id = await app.start("resume");
    expect((await fetch(`${app.base}/api/chat/${id}/stream`)).status).toBe(200);
    const url = `${app.base}/responses/${id}`;
    const first = await read(url, "-1", 5);
    expect(first.items).toHaveLength(5);
    const rest = await read(url, first.offset);
    const late = await read(url);
    expect([...first.items, ...rest.items]).toEqual(late.items);
    expect(rest.offset).toBe(late.offset);
    expect(rest.closed).toBe(true);
    const chat = await nativeReplay(app, id);
    expect(text(chat)).toBe(Array.join(chunks, ""));
    expect(chat.messages).toHaveLength(1);
    expect(chat.messages[0]?.metadata).toMatchObject({ prompt: "Explain durable streams" });
    expect((await fetch(`${app.base}/api/chat/${id}/stream`)).status).toBe(204);
    expect((await fetch(`${app.base}/api/chat/${id}/replay`)).status).toBe(200);
    expect(await fetch(`${app.base}/debug/model-calls`).then((r) => r.json())).toEqual({
      calls: 1,
      interruptions: 0,
    });
    expect(await read(url, late.offset)).toEqual({ items: [], offset: late.offset, closed: true });

    const finishedStop = await app.post(`/api/chat/${id}/stop`, {
      by: "late",
      reason: "After finish",
    });

    expect(finishedStop.status).toBe(200);
    expect(await finishedStop.json()).toEqual({ alreadyClosed: true });
    const stored = await app.raw(id);
    const suffix = await app.raw(id, first.offset);
    expect(`${JSON.stringify(first.items).slice(0, -1)},${suffix.body.slice(1)}`).toBe(stored.body);
  });

  it("two native readers and late replay see identical stopped metadata, partial text and closure; double stop is a no-op", async () => {
    const app = await boot(35);
    dispose = app.dispose;
    const id = await app.start("stop");
    const readerA = new Chat({ id, transport: app.transport(id) });
    const readerB = new Chat({ id, transport: app.transport(id) });
    const a = readerA.resumeStream();
    const b = readerB.resumeStream();
    const prefix = await read(`${app.base}/responses/${id}`, "-1", 6);
    expect(prefix.closed).toBe(false);
    const response = await app.post(`/api/chat/${id}/stop`, { by: "reader-a", reason: "Enough" });
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({ alreadyClosed: false });
    await Promise.all([a, b]);
    const late = await nativeReplay(app, id);
    expect(readerA.messages).toEqual(readerB.messages);
    expect(late.messages).toEqual(readerA.messages);
    expect(text(late).length).toBeGreaterThan(0);
    expect(text(late).length).toBeLessThan(Array.join(chunks, "").length);
    expect(late.messages[0]?.metadata).toMatchObject({
      stopped: { by: "reader-a", reason: "Enough" },
    });
    const stored = await app.raw(id);
    expect(stored.closed).toBe("true");
    expect(await fetch(`${app.base}/debug/model-calls`).then((r) => r.json())).toEqual({
      calls: 1,
      interruptions: 1,
    });

    const repeated = await Promise.all([
      app.post(`/api/chat/${id}/stop`, { by: "b", reason: "Again" }),
      app.post(`/api/chat/${id}/stop`, { by: "c", reason: "Again" }),
    ]);

    for (const r of repeated) {
      expect(r.status).toBe(200);
      expect(await r.json()).toEqual({ alreadyClosed: true });
    }

    expect(await app.raw(id)).toEqual(stored);
    expect((await fetch(`${app.base}/api/chat/${id}/stream`)).status).toBe(204);
  });

  it("one start and terminal decision win concurrent starts/stops and natural finish races", async () => {
    const app = await boot(0);
    dispose = app.dispose;

    for (let i = 0; i < 5; i++) {
      const id = `race-${i}`;
      const request = { id, messages: [{ role: "user", parts: [{ type: "text", text: "Race" }] }] };

      const starts = await Promise.all([
        app.post("/api/chat", request),
        app.post("/api/chat", request),
      ]);

      expect(
        Array.sort(
          Array.map(starts, (r) => r.status),
          Number.Order,
        ),
      ).toEqual([201, 409]);

      const stops = await Promise.all([
        app.post(`/api/chat/${id}/stop`, { by: "a", reason: "Race" }),
        app.post(`/api/chat/${id}/stop`, { by: "b", reason: "Race" }),
      ]);

      for (const r of stops) expect(r.status).toBe(200);
      const final = await read(`${app.base}/responses/${id}`);
      expect(final.closed).toBe(true);

      const terminals = Array.filter(
        final.items,
        (p) => "type" in p && (p.type === "finish" || p.type === "abort"),
      );

      expect(terminals).toHaveLength(1);
    }

    expect(await fetch(`${app.base}/debug/model-calls`).then((r) => r.json())).toMatchObject({
      calls: 5,
    });
  });

  it("rejects malformed inputs and encoded slashes", async () => {
    const app = await boot();
    dispose = app.dispose;
    expect((await app.post("/api/chat", {})).status).toBe(400);
    expect(
      (
        await fetch(`${app.base}/api/chat`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: "{",
        })
      ).status,
    ).toBe(400);
    expect((await app.post("/api/chat", { id: "bad.id", messages: [] })).status).toBe(400);
    expect((await app.post("/api/chat/missing/stop", {})).status).toBe(400);
    expect((await app.post("/api/chat/missing/stop", { by: "a", reason: "stop" })).status).toBe(
      404,
    );
    expect((await fetch(`${app.base}/api/chat/idle/stream`)).status).toBe(204);
    expect(
      (
        await fetch(`${app.base}/streams/responses%2Fencoded`, {
          method: "PUT",
          headers: { "content-type": "application/json" },
        })
      ).status,
    ).toBe(404);
    expect(await fetch(`${app.base}/debug/model-calls`).then((r) => r.json())).toEqual({
      calls: 0,
      interruptions: 0,
    });
  });
});
