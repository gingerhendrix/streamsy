/* oxlint-disable effecttsgo/async-function, effecttsgo/global-fetch -- HTTP proofs exercise the unchanged published writer and patched raw replay connection against actual SQLite Serve.layer. */
import { afterEach, describe, expect, it } from "vitest";
import { Array, Number } from "effect";
import { EventType, type StreamChunk } from "@tanstack/ai";
import { STOP_EVENT } from "../src/contract.ts";
import { answer } from "../src/model.ts";
import { boot, read } from "./support.ts";

let dispose = async () => {};

afterEach(async () => {
  await dispose();
});

const text = (chunks: readonly StreamChunk[]) =>
  Array.join(
    Array.flatMap(chunks, (chunk) =>
      chunk.type === EventType.TEXT_MESSAGE_CONTENT ? [chunk.delta] : [],
    ),
    "",
  );

const terminal = (chunks: readonly StreamChunk[]) =>
  Array.filter(chunks, (c) => c.type === EventType.RUN_FINISHED || c.type === EventType.RUN_ERROR);

describe("native TanStack source convention + published transport + SQLite", () => {
  it("natural finish, saved-offset byte continuation, full raw replay and probes use one model call", async () => {
    const app = await boot();
    dispose = app.dispose;
    const id = await app.start("resume");
    const active = await fetch(`${app.base}/api/chat/${id}/stream`);
    expect(active.status).toBe(200);
    expect(active.headers.get("location")).toBe(`/responses/${id}`);
    const current = app.replay(id);
    const first = await read(`${app.base}/responses/${id}`, "-1", 6);
    const rest = await read(`${app.base}/responses/${id}`, first.offset);
    const late = await app.replay(id);
    expect([...first.items, ...rest.items]).toEqual(late);
    expect(await current).toEqual(late);
    expect(text(late)).toBe(answer);
    expect(terminal(late)).toHaveLength(1);
    expect(terminal(late)[0]).toMatchObject({ type: EventType.RUN_FINISHED });
    expect(Array.filter(late, (c) => c.type === EventType.CUSTOM)).toHaveLength(0);
    expect(rest.closed).toBe(true);
    const stored = await app.raw(id);
    const suffix = await app.raw(id, first.offset);
    expect(`${JSON.stringify(first.items).slice(0, -1)},${suffix.body.slice(1)}`).toBe(stored.body);
    expect(stored.closed).toBe("true");
    expect((await fetch(`${app.base}/api/chat/${id}/stream`)).status).toBe(204);
    expect((await fetch(`${app.base}/api/chat/${id}/replay`)).status).toBe(200);
    expect(await app.stats()).toEqual({ calls: 1, interruptions: 0 });

    const stopped = await app.post(`/api/chat/${id}/stop`, {
      by: "late",
      reason: "Already finished",
    });

    expect(stopped.status).toBe(200);
    expect(await stopped.json()).toEqual({ alreadyClosed: true });
    expect(await app.raw(id)).toEqual(stored);
  });

  it("current, second, late and raw readers receive one identical stopped event, cancelled terminal and closure; repeated stop changes nothing", async () => {
    const app = await boot(15);
    dispose = app.dispose;
    const id = await app.start("stop");
    const a = app.replay(id);
    const b = app.replay(id);
    const prefix = await read(`${app.base}/responses/${id}`, "-1", 6);
    expect(prefix.closed).toBe(false);
    const stopped = await app.post(`/api/chat/${id}/stop`, { by: "reader-a", reason: "Enough" });
    expect(stopped.status).toBe(200);
    expect(await stopped.json()).toEqual({ alreadyClosed: false });
    // The successful reply is evidence that the wrapper, appends and durable close have completed.
    const stored = await app.raw(id);
    expect(stored.closed).toBe("true");
    const late = await app.replay(id);
    const raw = await read(`${app.base}/responses/${id}`);
    expect(await a).toEqual(late);
    expect(await b).toEqual(late);
    expect(raw.items).toEqual(late);
    expect(JSON.parse(stored.body)).toEqual(late);
    expect(text(late).length).toBeGreaterThan(0);
    expect(text(late).length).toBeLessThan(answer.length);

    const stopEvents = Array.filter(
      late,
      (c) => c.type === EventType.CUSTOM && c.name === STOP_EVENT,
    );

    expect(stopEvents).toHaveLength(1);
    expect(stopEvents[0]).toMatchObject({ value: { by: "reader-a", reason: "Enough" } });
    expect(terminal(late)).toHaveLength(1);
    expect(late.at(-1)).toMatchObject({ type: "RUN_FINISHED", outcome: { type: "cancelled" } });
    expect(late.at(-2)).toEqual(stopEvents[0]);
    expect(await app.stats()).toEqual({ calls: 1, interruptions: 1 });

    const repeats = await Promise.all(
      Array.makeBy(4, () => app.post(`/api/chat/${id}/stop`, { by: "b", reason: "Again" })),
    );

    for (const response of repeats) {
      expect(response.status).toBe(200);
      expect(await response.json()).toEqual({ alreadyClosed: true });
    }

    expect(await app.raw(id)).toEqual(stored);
    expect((await fetch(`${app.base}/api/chat/${id}/stream`)).status).toBe(204);
  });

  it("competing starts/stops and finish-stop races have one terminal decision", async () => {
    const app = await boot(0);
    dispose = app.dispose;

    for (let n = 0; n < 8; n++) {
      const id = `race-${n}`;
      expect((await app.prepare(id)).status).toBe(201);
      const input = { messages: [{ role: "user", parts: [{ type: "text", content: "Race" }] }] };

      const starts = await Promise.all([
        app.post(`/api/chat/${id}`, input),
        app.post(`/api/chat/${id}`, input),
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

      for (const stop of stops) expect(stop.status).toBe(200);
      const replayed = await app.replay(id);
      expect(terminal(replayed)).toHaveLength(1);

      const customs = Array.filter(
        replayed,
        (c) => c.type === EventType.CUSTOM && c.name === STOP_EVENT,
      );

      const last = replayed.at(-1);

      if (last?.type === EventType.RUN_FINISHED && last.outcome?.type === "cancelled")
        expect(customs).toHaveLength(1);
      else expect(customs).toHaveLength(0);
      expect((await app.raw(id)).closed).toBe("true");
    }

    expect(await app.stats()).toMatchObject({ calls: 8 });
  });

  it("validates bodies and ids, refuses unprepared/orphan starts, and never calls the model for invalid requests", async () => {
    const app = await boot();
    dispose = app.dispose;
    expect((await app.post("/api/chat/bad.id/prepare")).status).toBe(400);
    expect((await app.post("/api/chat/missing", {})).status).toBe(400);
    expect(
      (
        await fetch(`${app.base}/api/chat/missing`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: "{",
        })
      ).status,
    ).toBe(400);
    expect(
      (
        await app.post("/api/chat/missing", {
          messages: [{ role: "user", parts: [{ type: "text", content: "ok" }] }],
        })
      ).status,
    ).toBe(404);
    expect((await app.post("/api/chat/missing/stop", { by: "a", reason: "Enough" })).status).toBe(
      404,
    );
    expect((await app.post("/api/chat/missing/stop", {})).status).toBe(400);
    expect((await app.prepare("empty")).status).toBe(201);
    expect((await app.prepare("empty")).status).toBe(409);
    expect(
      (
        await app.post("/api/chat/empty", {
          messages: [{ role: "user", parts: [{ type: "text", content: " " }] }],
        })
      ).status,
    ).toBe(400);
    expect((await app.post("/api/chat/empty/stop", { by: "a", reason: "Stop" })).status).toBe(409);
    expect((await fetch(`${app.base}/api/chat/empty/stream`)).status).toBe(204);
    expect((await fetch(`${app.base}/api/chat/missing/replay`)).status).toBe(204);
    expect(await app.stats()).toEqual({ calls: 0, interruptions: 0 });
  });
});
