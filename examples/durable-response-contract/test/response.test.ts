import { describe, expect, it } from "vitest";
import { Response } from "effect/ai";
import { Schema } from "effect";
import {
  applyBatch,
  emptySnapshot,
  encodeEntry,
  finish,
  readerIdentity,
  resumeKey,
  resumeState,
  saveResume,
} from "../index.ts";

const delta = (text: string) =>
  encodeEntry(Response.makePart("text-delta", { id: "answer", delta: text }));

const store = () => {
  const values = new Map<string, string>();

  return {
    getItem: (key: string) => values.get(key) ?? null,
    setItem: (key: string, value: string) => {
      values.set(key, value);
    },
  };
};

describe("shared fold and session resume", () => {
  it("persists offset/text atomically and resumes without replay duplication", () => {
    const storage = store();

    const first = applyBatch(emptySnapshot, {
      items: [delta("Hello ")],
      offset: "001",
      closed: false,
    });

    saveResume(storage, "one", first);
    const restored = resumeState(storage, "one");
    expect(restored).toEqual(first);

    const final = applyBatch(restored, {
      items: [delta("world"), encodeEntry(finish())],
      offset: "002",
      closed: true,
    });

    expect(final.view).toMatchObject({
      text: "Hello world",
      entries: 3,
      status: "Finished",
      terminal: true,
    });
    expect(applyBatch(first, { items: [delta("Hello ")], offset: "001", closed: false })).toEqual(
      first,
    );
    expect(applyBatch(final, { items: [], offset: "002", closed: true })).toEqual(final);
    expect(resumeState(storage, "other")).toEqual(emptySnapshot);
  });
  it("preserves one stopped status, partial text, and closure for late readers", () => {
    const stopped = finish({ by: "reader-a", reason: "Enough", at: "2026-11-17T12:00:00Z" });

    const state = applyBatch(emptySnapshot, {
      items: [delta("Partial"), encodeEntry(stopped)],
      offset: "end",
      closed: true,
    });

    expect(state.view.status).toBe("Stopped by reader-a at 2026-11-17T12:00:00Z: Enough");
    expect(state.view.text).toBe("Partial");
    expect(applyBatch(state, { items: [delta("wrong")], offset: "later", closed: true })).toEqual(
      state,
    );
  });
  it("surfaces error records and missing terminal closure", () => {
    expect(
      applyBatch(emptySnapshot, {
        items: [encodeEntry(Response.makePart("error", { error: "evicted" }))],
        offset: "end",
        closed: true,
      }).view.status,
    ).toBe("Error: evicted");
    expect(
      applyBatch(emptySnapshot, { items: [], offset: "end", closed: true }).view.status,
    ).toContain("without a terminal");
  });
  it("does not advance on invalid native parts; JSON omits native optional fields", () => {
    expect(() =>
      applyBatch(emptySnapshot, { items: [{ type: "text-delta" }], offset: "bad", closed: false }),
    ).toThrow();
    const native = encodeEntry(finish());
    expect(JSON.stringify(native)).not.toContain("null");
    expect(Schema.decodeUnknownSync(Response.StreamPart())(native).type).toBe("finish");
  });
  it("recovers from corrupt, stale, or unavailable storage", () => {
    const storage = store();

    for (const value of ["bad json", "{}", '{"offset":"001"}', '{"offset":1,"view":{}}']) {
      storage.setItem(resumeKey("one"), value);
      expect(resumeState(storage, "one")).toEqual(emptySnapshot);
    }

    const broken = {
      getItem: () => {
        throw Error("denied");
      },
      setItem: () => {
        throw Error("quota");
      },
    };

    expect(resumeState(broken, "one")).toEqual(emptySnapshot);
    expect(() => saveResume(broken, "one", emptySnapshot)).not.toThrow();
    expect(readerIdentity(storage)).toBe(readerIdentity(storage));
  });
});
