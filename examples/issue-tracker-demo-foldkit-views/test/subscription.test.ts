import { Effect } from "effect";
import { afterEach, describe, expect, test, vi } from "vitest";
import { workspaceFromSearch, workspaceHref } from "../src/command.ts";
import { trackedFetch } from "../src/subscription.ts";

const reader = () => {
  const reports: boolean[] = [];

  return { reports, fetch: trackedFetch((isOnline) => reports.push(isOnline)) };
};

const answer = (status: number) => Promise.resolve(new Response(null, { status }));

const read = (fetch: typeof globalThis.fetch, init?: RequestInit) =>
  Effect.tryPromise({
    try: () => fetch("/streams/workspace/main", init),
    catch: (error) => String(error),
  });

describe("trackedFetch", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  test.each([500, 502, 503, 504])(
    "an unavailable %i is offline, and the next answer is online",
    (status) =>
      Effect.runPromise(
        Effect.gen(function* () {
          const { reports, fetch } = reader();

          vi.stubGlobal("fetch", () => answer(status));
          yield* read(fetch);
          yield* read(fetch);
          expect(reports).toEqual([false]);

          vi.stubGlobal("fetch", () => answer(200));
          yield* read(fetch);
          expect(reports).toEqual([false, true]);
        }),
      ),
  );

  test("any other HTTP status means the server answered", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const { reports, fetch } = reader();

        vi.stubGlobal("fetch", () => answer(404));
        yield* read(fetch);
        expect(reports).toEqual([]);
      }),
    ));

  test("a rejected fetch is offline, unless the reader aborted it", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const { reports, fetch } = reader();
        const failed = new TypeError("Failed to fetch");

        vi.stubGlobal("fetch", () => Promise.reject(failed));
        const aborted = yield* Effect.flip(read(fetch, { signal: AbortSignal.abort() }));
        expect(aborted).toBe(String(failed));
        expect(reports).toEqual([]);

        const rejected = yield* Effect.flip(read(fetch));
        expect(rejected).toBe(String(failed));
        expect(reports).toEqual([false]);
      }),
    ));
});

describe("workspace links", () => {
  test("?w= selects a workspace; no query is main", () => {
    expect(workspaceFromSearch("?w=team-1")).toBe("team-1");
    expect(workspaceFromSearch("?w=%20")).toBe("main");
    expect(workspaceFromSearch("")).toBe("main");
    expect(workspaceHref("main")).toBe("?");
    expect(workspaceHref("team-1")).toBe("?w=team-1");
  });
});
