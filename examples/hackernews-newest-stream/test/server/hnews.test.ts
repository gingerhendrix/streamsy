/* oxlint-disable effecttsgo/async-function -- Vitest tests own the runtime lifetime. */
import { expect, test } from "vitest";
import { Effect, Layer, Logger, LogLevel, References } from "effect";
import { HttpClient, HttpClientResponse } from "effect/http";
import { hackerNewsApiLayer } from "../../src/server/hnews.ts";
import { HackerNewsApi } from "../../src/server/poller/contract.ts";

const complete = { id: 1, by: "a", time: 30, title: "complete", type: "story" };

/** HN served story 49987728 like this for its first minutes on 2026-10-07. */
const untitled = { id: 2, by: "b", time: 20, type: "story" };

const items = new Map([
  ["1", complete],
  ["2", untitled],
]);

/** Answers `/item/<id>.json` from `items`. An unknown id answers 500. */
const fakeHackerNews = Layer.succeed(
  HttpClient.HttpClient,
  HttpClient.make((request) => {
    const id = /\/item\/(\d+)\.json$/.exec(request.url)?.[1] ?? "";
    const body = items.get(id);

    return Effect.succeed(
      HttpClientResponse.fromWeb(
        request,
        body === undefined ? new Response("boom", { status: 500 }) : Response.json(body),
      ),
    );
  }),
);

const fetchStories = (ids: readonly number[]) => {
  const logs: Array<{ readonly level: LogLevel.LogLevel; readonly message: string }> = [];

  const logger = Logger.make((options) => {
    logs.push({ level: options.logLevel, message: [options.message].flat().map(String).join(" ") });
  });

  return Effect.gen(function* () {
    const api = yield* HackerNewsApi;

    return yield* api.fetchStoriesById(ids);
  }).pipe(
    // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Each test is its own entry point.
    Effect.provide(hackerNewsApiLayer.pipe(Layer.provide(fakeHackerNews))),
    Effect.provideService(Logger.CurrentLoggers, new Set([logger])),
    Effect.provideService(References.MinimumLogLevel, "All"),
    Effect.map((stories) => ({ stories, logs })),
    Effect.runPromise,
  );
};

test("a story without a title is skipped at debug level", async () => {
  const { stories, logs } = await fetchStories([1, 2]);

  expect(stories.map((story) => story.id)).toEqual([1]);
  expect(logs).toEqual([{ level: "Debug", message: "HN story 2 is not complete yet" }]);
});

test("a failed fetch logs one readable warning line", async () => {
  const { stories, logs } = await fetchStories([3]);

  expect(stories).toEqual([]);
  expect(logs).toHaveLength(1);
  expect(logs[0]?.level).toBe("Warn");
  expect(logs[0]?.message).toMatch(/^Unable to fetch HN story 3: /);
  expect(logs[0]?.message).not.toContain("[object");
});
