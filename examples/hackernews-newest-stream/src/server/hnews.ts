import { Array, Effect, Layer, Option, Schema } from "effect";
import { HttpClient, HttpClientResponse } from "effect/http";
import { HackerNewsItem, HackerNewsStory, newestStorySort } from "../state-schema.ts";
import { DemoConfig } from "./config.ts";
import { HackerNewsApi, pollFailure } from "./poller/contract.ts";
import { errorMessage } from "./util.ts";

const NewestStoryIds = Schema.Array(Schema.Finite);

/**
 * Keep live stories only. Decoding drops the item fields that a story does not have.
 *
 * HN can serve a new story without `title` or `time` for its first minutes.
 * Such a story is not complete yet. It is skipped at debug level, and a later
 * poll fetches it again.
 */
const toStory = (item: typeof HackerNewsItem.Type) => {
  if (item === null || item.deleted || item.dead || item.type !== "story") {
    return Effect.succeedNone;
  }

  if (item.title === undefined || item.time === undefined) {
    return Effect.logDebug(`HN story ${item.id} is not complete yet`).pipe(
      Effect.as(Option.none()),
    );
  }

  return Effect.asSome(Schema.decodeUnknownEffect(HackerNewsStory)(item));
};

/**
 * The HN Firebase API at `HN_API_BASE` as named effects. A failed or skipped
 * item is logged and does not stall the batch, so a later poll can see it again.
 */
export const hackerNewsApiLayer = Layer.effect(
  HackerNewsApi,
  Effect.gen(function* () {
    const { hnApiBase: apiBase } = yield* DemoConfig;
    const client = HttpClient.filterStatusOk(yield* HttpClient.HttpClient);

    const getJson = <S extends Schema.Top>(path: string, schema: S) =>
      client
        .get(`${apiBase}${path}`)
        .pipe(Effect.flatMap(HttpClientResponse.schemaBodyJson(schema)));

    const fetchStory = (id: number) =>
      getJson(`/item/${id}.json`, HackerNewsItem).pipe(
        Effect.flatMap(toStory),
        Effect.catch((error) =>
          Effect.logWarning(`Unable to fetch HN story ${id}: ${errorMessage(error)}`).pipe(
            Effect.as(Option.none()),
          ),
        ),
      );

    return HackerNewsApi.of({
      fetchNewestStoryIds: Effect.fn("HackerNewsApi.fetchNewestStoryIds")(function* (
        limit: number,
      ) {
        const ids = yield* getJson("/newstories.json", NewestStoryIds).pipe(
          Effect.mapError(pollFailure("fetchNewestStoryIds")),
        );

        return ids.slice(0, limit);
      }),
      fetchStoriesById: Effect.fn("HackerNewsApi.fetchStoriesById")(function* (
        ids: readonly number[],
      ) {
        const stories = yield* Effect.forEach(ids, fetchStory, { concurrency: "unbounded" });

        return Array.getSomes(stories).toSorted(newestStorySort);
      }),
    });
  }),
);
