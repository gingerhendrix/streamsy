import { Streams } from "@streamsy/core";
import { Projection } from "@streamsy/projection";
import { Schema } from "effect";
import { hackerNewsSource, hackerNewsTarget } from "./stream-resources.ts";
import { HackerNewsStateChange, hackerNewsState } from "../state-schema.ts";

const decodeStateChange = Schema.decodeUnknownSync(HackerNewsStateChange);

/**
 * Convert deterministic newest-set reconciliation commands into the public
 * Durable State vocabulary consumed by createStreamDB in the browser.
 *
 * Fused form: every target append commits with the checkpoint in the one memory
 * owner transaction, so a restart never repeats output. Fact headers keep the
 * source position: `offset` is the unit's accepted source offset and `txid`
 * places the fact within that unit.
 */
export const hackerNewsStoryIndex = Projection.make({
  id: "hacker-news-newest-story-index",
  generation: 1,
  input: hackerNewsSource,
  process: Projection.each((entry, unit) => {
    const offset = unit.ranges.input.nextOffset;
    const headers = { offset, txid: `${offset}:${entry.index}` };
    const value = entry.item;
    const change =
      value.operation === "delete"
        ? hackerNewsState.stories.delete({ key: value.key, oldValue: value.oldValue, headers })
        : hackerNewsState.stories.upsert({ value: value.story, headers });
    return Streams.append(hackerNewsTarget, [decodeStateChange(change)]);
  }),
});
