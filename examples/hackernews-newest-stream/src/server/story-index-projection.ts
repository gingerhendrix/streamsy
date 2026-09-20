import { State } from "@streamsy/core";
import { Projection } from "@streamsy/projection";
import { Effect } from "effect";
import { hackerNewsSource, hackerNewsTarget } from "./stream-resources.ts";

/**
 * Convert deterministic newest-set reconciliation commands into the public
 * Durable State vocabulary consumed by createStreamDB in the browser.
 *
 * The declared stream output is pinned to the checkpoint before append, so a
 * restart resumes without repeating output. State.changes keeps the source
 * offset and gives each fact a stable position within the unit.
 */
export const hackerNewsStoryIndex = Projection.stream({
  id: "hn-story-index",
  generation: 1,
  input: hackerNewsSource,
  output: hackerNewsTarget,
  process: (batch, unit) =>
    Effect.sync(() =>
      State.changes(
        hackerNewsTarget,
        { offset: unit.ranges.input.nextOffset },
        Projection.items(batch).map(({ item }) =>
          item.operation === "delete"
            ? State.delete("hn-story", item.oldValue)
            : State.upsert("hn-story", item.story),
        ),
      ),
    ),
});
