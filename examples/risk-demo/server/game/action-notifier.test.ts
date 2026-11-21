/* oxlint-disable effecttsgo/async-function -- Vitest owns the Promise boundaries. */
import { AppendResult } from "@streamsy/core";
import { Effect } from "effect";
import { expect, it } from "vitest";
import { reconcilePlayerActions, type AgentMessage } from "./action-notifier.ts";
import { PlayerActions, memoryStreams } from "./streams.ts";

it.each([false, true])(
  "verifies the payload after a real duplicate producer result (matching: %s)",
  async (matching) => {
    const streams = memoryStreams();
    const wanted: AgentMessage = {
      type: "GameOver",
      messageId: "game-over",
      seq: 1,
      gameId: "game",
      playerId: "p1",
      winner: { id: "p1", name: "Alice" },
      since: { fromEventOffset: null, events: [] },
      eventOffset: "1",
    };
    let sawDuplicate = false;
    const run = streams.runPromise(
      Effect.gen(function* () {
        const stream = yield* PlayerActions;
        const raced = PlayerActions.of({
          ...stream,
          append: (params, items, options) =>
            Effect.gen(function* () {
              // Let another writer win this exact producer tuple before the caller receives its result.
              yield* stream.append(
                params,
                [matching ? wanted : { ...wanted, winner: { id: "p2", name: "Bob" } }],
                options,
              );
              const duplicate = yield* stream.append(params, items, options);
              sawDuplicate = AppendResult.$is("Duplicate")(duplicate);
              return duplicate;
            }),
        });
        return yield* reconcilePlayerActions("game", "p1", [wanted]).pipe(
          Effect.provideService(PlayerActions, raced),
        );
      }),
    );
    try {
      if (matching) await expect(run).resolves.toBe(true);
      else await expect(run).rejects.toThrow(/different durable payload won/);
      expect(sawDuplicate).toBe(true);
    } finally {
      await streams.dispose();
    }
  },
);
