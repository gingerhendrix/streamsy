import { Effect } from "effect";
import { BoardStream, GameEvents, type GameStreams } from "../server/game/streams.ts";
import type { GameEvent } from "../src/domain/events.ts";

/** Import recorded events into the current private command-item format. */
export function appendRecordedEvents(
  streams: GameStreams,
  gameId: string,
  events: readonly GameEvent[],
) {
  return streams.runPromise(
    Effect.gen(function* () {
      const stream = yield* GameEvents;
      const params = { gameId };
      yield* stream.create(params);
      const batches: [GameEvent, ...GameEvent[]][] = [];
      for (const event of events) {
        const last = batches.at(-1);
        if (last?.[0].commandId === event.commandId) last.push(event);
        else batches.push([event]);
      }
      for (const batch of batches) yield* stream.append(params, [{ events: batch }]);
      return (yield* stream.head(params)).nextOffset;
    }),
  );
}

export function readBoardRows(streams: GameStreams, gameId: string, generation = "board1") {
  return streams.runPromise(
    Effect.flatMap(BoardStream, (stream) => stream.collect({ gameId, generation })),
  );
}
