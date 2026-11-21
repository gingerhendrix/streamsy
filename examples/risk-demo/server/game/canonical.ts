import { ZERO_OFFSET } from "@streamsy/core";
import { Projection } from "@streamsy/projection";
import { Effect, Schema } from "effect";
import { CommandBatch, GameEvents } from "./streams.ts";

const RecordedCommand = Schema.Struct({ batch: CommandBatch, offset: Schema.String });
const History = Schema.Array(RecordedCommand);

// `current` folds the canonical stream without writing a checkpoint. Using the
// per-item position keeps command acknowledgements and action cursors independent
// of the read page size. The local owners provide those positions.
const history = Projection.fold({
  id: "hex-domination-command-history",
  params: { gameId: Schema.String },
  inputs: { events: GameEvents },
  state: { schema: History, initial: [] },
  fold: (state, { item, offset }) =>
    offset === undefined
      ? Effect.die(new Error("Canonical command replay requires local item offsets"))
      : Effect.succeed([...state, { batch: item, offset }]),
});

export const readHistory = (gameId: string) =>
  Projection.current(history.at({ gameId })).pipe(
    Effect.map(({ state, offsets }) => ({ commands: state, head: offsets.events || ZERO_OFFSET })),
  );
