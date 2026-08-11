/** Test-only Promise facade over the Effect-native recovered State path. */
import type { JsonValue, StreamBatch } from "@streamsy/core";
import { AppendStreamsLive, ReadStreamsLive } from "@streamsy/experimental/effect";
import {
  catchUpState,
  DerivedRecoveryLive,
  DerivedStateHistoryLive,
  type CatchUpLimits,
  type ProducerLane,
  type ProjectionBoundary,
} from "@streamsy/experimental/ivm-mesh";
import type { StreamBinding } from "@streamsy/experimental/binding";
import { Layer, ManagedRuntime } from "effect";

interface Fold<State> {
  initial(): State;
  apply(state: State, fact: JsonValue): State;
}

interface Options<Input, State> {
  readonly source: StreamBinding;
  readonly target: StreamBinding;
  readonly lane: ProducerLane;
  readonly limits: CatchUpLimits;
  readonly fold: Fold<State>;
  readonly validateRecovered?: (checkpoint: {
    readonly sourceThrough?: string;
    readonly materialized: State;
  }) => void | Promise<void>;
  readonly decode: (batch: StreamBatch, boundary: ProjectionBoundary) => Iterable<Input>;
  readonly reduce: (
    input: readonly Input[],
    boundary: ProjectionBoundary,
    state: State,
  ) => readonly JsonValue[];
}

const layer = Layer.mergeAll(DerivedRecoveryLive, DerivedStateHistoryLive).pipe(
  Layer.provideMerge(Layer.mergeAll(ReadStreamsLive, AppendStreamsLive)),
);

export async function catchUp<Input, State>(options: Options<Input, State>) {
  const runtime = ManagedRuntime.make(layer);
  try {
    const result = await runtime.runPromise(
      catchUpState<State, Input>({
        source: options.source,
        target: options.target,
        lane: options.lane,
        limits: options.limits,
        initial: options.fold.initial(),
        restore: (initial, facts) =>
          facts.reduce((state, fact) => options.fold.apply(state, fact), initial),
        validateRecovered: ({ state, ...checkpoint }) =>
          options.validateRecovered?.({ ...checkpoint, materialized: state }),
        decode: options.decode,
        step: (state, input, boundary) => ({ facts: options.reduce(input, boundary, state) }),
      }),
    );
    return "checkpoint" in result
      ? {
          ...result,
          checkpoint: { ...result.checkpoint, materialized: result.state },
        }
      : result;
  } finally {
    await runtime.dispose();
  }
}
