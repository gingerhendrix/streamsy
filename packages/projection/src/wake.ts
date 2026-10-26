/* oxlint-disable typescript/no-explicit-any, typescript/no-unsafe-type-assertion, anti-slop/require-safety-comment-for-type-assertion, anti-slop/no-chained-type-assertions, effecttsgo/any-unknown-in-error-context -- The public signature checks the input, output and params; this module binds them through the erased kernel boundary, as the other constructors do. */
import { Schema } from "effect";
import { DurableState, Offset, StreamId, Streamsy } from "@streamsy/core";
import type { StreamRoute } from "@streamsy/core/protocol";
import { CheckpointStore } from "./checkpoint-store.ts";
import * as Commit from "./commit.ts";
import {
  build,
  isDeclaration,
  type AccumulatingDeclaration,
  type DecodedParams,
  type Input,
  type OneWithin,
  type StreamDeclaration,
} from "./declaration.ts";

/** A State declaration with the wake collections. */
export type WakeOutput = StreamDeclaration & StreamRoute.StateRoute<any, Streamsy.WakeCollections>;

export interface WakeDefinition<C extends StreamRoute.ParamCodecs, I extends Input, O> {
  readonly id: string;
  readonly version?: number;
  readonly generation?: number;
  readonly params: C;
  /** The one source stream. Its params must be projection params. */
  readonly input: I & OneWithin<C, I>;
  /** The State declaration with wake collections that gets the row. Its params must be projection params. */
  readonly output: O & OneWithin<C, O>;
}

export type WakeDeclaration<
  C extends StreamRoute.ParamCodecs,
  I extends Input,
  O extends Input,
> = AccumulatingDeclaration<
  C,
  { readonly source: I },
  { readonly wake: O },
  null,
  never,
  never,
  never,
  CheckpointStore
>;

type Change = DurableState.Change<Streamsy.WakeCollections>;

const row = (stream: StreamId, offset: string, closed: boolean): Change =>
  DurableState.upsert(
    "stream",
    closed
      ? { stream, offset: Offset.make(offset), closed: true as const }
      : { stream, offset: Offset.make(offset) },
  );

/**
 * A wake projection: one source stream gives one keyed row in one wake stream.
 * Each unit appends one upsert `{ stream, offset, closed? }` with
 * `Commit.producer`, so several source writes in one unit fold into one row,
 * and a retry answers `Duplicate`. The producer creates a missing wake stream
 * on its own owner. A wake stream on another owner must exist before the first
 * append: its owner creates it at start. When the source is removed or
 * expires, the binding appends a State delete for its row and forgets itself.
 *
 * ```ts
 * const filesWake = Projection.wake({
 *   id: "files-wake",
 *   params: { user: UserKey, folderId: FolderKey },
 *   input: Files,
 *   output: UserWake,
 * });
 * ```
 */
export function wake<
  C extends StreamRoute.ParamCodecs,
  const I extends Input,
  const O extends WakeOutput,
>(definition: WakeDefinition<C, I, O>): WakeDeclaration<C, I, O> {
  const input: Input = definition.input;

  return build("mapAccumBatch", {
    id: definition.id,
    version: definition.version,
    generation: definition.generation,
    params: definition.params,
    commit: Commit.producer as Commit.Commit<any, any, any>,
    inputs: { source: input },
    outputs: { wake: definition.output },
    state: { schema: Schema.Null, initial: null },
    mapAccumBatch: (
      _state: null,
      batch: {
        readonly source: {
          readonly nextOffset: string;
          readonly closed: boolean;
          readonly gone?: boolean;
        };
      },
      unit: { readonly params: DecodedParams },
    ) => {
      const source = isDeclaration(input) ? input.at(unit.params) : input;
      const slice = batch.source;

      return [
        null,
        {
          wake: [
            slice.gone
              ? DurableState.remove("stream", StreamId.make(source.id))
              : row(StreamId.make(source.id), slice.nextOffset, slice.closed),
          ],
        },
      ] as const;
    },
  }) as unknown as WakeDeclaration<C, I, O>;
}
