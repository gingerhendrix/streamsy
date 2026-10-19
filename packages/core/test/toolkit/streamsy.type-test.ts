/** Named toolkit failures and the stream service's codec requirements. */
import { Context, Effect, Schema, type Stream } from "effect";
import type {
  AppendFailure,
  ReadFailure,
  FollowFailure,
  CreateFailure,
  HeadFailure,
  RemoveFailure,
  InvalidAppendRequest,
  InvalidEpochSeq,
  OffsetMismatch,
  StaleEpoch,
  ProducerGap,
  DecodeFault,
} from "../../src/index.ts";
import { Streamsy } from "../../src/index.ts";
import type {
  AppendResult,
  CreateResult,
  HeadResult,
  ReadNextResult,
} from "../../src/protocol/results.ts";
import type { AppendBaseError } from "../../src/protocol/errors.ts";
import type { StreamsFault, EncodeFault } from "../../src/fault.ts";
import type { Batch, Folded, Collected, Decided } from "../../src/toolkit/streams.ts";

type Equal<A, B> = [A] extends [B] ? ([B] extends [A] ? true : false) : false;

type ProducerOptions = {
  producer: { producerId: string; producerEpoch: number; producerSeq: number };
};

export const noCallerBugs: Equal<
  Extract<AppendFailure<{}>, InvalidAppendRequest | InvalidEpochSeq | OffsetMismatch>,
  never
> = true;

export const producerRejections: Equal<
  Extract<
    AppendFailure<ProducerOptions>,
    InvalidEpochSeq | OffsetMismatch | StaleEpoch | ProducerGap
  >,
  StaleEpoch | ProducerGap
> = true;

export const maybeOptions: Equal<
  AppendFailure,
  AppendBaseError | OffsetMismatch | StaleEpoch | ProducerGap | StreamsFault | EncodeFault
> = true;

class DecodeService extends Context.Service<DecodeService, {}>()("DecodeService") {}

class EncodeService extends Context.Service<EncodeService, {}>()("EncodeService") {}

class Refused extends Schema.TaggedError<Refused, { readonly brand: unique symbol }>()(
  "Refused",
  {},
) {}

declare const api: Streamsy.Api<{ group: string }, { n: number }, DecodeService, EncodeService>;

const params = { group: "events" };

const read = api.read(params);

const follow = api.follow(params);

const fold = api.fold(
  params,
  () => 0,
  (sum, item) => sum + item.n,
);

const collect = api.collect(params);

const append = api.append(params, [{ n: 1 }]);

const conditional = api.append(params, [{ n: 1 }], { expectedOffset: "tail" });

const create = api.create(params);

const fork = api.fork(params, params);

const head = api.head(params);

const remove = api.remove(params);

const session = api.session(params, { offset: "tail" });

const decide = api.decide(params, {
  initial: () => 0,
  fold: (sum, item) => sum + item.n,
  decide: (sum) =>
    sum > 0 ? Effect.fail(new Refused()) : Effect.succeed({ value: sum, items: [{ n: sum }] }),
});

export const readType: Equal<
  typeof read,
  Stream.Stream<Batch<{ n: number }>, ReadFailure, DecodeService>
> = true;

export const followType: Equal<
  typeof follow,
  Stream.Stream<Batch<{ n: number }>, FollowFailure, DecodeService>
> = true;

export const foldType: Equal<
  typeof fold,
  Effect.Effect<Folded<number>, ReadFailure, DecodeService>
> = true;

export const collectType: Equal<
  typeof collect,
  Effect.Effect<Collected<{ n: number }>, ReadFailure, DecodeService>
> = true;

export const appendType: Equal<
  typeof append,
  Effect.Effect<AppendResult, AppendFailure<{}>, EncodeService>
> = true;

export const conditionalType: Equal<
  typeof conditional,
  Effect.Effect<AppendResult, AppendFailure<{ expectedOffset: string }>, EncodeService>
> = true;

export const createType: Equal<typeof create, Effect.Effect<CreateResult, CreateFailure>> = true;

export const forkType: Equal<typeof fork, Effect.Effect<CreateResult, CreateFailure>> = true;

export const headType: Equal<typeof head, Effect.Effect<HeadResult, HeadFailure>> = true;

export const removeType: Equal<typeof remove, Effect.Effect<void, RemoveFailure>> = true;

export const sessionType: Equal<
  typeof session,
  Effect.Effect<ReadNextResult, Exclude<FollowFailure, DecodeFault>>
> = true;

export const decideType: Equal<
  typeof decide,
  Effect.Effect<
    Decided<number>,
    ReadFailure | AppendFailure<{ expectedOffset: string }> | Refused,
    DecodeService | EncodeService
  >
> = true;
