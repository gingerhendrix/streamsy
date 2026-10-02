import { Effect, Layer, Schema } from "effect";
import { HttpClient, HttpRouter } from "effect/unstable/http";
import {
  type CommitBoundary,
  type TransportFault,
  Http,
  Memory,
  Storage,
  Streamsy,
} from "../../src/index.ts";
import { StreamsReader, StreamsWriter } from "../../src/protocol/index.ts";
import type { OwnerRecords } from "../../src/internal/index.ts";
import * as Fetch from "../../src/fetch/index.ts";
class A extends Streamsy.json<A>()("GroupTypes/A", "a", {
  params: {},
  schema: Schema.String,
}) {}
class B extends Streamsy.bytes<B>()("GroupTypes/B", "b", { params: {} }) {}
const group = Streamsy.group(A, B);
type Equal<A, B> =
  (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;
const union: Equal<Streamsy.Services<typeof group>, A | B> = true;
const memory = Memory.layer(group);
const fetch = Fetch.layer(group, { baseUrl: "http://localhost" });
const list = Fetch.layer(Streamsy.group(A, B), { baseUrl: "http://localhost" });
const remote: Equal<Layer.Success<typeof fetch>, A | B> = true;
const remoteList: Equal<Layer.Success<typeof list>, A | B> = true;
const client: Equal<Layer.Services<typeof fetch>, HttpClient.HttpClient> = true;
const feeds: Equal<
  Extract<Layer.Success<typeof memory>, Streamsy.ChangeFeed<A | B>>,
  Streamsy.ChangeFeed<A | B>
> = true;
const hidden: Equal<
  Extract<Layer.Success<typeof memory>, StreamsReader | StreamsWriter>,
  never
> = true;
const noFeed: Equal<Extract<Layer.Success<typeof fetch>, Streamsy.ChangeFeed<A | B>>, never> = true;
const needsFeed = Effect.gen(function* () {
  yield* Streamsy.ChangeFeed(A);
});
// oxlint-disable effecttsgo/missing-effect-context -- This negative type test deliberately omits the Streamsy.ChangeFeed capability.
// @ts-expect-error Fetch streams cannot supply a Streamsy.ChangeFeed capability.
const missingFeed: Effect.Effect<void, TransportFault, HttpClient.HttpClient> = needsFeed.pipe(
  Effect.provide(fetch),
);
// oxlint-enable effecttsgo/missing-effect-context
void [union, remote, remoteList, client, feeds, hidden, noFeed, missingFeed];

const routes = Http.routes(group, { access: "full" });
const routeRequirements: Equal<
  Layer.Services<typeof routes>,
  HttpRouter.HttpRouter | HttpRouter.Request.From<"Requires", A | B>
> = true;
// @ts-expect-error A group's host must explicitly choose an HTTP access policy.
Http.routes(group);
void routeRequirements;

const memoryExact: Equal<
  Layer.Success<typeof memory>,
  Storage | CommitBoundary | OwnerRecords | A | B | Streamsy.ChangeFeed<A | B>
> = true;
const memoryRequirements: Equal<Layer.Services<typeof memory>, never> = true;
const memoryErrors: Equal<Layer.Error<typeof memory>, never> = true;
const protocolFetch: Equal<
  Layer.Success<ReturnType<typeof Fetch.layerProtocol>>,
  StreamsReader | StreamsWriter
> = true;
const mappedFetch = Effect.map(
  Effect.succeed({ baseUrl: "http://localhost" }),
  Fetch.layerProtocol,
);
const pointFreeFetch: Equal<
  Layer.Success<Effect.Success<typeof mappedFetch>>,
  StreamsReader | StreamsWriter
> = true;
const hasFeed: Effect.Effect<void> = needsFeed.pipe(Effect.provide(memory));
const emptyGroup = Memory.layer(Streamsy.group());
const emptyExact: Equal<
  Layer.Success<typeof emptyGroup>,
  Storage | CommitBoundary | OwnerRecords
> = true;
void [
  memoryExact,
  memoryRequirements,
  memoryErrors,
  protocolFetch,
  pointFreeFetch,
  hasFeed,
  emptyExact,
];

// @ts-expect-error Application Fetch always requires streams.
Fetch.layer({ baseUrl: "http://localhost" });
// @ts-expect-error Application HTTP always requires streams.
Http.routes({ prefix: "/streams" });

// @ts-expect-error Hosts require an explicit group.
Memory.layer(A);
// @ts-expect-error HTTP requires an explicit group.
Http.routes([A], { access: "read" });
// @ts-expect-error Declaration metadata is internal.
void A.route;
// @ts-expect-error Declaration construction is internal.
void A.make;
