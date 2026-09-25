import { Effect, Layer, Schema } from "effect";
import { HttpClient, HttpRouter } from "effect/unstable/http";
import {
  type TransportFault,
  Changes,
  Http,
  Memory,
  Storage,
  StreamFamily,
  StreamGroup,
} from "../../src/index.ts";
import { StreamsReader, StreamsWriter } from "../../src/protocol/index.ts";
import type { MemoryCommitBoundary } from "../../src/internal/memory.ts";
import * as Fetch from "../../src/fetch/index.ts";
class A extends StreamFamily.json<A>()("GroupTypes/A", "a", {
  params: {},
  schema: Schema.String,
}) {}
class B extends StreamFamily.bytes<B>()("GroupTypes/B", "b", { params: {} }) {}
const group = StreamGroup.make(A, B);
type Equal<A, B> =
  (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;
const union: Equal<StreamGroup.Services<typeof group>, A | B> = true;
const memory = Memory.layer({}, group);
const fetch = Fetch.layer({ baseUrl: "http://localhost" }, group);
const list = Fetch.layer({ baseUrl: "http://localhost" }, A, B);
const remote: Equal<Layer.Success<typeof fetch>, A | B> = true;
const remoteList: Equal<Layer.Success<typeof list>, A | B> = true;
const client: Equal<Layer.Services<typeof fetch>, HttpClient.HttpClient> = true;
const feeds: Equal<Extract<Layer.Success<typeof memory>, Changes<A | B>>, Changes<A | B>> = true;
const hidden: Equal<
  Extract<Layer.Success<typeof memory>, StreamsReader | StreamsWriter>,
  never
> = true;
const noFeed: Equal<Extract<Layer.Success<typeof fetch>, Changes<A | B>>, never> = true;
const needsFeed = Effect.gen(function* () {
  yield* Changes(A);
});
// oxlint-disable effecttsgo/missing-effect-context -- This negative type test deliberately omits the Changes capability.
// @ts-expect-error Fetch families cannot supply a Changes capability.
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
  Storage | MemoryCommitBoundary | A | B | Changes<A | B>
> = true;
const memoryRequirements: Equal<Layer.Services<typeof memory>, never> = true;
const memoryErrors: Equal<Layer.Error<typeof memory>, never> = true;
const legacyFetch: Equal<
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
const emptyGroup = Memory.layer({}, StreamGroup.make());
const emptyExact: Equal<Layer.Success<typeof emptyGroup>, Storage | MemoryCommitBoundary> = true;
void [
  memoryExact,
  memoryRequirements,
  memoryErrors,
  legacyFetch,
  pointFreeFetch,
  hasFeed,
  emptyExact,
];

// @ts-expect-error Application Fetch always requires families.
Fetch.layer({ baseUrl: "http://localhost" });
// @ts-expect-error Application HTTP always requires families.
Http.routes({ prefix: "/streams" });
