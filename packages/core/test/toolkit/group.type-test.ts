import { Effect, Layer, Schema } from "effect";
import { HttpClient, HttpRouter } from "effect/unstable/http";
import {
  Changes,
  Http,
  Memory,
  StreamFamily,
  StreamGroup,
  StreamsReader,
  StreamsWriter,
} from "../../src/index.ts";
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
// @ts-expect-error Fetch families cannot supply a Changes capability.
const missingFeed: Effect.Effect<void, unknown, HttpClient.HttpClient> = needsFeed.pipe(
  Effect.provide(fetch),
);
void [union, remote, remoteList, client, feeds, hidden, noFeed, missingFeed];

const routes = Http.routes(group, { access: "full" });
const routeRequirements: Equal<
  Layer.Services<typeof routes>,
  HttpRouter.HttpRouter | HttpRouter.Request.From<"Requires", A | B>
> = true;
// @ts-expect-error A group's host must explicitly choose an HTTP access policy.
Http.routes(group);
void routeRequirements;
