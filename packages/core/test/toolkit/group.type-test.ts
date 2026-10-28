import { Effect, Layer, Schema } from "effect";
import { HttpClient, HttpRouter } from "effect/http";
import { type CommitBoundary, Http, MemoryStorage, Storage, Streamsy } from "../../src/index.ts";
import { StreamsReader, StreamsWriter } from "../../src/protocol/index.ts";
import type { CommitHooks, OwnerRecords } from "../../src/internal/index.ts";
import { RemoteStreams } from "../../src/remote/index.ts";

class A extends Streamsy.json<A>()("GroupTypes/A", "a", {
  params: {},
  schema: Schema.String,
}) {}

class B extends Streamsy.bytes<B>()("GroupTypes/B", "b", { params: {} }) {}

const group = Streamsy.group(A, B);

type Equal<A, B> =
  // oxlint-disable-next-line typescript/no-unnecessary-type-parameters -- Universal quantification checks equality in both directions, including unions.
  (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;

const union: Equal<Streamsy.Services<typeof group>, A | B> = true;

const memory = MemoryStorage.layer(group);

const remoteStreams = RemoteStreams.layer(group, { baseUrl: "http://localhost" });

const list = RemoteStreams.layer(Streamsy.group(A, B), { baseUrl: "http://localhost" });

const remote: Equal<Layer.Success<typeof remoteStreams>, A | B> = true;

const remoteList: Equal<Layer.Success<typeof list>, A | B> = true;

const client: Equal<Layer.Services<typeof remoteStreams>, HttpClient.HttpClient> = true;

const hidden: Equal<
  Extract<Layer.Success<typeof memory>, StreamsReader | StreamsWriter>,
  never
> = true;

void [union, remote, remoteList, client, hidden];

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
  Storage | CommitBoundary | OwnerRecords | CommitHooks | A | B
> = true;

const memoryRequirements: Equal<Layer.Services<typeof memory>, never> = true;

const memoryErrors: Equal<Layer.Error<typeof memory>, never> = true;

const protocolRemote: Equal<
  Layer.Success<ReturnType<typeof RemoteStreams.layerProtocol>>,
  StreamsReader | StreamsWriter
> = true;

const mappedRemote = Effect.map(
  Effect.succeed({ baseUrl: "http://localhost" }),
  RemoteStreams.layerProtocol,
);

const pointFreeRemote: Equal<
  Layer.Success<Effect.Success<typeof mappedRemote>>,
  StreamsReader | StreamsWriter
> = true;

const emptyGroup = MemoryStorage.layer(Streamsy.group());

const emptyExact: Equal<
  Layer.Success<typeof emptyGroup>,
  Storage | CommitBoundary | OwnerRecords | CommitHooks
> = true;

void [memoryExact, memoryRequirements, memoryErrors, protocolRemote, pointFreeRemote, emptyExact];

// @ts-expect-error Application RemoteStreams always requires streams.
RemoteStreams.layer({ baseUrl: "http://localhost" });

// @ts-expect-error Application HTTP always requires streams.
Http.routes({ prefix: "/streams" });

// @ts-expect-error Hosts require an explicit group.
MemoryStorage.layer(A);

// @ts-expect-error HTTP requires an explicit group.
Http.routes([A], { access: "read" });

// @ts-expect-error Declaration metadata is internal.
void A.route;

// @ts-expect-error Declaration construction is internal.
void A.make;
