import { Context, Effect, Layer, Option, Stream } from "effect";
import type { StreamsFault } from "../fault.ts";
import { StreamsReader, StreamsWriter, type Reader, type Writer } from "../protocol/tags.ts";
import type {
  AppendOptions,
  CreateOptions,
  ReadOptions,
  ReadNextOptions,
} from "../protocol/options.ts";
import * as Backend from "./backend.ts";
import * as Route from "./route.ts";
import * as Ref from "./ref.ts";
import * as Streams from "./streams.ts";

export function bind<P, A, RD, RE>(
  route: Route.StreamRoute<P, A, RD, RE>,
  services: Backend.BackendServices,
) {
  const own = <A, E>(id: string, run: () => Effect.Effect<A, E>): Effect.Effect<A, E> =>
    Effect.suspend(() =>
      route.match(id)
        ? run()
        : Effect.die(new RangeError(`${id} is outside family ${route.template}`)),
    );
  const wire: Reader<StreamsFault> & Writer<StreamsFault> = {
    head: (id) => own(id, () => services.reader.head(id)),
    read: (id, options) => own(id, () => services.reader.read(id, options)),
    readNext: (id, options) => own(id, () => services.reader.readNext(id, options)),
    create: (id, options) =>
      own(id, () =>
        options?.forkedFrom === undefined
          ? services.writer.create(id, options)
          : own(options.forkedFrom, () => services.writer.create(id, options)),
      ),
    fork: (id, source, options) =>
      own(id, () => own(source, () => services.writer.fork(id, source, options))),
    append: (id, options) => own(id, () => services.writer.append(id, options)),
    remove: (id) => own(id, () => services.writer.remove(id)),
  };
  const context = Context.make(StreamsReader, wire).pipe(Context.add(StreamsWriter, wire));
  return {
    route,
    wire,
    create: (params: P, options?: Omit<CreateOptions, "contentType">) =>
      Streams.create(route.ref(params), options).pipe(Effect.provide(context)),
    append: (
      params: P,
      items: ReadonlyArray<A>,
      options?: Omit<AppendOptions, "data" | "contentType">,
    ) => Streams.append(route.ref(params), items, options).pipe(Effect.provide(context)),
    read: (params: P, options?: ReadOptions) =>
      Streams.read(route.ref(params), options).pipe(Stream.provideContext(context)),
    follow: (params: P, options?: ReadOptions) =>
      Streams.follow(route.ref(params), options).pipe(Stream.provideContext(context)),
    session: (params: P, options: ReadNextOptions) => wire.readNext(route.ref(params).id, options),
    head: (params: P) => wire.head(route.ref(params).id),
    remove: (params: P) => wire.remove(route.ref(params).id),
    fork: (params: P, source: P, options?: Omit<CreateOptions, "contentType" | "forkedFrom">) =>
      wire.fork(route.ref(params).id, route.ref(source).id, {
        ...options,
        contentType: route.ref(params).contentType,
      }),
  };
}
export type Family<P, A, RD = never, RE = never> = ReturnType<typeof bind<P, A, RD, RE>>;

function define<Self>() {
  return <P, A, RD, RE, R extends Route.StreamRoute<P, A, RD, RE>>(
    route: R & Route.StreamRoute<P, A, RD, RE>,
  ) =>
    Object.assign(
      Context.Service<Self, Family<P, A, RD, RE>>()(`@streamsy/core/Family/${route.template}`),
      route,
      { route, make: (services: Backend.BackendServices) => bind(route, services) },
    );
}
export const json =
  <Self>() =>
  <const T extends string, C extends Route.ParamCodecs, A, I, RD, RE>(
    ...args: Parameters<typeof Route.json<T, C, A, I, RD, RE>>
  ) =>
    define<Self>()(Route.json<T, C, A, I, RD, RE>(...args));
export const state =
  <Self>() =>
  <const T extends string, C extends Route.ParamCodecs, const Collections extends Ref.Collections>(
    ...args: Parameters<typeof Route.state<T, C, Collections>>
  ) =>
    define<Self>()(Route.state<T, C, Collections>(...args));
export const bytes =
  <Self>() =>
  <const T extends string, C extends Route.ParamCodecs>(
    ...args: Parameters<typeof Route.bytes<T, C>>
  ) =>
    define<Self>()(Route.bytes<T, C>(...args));

export class AnyStream extends define<AnyStream>()(
  Route.custom({
    parse: (id: string) => Option.some({ id }),
    ref: ({ id }: { readonly id: string }) => Ref.bytes(id),
  }),
) {}

/** Class instances carry Effect's service marker; individual Self types remain on each key. */
export interface FamilyIdentifier {
  readonly [Context.ServiceTypeId]: typeof Context.ServiceTypeId;
}

/** The provider and HTTP boundary need only the wire and the route grammar. */
export interface AnyFamily extends Context.Key<
  FamilyIdentifier,
  { readonly wire: Reader<StreamsFault> & Writer<StreamsFault> }
> {
  readonly route: Route.Matcher;
  readonly template: string;
  readonly match: (id: string) => boolean;
  readonly make: (services: Backend.BackendServices) => {
    readonly wire: Reader<StreamsFault> & Writer<StreamsFault>;
  };
}
export type Identifiers<Keys extends ReadonlyArray<AnyFamily>> = Keys[number]["Identifier"];

export function checkFamilies(keys: ReadonlyArray<AnyFamily>): void {
  const templates = new Set<string>();
  for (const [index, key] of keys.entries()) {
    if (templates.has(key.template))
      throw new RangeError(`duplicate family template: ${key.template}`);
    templates.add(key.template);
    if (key === AnyStream && index !== keys.length - 1)
      throw new RangeError("AnyStream must be last");
  }
  Backend.checkNoOverlap([
    { backend: Backend.make("family-check"), routes: keys.map((key) => key.route) },
  ]);
}

export function provides<Name extends string, const Keys extends ReadonlyArray<AnyFamily>>(
  backend: Backend.Backend<Name>,
  ...keys: Keys
): Layer.Layer<Identifiers<Keys>, never, Backend.BackendTag<Name>>;
export function provides<E, R, const Keys extends ReadonlyArray<AnyFamily>>(
  graph: Layer.Layer<StreamsReader | StreamsWriter, E, R>,
  ...keys: Keys
): Layer.Layer<Identifiers<Keys>, E, R>;
export function provides<E, R, Name extends string, const Keys extends ReadonlyArray<AnyFamily>>(
  source: Backend.Backend<Name> | Layer.Layer<StreamsReader | StreamsWriter, E, R>,
  ...keys: Keys
) {
  checkFamilies(keys);
  const services: Effect.Effect<
    Backend.BackendServices,
    never,
    Backend.BackendTag<Name> | StreamsReader | StreamsWriter
  > = "key" in source ? source.key : Effect.all({ reader: StreamsReader, writer: StreamsWriter });
  const layer = Layer.effectContext(
    Effect.map(services, (backend) => {
      let context = Context.empty();
      for (const key of keys) context = Context.add(context, key, key.make(backend));
      // SAFETY: the loop supplies exactly the keys in Keys, each through its own route.
      return context as Context.Context<Identifiers<Keys>>;
    }),
  );
  return "key" in source ? layer : layer.pipe(Layer.provide(source));
}
