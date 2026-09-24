import { Context, Effect, Layer, Option, Stream } from "effect";
import { StreamId } from "../schema/index.ts";
import { ForkSourceNotFound } from "../protocol/errors.ts";
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

/** Build a family implementation over backend values, retaining the same ownership guard. */
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
          : route.match(options.forkedFrom)
            ? services.writer.create(id, options)
            : Effect.fail(
                new ForkSourceNotFound({ id, source: StreamId.make(options.forkedFrom) }),
              ),
      ),
    fork: (id, source, options) =>
      own(id, () =>
        route.match(source)
          ? services.writer.fork(id, source, options)
          : Effect.fail(new ForkSourceNotFound({ id, source })),
      ),
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

/** A concrete member retains the family key used to access its scoped wire. */
export type Member<A, R, RD = never, RE = never> = Ref.StreamRef<A, RD, RE> & {
  readonly family: Context.Key<R, { readonly wire: Reader<StreamsFault> & Writer<StreamsFault> }>;
};

export type FamilyClass<
  Self,
  Name extends string,
  P,
  A,
  RD,
  RE,
  R extends Route.StreamRoute<P, A, RD, RE>,
> = Context.ServiceClass<Self, `@streamsy/core/Family/${Name}`, Family<P, A, RD, RE>> &
  R & {
    readonly route: R;
    readonly family: Context.ServiceClass<
      Self,
      `@streamsy/core/Family/${Name}`,
      Family<P, A, RD, RE>
    >;
    readonly at: (params: P) => ReturnType<R["ref"]> & Member<A, Self, RD, RE>;
    readonly make: (services: Backend.BackendServices) => Family<P, A, RD, RE>;
  };

function define<Self>() {
  return <const Name extends string, P, A, RD, RE, R extends Route.StreamRoute<P, A, RD, RE>>(
    name: Name,
    route: R & Route.StreamRoute<P, A, RD, RE>,
  ): FamilyClass<Self, Name, P, A, RD, RE, R> => {
    const key: `@streamsy/core/Family/${Name}` = `@streamsy/core/Family/${name}`;
    const service = Context.Service<Self, Family<P, A, RD, RE>>()(key);
    return Object.assign(service, route, {
      route,
      family: service,
      at: (params: P): ReturnType<R["ref"]> & Member<A, Self, RD, RE> => {
        const member: unknown = Object.assign(route.ref(params), { family: service });
        // SAFETY: the route returns its declared ref, including State collections; only its key is added.
        return member as ReturnType<R["ref"]> & Member<A, Self, RD, RE>;
      },
      make: (services: Backend.BackendServices) => bind(route, services),
    });
  };
}
export type JsonFamilyClass<
  Self,
  Name extends string,
  T extends string,
  C extends Route.ParamCodecs,
  A,
  I,
  RD,
  RE,
> = FamilyClass<
  Self,
  Name,
  Route.Params<C>,
  A,
  RD,
  RE,
  ReturnType<typeof Route.json<T, C, A, I, RD, RE>>
>;
export const json =
  <Self>() =>
  <const Name extends string, const T extends string, C extends Route.ParamCodecs, A, I, RD, RE>(
    name: Name,
    ...args: Parameters<typeof Route.json<T, C, A, I, RD, RE>>
  ): JsonFamilyClass<Self, Name, T, C, A, I, RD, RE> =>
    define<Self>()(name, Route.json<T, C, A, I, RD, RE>(...args));
export type StateFamilyClass<
  Self,
  Name extends string,
  T extends string,
  C extends Route.ParamCodecs,
  Collections extends Ref.Collections,
> = FamilyClass<
  Self,
  Name,
  Route.Params<C>,
  Ref.CollectionsChange<Collections>,
  Ref.CollectionsDecodingServices<Collections>,
  Ref.CollectionsEncodingServices<Collections>,
  ReturnType<typeof Route.state<T, C, Collections>>
>;
export const state =
  <Self>() =>
  <
    const Name extends string,
    const T extends string,
    C extends Route.ParamCodecs,
    const Collections extends Ref.Collections,
  >(
    name: Name,
    ...args: Parameters<typeof Route.state<T, C, Collections>>
  ): StateFamilyClass<Self, Name, T, C, Collections> =>
    define<Self>()(name, Route.state<T, C, Collections>(...args));
export type BytesFamilyClass<
  Self,
  Name extends string,
  T extends string,
  C extends Route.ParamCodecs,
> = FamilyClass<
  Self,
  Name,
  Route.Params<C>,
  Uint8Array,
  never,
  never,
  ReturnType<typeof Route.bytes<T, C>>
>;
export const bytes =
  <Self>() =>
  <const Name extends string, const T extends string, C extends Route.ParamCodecs>(
    name: Name,
    ...args: Parameters<typeof Route.bytes<T, C>>
  ): BytesFamilyClass<Self, Name, T, C> =>
    define<Self>()(name, Route.bytes<T, C>(...args));

export type CustomFamilyClass<Self, Name extends string, P, A, RD, RE> = FamilyClass<
  Self,
  Name,
  P,
  A,
  RD,
  RE,
  ReturnType<typeof Route.custom<P, A, RD, RE>>
>;
export const custom =
  <Self>() =>
  <const Name extends string, P, A, RD, RE>(
    name: Name,
    options: Parameters<typeof Route.custom<P, A, RD, RE>>[0],
  ): CustomFamilyClass<Self, Name, P, A, RD, RE> =>
    define<Self>()(name, Route.custom(options));

export class AnyStream extends define<AnyStream>()(
  "AnyStream",
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
  const names = new Set<string>();
  for (const [index, key] of keys.entries()) {
    if (names.has(key.key)) throw new RangeError(`duplicate family name: ${key.key}`);
    names.add(key.key);
    if (key.template !== "" && templates.has(key.template))
      throw new RangeError(`duplicate family template: ${key.template}`);
    templates.add(key.template);
    if (key.key === AnyStream.key && index !== keys.length - 1)
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
