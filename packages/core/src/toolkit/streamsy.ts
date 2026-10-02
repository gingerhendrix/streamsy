import { Context, Effect, Layer, Option, Stream } from "effect";
import { checkWriteOwner, storageOf, ownerOf } from "../internal/owner.ts";
import { Storage } from "../storage/storage.ts";
import { ChangeFeed } from "./changes.ts";
import { StreamId } from "../schema/index.ts";
import { ForkSourceNotFound } from "../protocol/errors.ts";
import type { StreamsFault } from "../fault.ts";
import { StreamsReader, StreamsWriter, type Reader, type Writer } from "../protocol/tags.ts";
import type { LazyArg } from "effect/Function";
import type {
  AppendCallOptions,
  CreateOptions,
  ReadOptions,
  ReadNextOptions,
} from "../protocol/options.ts";

import { templatesOverlap } from "./route-internal.ts";
import * as Route from "./route.ts";
import * as StreamAddress from "./address.ts";
import * as Streams from "./streams.ts";

interface Wires {
  readonly reader: Reader<StreamsFault>;
  readonly writer: Writer<StreamsFault>;
}

const templateOf = (route: Route.Matcher): string | undefined => {
  if (!("template" in route)) return undefined;
  // SAFETY: StreamRoute carries a string template; custom routes carry the empty string.
  const template = route.template as string;
  return template.length > 0 ? template : undefined;
};

/** Template streams must have disjoint id grammars. Custom routes use declaration order. */
function checkNoOverlap(routes: ReadonlyArray<Route.Matcher>): void {
  const templates = routes.flatMap((route) => {
    const template = templateOf(route);
    return template === undefined ? [] : [template];
  });
  for (const [index, template] of templates.entries())
    for (const other of templates.slice(index + 1))
      if (templatesOverlap(template, other))
        throw new RangeError(`stream route templates overlap: ${template} and ${other}`);
}

/** Build a stream service over one protocol graph's wires, with the ownership guard on writes. */
function wireApi<P, A, RD, RE>(route: Route.StreamRoute<P, A, RD, RE>, services: Wires) {
  const storage = storageOf(services.writer);
  const owner = storage === undefined ? services.writer : ownerOf(storage);
  const write = <A, E>(id: string, run: () => Effect.Effect<A, E>) =>
    checkWriteOwner(owner, id, storage !== undefined).pipe(Effect.andThen(Effect.suspend(run)));
  const own = <A, E>(id: string, run: () => Effect.Effect<A, E>): Effect.Effect<A, E> =>
    Effect.suspend(() =>
      route.match(id)
        ? run()
        : Effect.die(new RangeError(`${id} is outside stream ${route.template}`)),
    );
  const wire: Reader<StreamsFault> & Writer<StreamsFault> = {
    head: (id) => own(id, () => services.reader.head(id)),
    read: (id, options) => own(id, () => services.reader.read(id, options)),
    readNext: (id, options) => own(id, () => services.reader.readNext(id, options)),
    create: (id, options) =>
      write(id, () =>
        own(id, () =>
          options?.forkedFrom === undefined
            ? services.writer.create(id, options)
            : route.match(options.forkedFrom)
              ? services.writer.create(id, options)
              : Effect.fail(
                  new ForkSourceNotFound({ id, source: StreamId.make(options.forkedFrom) }),
                ),
        ),
      ),
    fork: (id, source, options) =>
      write(id, () =>
        own(id, () =>
          route.match(source)
            ? services.writer.fork(id, source, options)
            : Effect.fail(new ForkSourceNotFound({ id, source })),
        ),
      ),
    append: (id, options) => write(id, () => own(id, () => services.writer.append(id, options))),
    remove: (id) => write(id, () => own(id, () => services.writer.remove(id))),
  };
  const context = Context.make(StreamsReader, wire).pipe(Context.add(StreamsWriter, wire));
  return {
    owner,
    route,
    wire,
    create: (params: P, options?: Omit<CreateOptions, "contentType">) =>
      Streams.create(route.address(params), options).pipe(Effect.provide(context)),
    append: <const O extends AppendCallOptions = {}>(
      params: P,
      items: Streams.AppendItems<A, O>,
      options?: O,
    ) => Streams.append(route.address(params), items, options).pipe(Effect.provide(context)),
    read: (params: P, options?: ReadOptions) =>
      Streams.read(route.address(params), options).pipe(Stream.provideContext(context)),
    fold: <S>(params: P, initial: LazyArg<S>, f: (state: S, item: A) => S, options?: ReadOptions) =>
      Streams.fold(route.address(params), initial, f, options).pipe(Effect.provide(context)),
    collect: (params: P, options?: ReadOptions) =>
      Streams.collect(route.address(params), options).pipe(Effect.provide(context)),
    follow: (params: P, options?: ReadOptions) =>
      Streams.follow(route.address(params), options).pipe(Stream.provideContext(context)),
    session: (params: P, options: ReadNextOptions) =>
      wire.readNext(route.address(params).id, options),
    head: (params: P) => wire.head(route.address(params).id),
    remove: (params: P) => wire.remove(route.address(params).id),
    fork: (params: P, source: P, options?: Omit<CreateOptions, "contentType" | "forkedFrom">) =>
      wire.fork(route.address(params).id, route.address(source).id, {
        ...options,
        contentType: route.address(params).contentType,
      }),
  };
}
export type Api<P, A, RD = never, RE = never> = ReturnType<typeof wireApi<P, A, RD, RE>>;

/** An address names one stream. It holds no data. It is not an Effect `Ref`. */
export type Address<A, R, RD = never, RE = never> = StreamAddress.StreamAddress<A, RD, RE> & {
  readonly stream: Context.Key<
    R,
    { readonly owner: object; readonly wire: Reader<StreamsFault> & Writer<StreamsFault> }
  >;
};

export type Class<
  Self,
  Name extends string,
  P,
  A,
  RD,
  RE,
  R extends Route.StreamRoute<P, A, RD, RE>,
> = Context.ServiceClass<Self, `@streamsy/core/Stream/${Name}`, Api<P, A, RD, RE>> &
  R & {
    readonly at: (params: P) => ReturnType<R["address"]> & Address<A, Self, RD, RE>;
  };

/**
 * Names are global Context identities, including across separate providers.
 * Use an application namespace for distinct streams. Reusing a name intentionally
 * aliases the service; providers cannot detect collisions in other Layer graphs.
 * Wildcard is reserved for the built-in catch-all and its intentional aliases.
 */
function define<Self>() {
  return <const Name extends string, P, A, RD, RE, R extends Route.StreamRoute<P, A, RD, RE>>(
    name: Name,
    route: R & Route.StreamRoute<P, A, RD, RE>,
  ): Class<Self, Name, P, A, RD, RE, R> => {
    const key: `@streamsy/core/Stream/${Name}` = `@streamsy/core/Stream/${name}`;
    const service = Context.Service<Self, Api<P, A, RD, RE>>()(key);
    const declared = Object.defineProperty(service, metadataKey, {
      value: { route, make: (services: Wires) => wireApi(route, services) },
    });
    return Object.assign(declared, route, {
      at: (params: P): ReturnType<R["address"]> & Address<A, Self, RD, RE> => {
        const address: unknown = Object.assign(route.address(params), { stream: service });
        // SAFETY: the route returns its declared address, including State collections; only its key is added.
        return address as ReturnType<R["address"]> & Address<A, Self, RD, RE>;
      },
    });
  };
}
export type JsonClass<
  Self,
  Name extends string,
  T extends string,
  C extends Route.ParamCodecs,
  A,
  I,
  RD,
  RE,
> = Class<
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
  ): JsonClass<Self, Name, T, C, A, I, RD, RE> =>
    define<Self>()(name, Route.json<T, C, A, I, RD, RE>(...args));
export type StateClass<
  Self,
  Name extends string,
  T extends string,
  C extends Route.ParamCodecs,
  Collections extends StreamAddress.Collections,
> = Class<
  Self,
  Name,
  Route.Params<C>,
  StreamAddress.CollectionsChange<Collections>,
  StreamAddress.CollectionsDecodingServices<Collections>,
  StreamAddress.CollectionsEncodingServices<Collections>,
  ReturnType<typeof Route.state<T, C, Collections>>
>;
export const state =
  <Self>() =>
  <
    const Name extends string,
    const T extends string,
    C extends Route.ParamCodecs,
    const Collections extends StreamAddress.Collections,
  >(
    name: Name,
    ...args: Parameters<typeof Route.state<T, C, Collections>>
  ): StateClass<Self, Name, T, C, Collections> =>
    define<Self>()(name, Route.state<T, C, Collections>(...args));
export type BytesClass<
  Self,
  Name extends string,
  T extends string,
  C extends Route.ParamCodecs,
> = Class<
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
  ): BytesClass<Self, Name, T, C> =>
    define<Self>()(name, Route.bytes<T, C>(...args));

export type CustomClass<Self, Name extends string, P, A, RD, RE> = Class<
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
  ): CustomClass<Self, Name, P, A, RD, RE> =>
    define<Self>()(name, Route.custom(options));

const metadataKey = Symbol.for("@streamsy/core/Stream/metadata");
interface Metadata {
  readonly route: Route.Matcher;
  readonly make: (services: Wires) => {
    readonly owner: object;
    readonly wire: Reader<StreamsFault> & Writer<StreamsFault>;
  };
}
function declaration(key: Any): Metadata {
  // SAFETY: define installs this inherited symbol on every declaration, including across package copies.
  const value = (key as Any & { readonly [metadataKey]?: Metadata })[metadataKey];
  if (value === undefined) throw new TypeError(`Unknown stream declaration: ${key.key}`);
  return value;
}
export class Wildcard extends define<Wildcard>()(
  "Wildcard",
  Route.custom({
    parse: (id: string) => Option.some({ id }),
    address: ({ id }: { readonly id: string }) => StreamAddress.bytes(id),
  }),
) {}

/** Class instances carry Effect's service marker; individual Self types remain on each key. */
export interface StreamIdentifier {
  readonly [Context.ServiceTypeId]: typeof Context.ServiceTypeId;
}

/** The provider and HTTP boundary need only the wire and the route grammar. */
export interface Any extends Context.Key<
  StreamIdentifier,
  { readonly owner: object; readonly wire: Reader<StreamsFault> & Writer<StreamsFault> }
> {
  readonly template: string;
  readonly match: (id: string) => boolean;
}
export type Identifiers<Keys extends ReadonlyArray<Any>> = Keys[number]["Identifier"];

export function checkStreams(keys: ReadonlyArray<Any>): void {
  const templates = new Set<string>();
  const names = new Set<string>();
  for (const [index, key] of keys.entries()) {
    if (names.has(key.key)) throw new RangeError(`duplicate stream name: ${key.key}`);
    names.add(key.key);
    if (key.template !== "" && templates.has(key.template))
      throw new RangeError(`duplicate stream template: ${key.template}`);
    templates.add(key.template);
    if (key.key === Wildcard.key && index !== keys.length - 1)
      throw new RangeError("Wildcard must be last");
  }
  checkNoOverlap(keys.map((key) => declaration(key).route));
}

/**
 * Bind streams to their owning protocol graph.
 * Graphs exposing Storage also provide ChangeFeed.
 */
export function bind<A, E, R, const Keys extends ReadonlyArray<Any>>(
  graph: Layer.Layer<A | StreamsReader | StreamsWriter, E, R>,
  group: Group<Keys>,
): Layer.Layer<
  Identifiers<Keys> | (Storage extends A ? ChangeFeed<Identifiers<Keys>> : never),
  E,
  R
>;
export function bind<E, R, const Keys extends ReadonlyArray<Any>>(
  source: Layer.Layer<StreamsReader | StreamsWriter, E, R>,
  group: Group<Keys>,
) {
  const keys = group.streams;
  checkStreams(keys);
  const bindContext = (wires: Wires, storage?: typeof Storage.Service) => {
    if (
      storage !== undefined &&
      (storageOf(wires.reader) !== storage || storageOf(wires.writer) !== storage)
    )
      throw new RangeError("The exposed Storage does not own the protocol wires");
    let context = Context.empty();
    for (const key of keys) {
      context = Context.add(context, key, declaration(key).make(wires));
      if (storage !== undefined)
        context = Context.add(context, ChangeFeed(key), {
          owner: ownerOf(storage),
          changes: (id) =>
            key.match(id)
              ? storage.changes(id)
              : Stream.die(new RangeError(`${id} is outside stream ${key.template}`)),
        });
    }
    // SAFETY: each stream comes from its factory; feed keys are added only for an exposed Storage.
    return context as Context.Context<Identifiers<Keys>>;
  };
  return source.pipe(
    Layer.flatMap((context) =>
      Layer.syncContext(() =>
        bindContext(
          {
            reader: Context.get(context, StreamsReader),
            writer: Context.get(context, StreamsWriter),
          },
          Option.getOrUndefined(Context.getOption(context, Storage)),
        ),
      ),
    ),
  );
}

export interface Group<Keys extends ReadonlyArray<Any> = ReadonlyArray<Any>> {
  readonly streams: Keys;
}
export type Services<G extends Group> = G["streams"][number]["Identifier"];
export function group<const Keys extends ReadonlyArray<Any>>(...keys: Keys): Group<Keys> {
  checkStreams(keys);
  return Object.freeze({ streams: Object.freeze(keys) });
}
export { ChangeFeed, type ChangeFeedApi } from "./changes.ts";
