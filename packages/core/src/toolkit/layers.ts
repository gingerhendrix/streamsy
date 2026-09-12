import { Context, Effect, Layer } from "effect";
import { NotSupported } from "../protocol/errors.ts";
import { StreamsReader, StreamsWriter } from "../protocol/tags.ts";
import * as Protocol from "../protocol/layer.ts";
import * as Memory from "../storage/memory/layer.ts";
import {
  checkNoOverlap,
  type AnyBackend,
  type AnyBinding,
  type Backend,
  type BackendServices,
  type BackendTag,
} from "./backend.ts";

export const layerMemory = (options: Memory.MemoryOptions & Protocol.ProtocolOptions = {}) =>
  Protocol.layer(options).pipe(Layer.provideMerge(Memory.layer(options)));

export interface RoutedOptions<Fallback extends AnyBackend = AnyBackend> {
  /** Serves any id that no route matches. Without it an unmatched id is a defect. */
  readonly fallback?: Fallback;
}

/** The one backend a named backend value requires. */
export type RequiredBackend<Named extends AnyBackend> =
  Named extends Backend<infer Name> ? BackendTag<Name> : never;

/** The exact backend services a binding table requires at the edge. */
export type RequiredBackends<Bindings extends ReadonlyArray<AnyBinding>> = {
  [Position in keyof Bindings]: RequiredBackend<Bindings[Position]["backend"]>;
}[number];

/**
 * One reader and one writer that forward to a backend per stream id.
 *
 * The Layer requires each bound backend key, so a missing `Layer.provide` is a
 * type error at the edge. It never calls `Layer.build` on a backend: re-tagging
 * through `Layer.provide` is what lets a sibling Layer share one storage graph
 * regardless of merge order.
 */
export const layerRouted = <
  Bindings extends ReadonlyArray<AnyBinding>,
  Fallback extends AnyBackend = AnyBackend,
>(
  bindings: Bindings,
  options: RoutedOptions<Fallback> = {},
): Layer.Layer<
  StreamsReader | StreamsWriter,
  never,
  RequiredBackends<Bindings> | RequiredBackend<Fallback>
> =>
  Layer.effectContext(
    // SAFETY: the generator reads each backend through its own binding's key, so
    // its requirement set is exactly the keys this binding table names. The
    // structural `AnyBinding` shape keeps a literal tuple's own names intact,
    // which is what makes a missing `Layer.provide` a type error at the edge.
    Effect.gen(function* () {
      yield* Effect.sync(() => checkNoOverlap(bindings));
      const table: Array<{
        readonly match: (id: string) => boolean;
        readonly services: BackendServices;
      }> = [];
      for (const binding of bindings) {
        const services = yield* binding.backend.key;
        for (const route of binding.routes) table.push({ match: route.match, services });
      }
      const fallback: BackendServices | undefined =
        options.fallback === undefined ? undefined : yield* options.fallback.key;
      const pick = Effect.fn("Streams.pickBackend")(function* (id: string) {
        const services = table.find((entry) => entry.match(id))?.services ?? fallback;
        if (services === undefined)
          return yield* Effect.die(new RangeError(`No route or fallback backend for stream ${id}`));
        return services;
      });
      const reader = StreamsReader.of({
        head: (id) => Effect.flatMap(pick(id), (services) => services.reader.head(id)),
        read: (id, opts) => Effect.flatMap(pick(id), (services) => services.reader.read(id, opts)),
        readNext: (id, opts) =>
          Effect.flatMap(pick(id), (services) => services.reader.readNext(id, opts)),
      });
      const writer = StreamsWriter.of({
        create: (id, opts) =>
          Effect.flatMap(pick(id), (services) => services.writer.create(id, opts)),
        fork: (id, source, opts) =>
          Effect.gen(function* () {
            const target = yield* pick(id);
            const origin = yield* pick(source);
            // A storage fork copies bytes inside one backend, so a cross-backend
            // fork has no honest implementation and no copy fallback.
            if (target !== origin)
              return yield* new NotSupported({ id, feature: "cross-backend-fork" });
            return yield* target.writer.fork(id, source, opts);
          }),
        append: (id, opts) =>
          Effect.flatMap(pick(id), (services) => services.writer.append(id, opts)),
        remove: (id) => Effect.flatMap(pick(id), (services) => services.writer.remove(id)),
      });
      return Context.make(StreamsReader, reader).pipe(Context.add(StreamsWriter, writer));
    }) as Effect.Effect<
      Context.Context<StreamsReader | StreamsWriter>,
      never,
      RequiredBackends<Bindings> | RequiredBackend<Fallback>
    >,
  );
