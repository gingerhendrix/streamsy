import { Array, Context, Effect, Option, Predicate, Record, Semaphore } from "effect";
import { ZERO_OFFSET, type Streamsy } from "@streamsy/core";
import { CurrentOwner } from "@streamsy/core/internal";
import {
  CheckpointStore,
  recordKey,
  type CheckpointStoreApi,
  type ProjectionKey,
} from "./checkpoint-store.ts";
import type { DecodedParams, StreamDeclaration } from "./declaration.ts";
import { ProjectionFault } from "./fault.ts";
import { createOutput } from "./outputs.ts";

/** Open one generation-owned stream in the current process transaction. */
export interface RoutedHandle<A, Params, R> {
  readonly open: (params: Params) => Effect.Effect<A, ProjectionFault, R>;
}

export const RoutedOutputs = Symbol.for("@streamsy/projection/internal/RoutedOutputs");

export interface RoutedDefinition<R> {
  readonly matches: (streamId: string) => boolean;
  readonly restore: (streamId: string) => Streamsy.Address<unknown, R> | undefined;
}

export interface RoutedOutputBindings<R> {
  readonly [RoutedOutputs]?: ReadonlyMap<string, RoutedDefinition<R>>;
}

const processFault = (reason: ProjectionFault["reason"], message: string, cause?: unknown) =>
  new ProjectionFault({ phase: "process", reason, message, cause });

export const requireRouteRecords = (
  store: CheckpointStoreApi | undefined,
  phase: "process" | "checkpoint",
) =>
  store?.routes === undefined
    ? Effect.fail(
        new ProjectionFault({
          phase,
          reason: "unsupported-composition",
          message: "This CheckpointStore does not support routed outputs",
        }),
      )
    : Effect.succeed(store.routes);

export const currentRouteRecords = Effect.serviceOption(CheckpointStore).pipe(
  Effect.flatMap((store) => requireRouteRecords(Option.getOrUndefined(store), "checkpoint")),
);

export const routedUnit = (key: ProjectionKey, store: CheckpointStoreApi) => ({
  key: recordKey(key),
  store,
  opened: new Map<string, string>(),
  lock: Semaphore.makeUnsafe(1),
});

export const TransactionRoutedOutputs = Context.Reference<
  ReturnType<typeof routedUnit> | undefined
>("@streamsy/projection/internal/TransactionRoutedOutputs", { defaultValue: () => undefined });

export const routedDefinition = (
  route: StreamDeclaration,
): RoutedDefinition<Streamsy.Any["Identifier"]> => ({
  matches: route.match,
  restore: (id) => {
    const parsed = route.parse(id);

    if (Option.isNone(parsed)) return undefined;
    const address = route.at(parsed.value);

    return address.id === id ? address : undefined;
  },
});

export const routedHandle = <R>(
  name: string,
  route: StreamDeclaration,
  names: readonly string[],
  params: DecodedParams,
  key: ProjectionKey,
  occupied: ReadonlySet<string>,
  routed: ReadonlyMap<string, RoutedDefinition<R>>,
): RoutedHandle<Streamsy.Address<unknown, R>, DecodedParams, R> => ({
  open: (values) =>
    Effect.gen(function* () {
      const unit = yield* TransactionRoutedOutputs;

      if (unit === undefined || unit.key !== recordKey(key))
        return yield* processFault(
          "unsupported-composition",
          `Routed output ${name} requires its binding's process transaction`,
        );

      const address: Streamsy.Address<unknown, R> = yield* Effect.try({
        try: () => {
          if (
            Record.keys(values).length !== names.length ||
            !Array.every(names, (param) => Record.has(values, param))
          )
            throw new RangeError(`Expected route parameters ${Array.join(names, ", ")}`);

          return route.at({ ...params, ...values });
        },
        catch: (cause) =>
          processFault(
            "invalid-output",
            `Cannot resolve routed output ${name}: ${cause instanceof RangeError ? cause.message : String(cause)}`,
            cause,
          ),
      });

      if (occupied.has(address.id))
        return yield* processFault(
          "invalid-output",
          `Routed output ${name} overlaps an input or output at ${address.id}`,
        );

      if (!Predicate.isTagged(address, "Json"))
        return yield* processFault(
          "invalid-output",
          "Projection outputs require JSON or state routes",
        );

      const service = yield* address.stream;
      const owner = yield* CurrentOwner;

      if (service.owner !== owner)
        return yield* processFault(
          "unsupported-composition",
          `Output ${address.id} does not share the transaction owner`,
        );

      return yield* unit.lock.withPermits(1)(
        Effect.gen(function* () {
          const opened = unit.opened.get(address.id);

          if (opened === name) return address;

          if (opened !== undefined)
            return yield* processFault(
              "invalid-output",
              `Routed outputs ${opened} and ${name} both name ${address.id}`,
            );

          const records = yield* requireRouteRecords(unit.store, "process");

          if ((yield* records.state(key, name, address.id)) !== "live") {
            for (const [other, definition] of routed) {
              if (
                other !== name &&
                definition.matches(address.id) &&
                (yield* records.has(key, other, address.id))
              )
                return yield* processFault(
                  "invalid-output",
                  `Routed outputs ${other} and ${name} both name ${address.id}`,
                );
            }

            const created = yield* createOutput(address, service.wire);

            if (created.closed || created.nextOffset !== ZERO_OFFSET)
              return yield* processFault(
                "invalid-output",
                `Generation-bound output ${address.id} must start empty and open`,
              );

            yield* records.add(key, name, address.id);
          }

          unit.opened.set(address.id, name);

          return address;
        }),
      );
    }),
});
