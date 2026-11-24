import { Array, Effect, Option, type Schema } from "effect";
import { ProjectionFault, type Slices } from "@streamsy/projection";
import type { SqlError } from "effect/sql";
import type { CircuitError } from "./circuit.ts";
import type { Backend } from "./backend.ts";
import { encodeRowKey } from "./runtime/key.ts";
import { dataFault } from "./sql/data-fault.ts";
import { type SqlEngine, type SqlEngineFault, type StateChange } from "./sql/engine.ts";
import { normalize } from "./sql/normalize.ts";
import type { MemoryEnlistFailure } from "./store.ts";
import { memoryEnlistFault } from "./view-backend.ts";
import { ViewFault } from "./view-fault.ts";
import type { Input } from "./view.ts";

export const normalizeUnit = (
  engine: SqlEngine,
  backend: Backend,
  namespace: string,
  sources: ReadonlyMap<string, { readonly state: Input; readonly collection: string }>,
  batch: Slices<Readonly<Record<string, ReturnType<Input["at"]>>>>,
) =>
  Effect.gen(function* () {
    const changes: StateChange[] = [];
    const store = backend.store(namespace);

    const active = Array.flatMap([...sources], ([sourceId, input]) => {
      const slice = batch[input.state.key];

      return slice === undefined
        ? []
        : [{ sourceId, input, slice, markerKey: encodeRowKey(sourceId) }];
    });

    const snapshots = yield* store.getMany(
      "@snapshots",
      Array.map(active, ({ markerKey }) => markerKey),
    );

    for (const { sourceId, input, slice, markerKey } of active) {
      const source = Array.findFirst(
        engine.program.nodes,
        (node) => node.kind === "source" && node.sourceId === sourceId,
      );

      if (Option.isNone(source)) throw new TypeError(`Missing compiled source ${sourceId}`);
      const location = engine.program.locations.get(source.value.id);

      if (location === undefined) throw new Error(`Missing source location ${sourceId}`);
      changes.push(
        ...(yield* normalize(
          backend,
          namespace,
          sourceId,
          source.value.id,
          input.collection,
          dataFault(location),
          slice.items,
          snapshots.has(markerKey),
        )),
      );
    }

    return changes;
  });

export const circuitErrors = <A, R>(
  body: Effect.Effect<
    A,
    CircuitError | SqlError.SqlError | Schema.SchemaError | SqlEngineFault | MemoryEnlistFailure,
    R
  >,
): Effect.Effect<A, CircuitError, R> =>
  body.pipe(
    Effect.catchTags({
      MemoryEnlistDefect: () => Effect.fail(memoryEnlistFault()),
      SqlError: (cause) =>
        new ProjectionFault({
          phase: "process",
          reason: "storage-failure",
          message: "Cannot update circuit state",
          cause,
        }),
      SchemaError: (cause) => new ViewFault({ message: cause.message }),
      SqlEngineFault: (cause) => new ViewFault({ message: "Circuit operator failed", cause }),
    }),
  );
