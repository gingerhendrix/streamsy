import { Context, Effect, Predicate, Record } from "effect";
import {
  isStreamsFault,
  type AppendError,
  type RemoveError,
  type StreamAddress,
  type StreamsFault,
} from "@streamsy/core";
import { CurrentOwner } from "@streamsy/core/internal";
import type { StreamRoute } from "@streamsy/core/protocol";
import type { ProjectionKey, RetirementStatus } from "./checkpoint-store.ts";
import type { Commit } from "./commit.ts";
import type { ResolvedOutput } from "./commit/internal.ts";
import { bindAtGeneration, type Declaration } from "./declaration.ts";
import { ProjectionFault } from "./fault.ts";
import { boundOutputs, type BoundOutputs } from "./outputs.ts";
import { GenerationOutputs } from "./per-generation.ts";
import { currentRouteRecords, RoutedOutputs } from "./routed-outputs.ts";

type StreamMode = "remove" | "close";

export interface RetiredOutput {
  readonly id: string;
  readonly result: "removed" | "closed" | "absent" | "shared";
}

export interface Retired {
  readonly record: RetirementStatus;
  readonly outputs: Readonly<Record<string, RetiredOutput>>;
  /** Exclusively owned streams opened by each routed output. */
  readonly routed?: Readonly<Record<string, ReadonlyArray<RetiredOutput>>>;
}

interface Retirable<R, Q> extends ProjectionKey, BoundOutputs<R> {
  readonly commit: Pick<Commit<never, never, Q>, "retire">;
}

const cleanStream = (
  address: StreamAddress.StreamAddress<unknown>,
  wire: ResolvedOutput["service"]["wire"],
  mode: StreamMode,
): Effect.Effect<RetiredOutput["result"], ProjectionFault> => {
  const { id, contentType } = address;

  const operation: Effect.Effect<"removed" | "closed", AppendError | RemoveError | StreamsFault> =
    mode === "remove"
      ? wire.remove(id).pipe(Effect.as("removed" as const))
      : wire.append(id, { close: true, data: new Uint8Array(), contentType }).pipe(
          Effect.as("closed" as const),
          Effect.catchTag("StreamClosed", () => Effect.succeed("closed" as const)),
        );

  return operation.pipe(
    Effect.catchTags({
      StreamNotFound: () => Effect.succeed("absent" as const),
      StreamGone: () => Effect.succeed("absent" as const),
    }),
    Effect.mapError(
      (cause) =>
        new ProjectionFault({
          phase: "checkpoint",
          reason: Predicate.isTagged(cause, "StreamBusy")
            ? "output-busy"
            : isStreamsFault(cause)
              ? "storage-failure"
              : "invalid-output",
          message: `Cannot ${mode} output ${id}`,
          cause,
        }),
    ),
  );
};

/** Retire one binding and clean up its exclusively owned generation outputs. */
export const retire = <C extends StreamRoute.ParamCodecs, R, Q, HE, HR>(
  declaration: Declaration<C, Retirable<R, Q>, HE, HR>,
  params: StreamRoute.Params<C>,
  options: { readonly generation: number; readonly streams?: StreamMode },
): Effect.Effect<Retired, ProjectionFault, R | Q> => {
  const bound = bindAtGeneration(declaration, params, options.generation);
  const mode = options.streams ?? "remove";

  return Effect.gen(function* () {
    if (bound?.commit.retire === undefined)
      return yield* new ProjectionFault({
        phase: "checkpoint",
        reason: "unsupported-composition",
        message: "Retirement requires a projection declaration with a Commit that supports it",
      });

    const outputs: [string, RetiredOutput][] = [];
    const owned: (ResolvedOutput & { readonly name: string })[] = [];
    const bindings = bound[GenerationOutputs];
    const routes = bound[RoutedOutputs];
    const routed = new Map<string, RetiredOutput[]>();
    const context = yield* Effect.context<R>();

    if (routes !== undefined) for (const name of routes.keys()) routed.set(name, []);

    for (const [name, address] of Record.toEntries(boundOutputs(bound))) {
      const binding = bindings?.get(name);

      if (binding === undefined) continue;

      if (binding.shared) {
        outputs.push([name, { id: address.id, result: "shared" }]);
      } else {
        owned.push({ name, address, service: yield* address.stream });
      }
    }

    const record = yield* bound.commit.retire({
      key: {
        id: bound.id,
        version: bound.version,
        generation: bound.generation,
        params: bound.params,
      },
      cleanup: Effect.gen(function* () {
        const owner = yield* CurrentOwner;

        const clean = (address: ResolvedOutput["address"], service: ResolvedOutput["service"]) => {
          if (owner !== undefined && service.owner !== owner)
            return Effect.fail(
              new ProjectionFault({
                phase: "checkpoint",
                reason: "unsupported-composition",
                message: `Output ${address.id} does not share the transaction owner`,
              }),
            );

          return cleanStream(address, service.wire, mode).pipe(
            Effect.map((result): RetiredOutput => ({ id: address.id, result })),
          );
        };

        for (const { name, address, service } of owned)
          outputs.push([name, yield* clean(address, service)]);

        if (routes !== undefined) {
          const records = yield* currentRouteRecords;

          for (const { output, streamId } of yield* records.list(bound)) {
            const definition = routes.get(output);
            const results = routed.get(output);

            const invalid = (cause?: unknown) =>
              new ProjectionFault({
                phase: "checkpoint",
                reason: "invalid-record",
                message: `Cannot restore routed output ${output} at ${streamId}`,
                cause,
              });

            const address = yield* Effect.try({
              try: () => definition?.restore(streamId),
              catch: invalid,
            });

            if (address === undefined || results === undefined) return yield* invalid();
            results.push(yield* clean(address, Context.get(context, address.stream)));
          }

          if (mode === "remove") yield* records.clear(bound);
        }

        return yield* Effect.void;
      }),
    });

    const retired = { record, outputs: Record.fromEntries(outputs) };

    return routes === undefined ? retired : { ...retired, routed: Record.fromEntries(routed) };
  });
};
