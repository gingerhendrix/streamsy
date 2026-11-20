import { Effect, Predicate, Record } from "effect";
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

type StreamMode = "remove" | "close";

interface RetiredOutput {
  readonly id: string;
  readonly result: "removed" | "closed" | "absent" | "shared";
}

export interface Retired {
  readonly record: RetirementStatus;
  readonly outputs: Readonly<Record<string, RetiredOutput>>;
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

        for (const { name, address, service } of owned) {
          if (owner !== undefined && service.owner !== owner)
            return yield* new ProjectionFault({
              phase: "checkpoint",
              reason: "unsupported-composition",
              message: `Output ${address.id} does not share the transaction owner`,
            });

          const result = yield* cleanStream(address, service.wire, mode);

          outputs.push([name, { id: address.id, result }]);
        }

        return yield* Effect.void;
      }),
    });

    return { record, outputs: Record.fromEntries(outputs) };
  });
};
