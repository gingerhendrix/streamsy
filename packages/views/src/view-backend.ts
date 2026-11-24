import { CommitBoundary } from "@streamsy/core";
import { ownerOf } from "@streamsy/core/internal";
import { Context, Effect, Layer, Option } from "effect";
import { SqlClient } from "effect/sql";
import type { Backend } from "./backend.ts";
// oxlint-disable-next-line anti-slop-effect/no-service-constructor-imports -- The owning ViewBackend layer allocates the private memory store here.
import { makeMemoryBackend } from "./memory/backend.ts";
import { backendFactoryFor } from "./sql/backend.ts";
import { nonRetryableViewFault, type ViewFault } from "./view-fault.ts";

const BackendTypeId = Symbol("@streamsy/views/ViewBackend");

interface Opaque {
  readonly [BackendTypeId]: true;
}

const BackendState = Symbol("@streamsy/views/ViewBackend/state");

class BackendService implements Opaque {
  readonly [BackendTypeId] = true;
  readonly [BackendState]: Effect.Effect<Backend, ViewFault>;

  constructor(backend: Effect.Effect<Backend, ViewFault>) {
    this[BackendState] = backend;
  }
}

export const makeViewBackend = (backend: Effect.Effect<Backend, ViewFault>): Opaque =>
  new BackendService(backend);

export const memoryEnlistFault = (): ViewFault =>
  nonRetryableViewFault(
    "Views memory state requires the active memory owner transaction on this fiber",
  );

/** Host-selected operator storage for Views circuits. */
export class ViewBackend extends Context.Service<ViewBackend, Opaque>()(
  "@streamsy/views/ViewBackend",
) {
  /** Store operator state in the host database. Run View.migrate with the same client. */
  static readonly layerSql: Layer.Layer<ViewBackend, ViewFault, SqlClient.SqlClient> = Layer.effect(
    this,
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const create = yield* backendFactoryFor(sql);

      return makeViewBackend(
        Effect.flatMap(Effect.serviceOption(sql.transactionService), (transaction) =>
          Option.isSome(transaction)
            ? // A server backend caches namespace digests for this operation alone.
              Effect.sync(create)
            : Effect.fail(
                nonRetryableViewFault(
                  "The Views SQL client is outside the storage owner's transaction",
                ),
              ),
        ),
      );
    }),
  );

  /** Store operator state in the heap and join the memory owner's transaction. */
  static readonly layerMemory: Layer.Layer<ViewBackend, never, CommitBoundary> = Layer.effect(
    this,
    Effect.map(CommitBoundary, (boundary) =>
      makeViewBackend(Effect.succeed(makeMemoryBackend(ownerOf(boundary)))),
    ),
  );
}

export const currentBackend = Effect.flatMap(ViewBackend, (service) =>
  service instanceof BackendService
    ? service[BackendState]
    : Effect.die(new TypeError("Expected a Views backend service")),
);
