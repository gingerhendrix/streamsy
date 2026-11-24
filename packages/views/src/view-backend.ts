import { CommitBoundary, MemoryStorage, Streamsy } from "@streamsy/core";
import { ownerOf } from "@streamsy/core/internal";
import { CheckpointStore, OwnerTransaction } from "@streamsy/projection";
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

interface MemoryBackend {
  readonly backend: Backend;
  readonly owner: object;
  readonly live: Set<string>;
}

interface BackendState {
  readonly backend: Effect.Effect<Backend, ViewFault>;
  readonly memory?: MemoryBackend;
}

class BackendService implements Opaque {
  readonly [BackendTypeId] = true;
  readonly [BackendState]: BackendState;

  constructor(state: BackendState) {
    this[BackendState] = state;
  }
}

// Different backend layers can still share the owner's checkpoint key space.
const ownerSlots = new WeakMap<object, Set<string>>();

export const memoryBackendOf = (service: Opaque) =>
  service instanceof BackendService ? service[BackendState].memory : undefined;

export const makeViewBackend = (backend: Effect.Effect<Backend, ViewFault>): Opaque =>
  new BackendService({ backend });

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
    Effect.map(CommitBoundary, (boundary) => {
      const owner = ownerOf(boundary);
      const backend = makeMemoryBackend(owner);

      let live = ownerSlots.get(owner);

      if (live === undefined) {
        live = new Set();
        ownerSlots.set(owner, live);
      }

      return new BackendService({
        backend: Effect.succeed(backend),
        memory: { backend, owner, live },
      });
    }),
  );

  /** Provide heap operator state and transactions for a host with external inputs. */
  static readonly layerMemoryHost: Layer.Layer<ViewBackend | CheckpointStore | OwnerTransaction> =
    Layer.mergeAll(ViewBackend.layerMemory, CheckpointStore.layer, OwnerTransaction.layer).pipe(
      Layer.provide(MemoryStorage.layer(Streamsy.group())),
    );
}

export const currentBackend = Effect.flatMap(ViewBackend, (service) =>
  service instanceof BackendService
    ? service[BackendState].backend
    : Effect.die(new TypeError("Expected a Views backend service")),
);
