# Storage adapter authoring

A Streamsy storage package implements one flat interface: `StorageAdapter`.

```ts
import type { StorageAdapter } from "@streamsy/core";
```

`StorageAdapter` is the whole seam. Every per-stream method takes `streamId` as
its first argument, and the lifecycle intents (`create` / `fork` / `delete`) take
a plan that carries the id. There is **no returned per-stream handle** — nothing
lifetime-bearing or non-serializable crosses the seam.

## The seam invariant

> Core owns protocol policy. Adapters own durable facts plus exactly one atomic
> boundary per intent. Nothing non-serializable — no `AbortSignal`, DOM object,
> timer, or closure — crosses the storage seam. Every adapter method argument and
> result is **structured-clone-serializable** data.

Note the invariant is structured clone (the Durable Object RPC boundary), not
JSON: `StoredMessage.data` is a `Uint8Array`, which survives structured clone
but is not plain JSON. An adapter that transports seam values over JSON (an
HTTP-remote backend) must encode binary data itself (e.g. base64) — taking
"JSON" literally corrupts message bytes. The conformance kit enforces the
invariant mechanically by `structuredClone`-ing every argument and result.

Concretely:

- **Serializable in and out.** Arguments and results are structured-clone-safe
  data, so a call maps directly onto a Durable Object RPC, an HTTP request, or a
  SQL statement. (An `AbortSignal` reaching the seam is exactly the bug this
  design removed — it cannot be structured-cloned across a Durable Object RPC.)
- **One-shot, not interactive.** Core does all read-decide work first
  (idempotency, fork materialization, expiry/lineage policy) and hands the adapter
  a fully-determined mutation. The adapter never reads-then-branches inside its
  transaction.
- **Optimistic concurrency via preconditions.** `expectedOffset` /
  `expectedClosed` / producer compare-and-set are checked atomically with the
  writes — this is what makes a one-shot commit safe.
- **One atomic boundary per call.** Each method wraps its whole intent in exactly
  one transaction / serialized actor turn.

Adapters keep a **private** per-stream handle and delegate to it internally (a
`Map` keyed by id, a SQL row, a Durable Object stub). Core builds its own
ergonomic per-stream view with `bindStream(adapter, id)` (`BoundStream`); that is
core-internal and not part of the seam.

## The interface

```ts
export interface StorageAdapter {
  // --- per-stream reads (streamId-first) ---
  getRecord(streamId: StreamId): Promise<StreamRecord | null>;
  listMessages(streamId: StreamId, options?: ListMessagesOptions): Promise<StoredMessage[]>;
  getProducerState(streamId: StreamId, producerId: string): Promise<ProducerState | undefined>;

  // --- per-stream write intent ---
  append(streamId: StreamId, plan: AppendPlan): Promise<StorageAppendResult>;

  // --- per-stream live wait (required) ---
  awaitChange(streamId: StreamId, options: AwaitChangeOptions): Promise<AwaitChangeResult>;

  // --- per-stream expiry ---
  scheduleExpiry(streamId: StreamId, at: number): Promise<void> | void;
  cancelExpiry(streamId: StreamId): Promise<void> | void;

  // --- lifecycle intents (plans carry the id) ---
  create(plan: CreatePlan): Promise<StorageCreateResult>;
  fork?(plan: ForkPlan): Promise<StorageForkResult>; // optional capability
  delete(plan: DeletePlan): Promise<StorageDeleteResult>;
}
```

The adapter-level result unions are root-exported with a `Storage` prefix
(`StorageAppendResult` / `StorageCreateResult` / `StorageForkResult` /
`StorageDeleteResult`) so they never collide with the protocol-level
`AppendResult` / `CreateResult` / `DeleteResult`:

```ts
import type { StorageAppendResult, StorageCreateResult } from "@streamsy/core";
```

```ts
export function createExampleStorageAdapter(): StorageAdapter {
  const streams = new ExampleStreamRegistry(); // private per-id handles

  return {
    getRecord: (streamId) => streams.get(streamId).getRecord(),
    listMessages: (streamId, options) => streams.get(streamId).listMessages(options),
    getProducerState: (streamId, producerId) => streams.get(streamId).getProducerState(producerId),
    append: (streamId, plan) => streams.get(streamId).append(plan),
    awaitChange: (streamId, options) => streams.get(streamId).awaitChange(options),
    scheduleExpiry: (streamId, at) => streams.get(streamId).scheduleExpiry(at),
    cancelExpiry: (streamId) => streams.get(streamId).cancelExpiry(),
    create: (plan) => /* insert record + initial messages atomically */,
    fork: (plan) => /* validate source + create child atomically; edge may be a saga */,
    delete: (plan) => /* execute lineage policy: purge or soft-delete */,
  };
}
```

## The offset contract

`Offset` is an opaque, case-sensitive string. The only ordering guarantee at
the adapter seam is: **ordinary lexicographic order = stream order**. Adapters
must not parse, normalize, pad, or otherwise interpret offsets. SQL
`WHERE offset > ?`, key-range scans, and plain string comparison are all
correct. `compareOffsets` is exported for in-process comparisons.

Core's default generator retains the historic `<counter:16>_<sub:16>` values,
and `ZERO_OFFSET` is its empty-stream tail. Applications can inject another
`OffsetGenerator` (for example monotonic ULIDs), including its own
`initialOffset` and boundary validator. Core allocates offsets before crossing
the storage seam and verifies generated values are safe, valid for that scheme,
and strictly increasing. Adapters require no generator-specific configuration.

`StreamRecord.counter` is retained for persisted-record compatibility and is
incremented with message-bearing mutations, but generation and ordering do not
depend on it. A custom generator never has to parse an offset into a number.

`ListMessagesOptions` windowing is pinned as:

- `after` is **exclusive** — the message at `after` is not returned;
- `until` is **inclusive** — the message at `until` is returned;
- results are in ascending offset order; `limit` truncates after windowing.

Within one stream incarnation an offset only ever advances. A purge followed by
a re-create restarts the sequence — which is why the level-triggered predicate
treats offset _inequality_, not just advance, as change.

## `append` — the atomic write intent

`append` is Streamsy's only per-stream write. It commits, in **one** transaction:

- the optional `messages` (already framed by core),
- the **required** `recordPatch` (offset advance and compatibility-counter update; a pure close folds in
  via `recordPatch.lifecycle.closed`),
- an optional producer compare-and-set,

all guarded by `preconditions` checked atomically with the writes:

```ts
interface AppendPlan {
  preconditions: {
    expectedOffset?: Offset; // CAS: the tail is still at this offset
    expectedClosed?: boolean; // CAS: the stream is (not) closed
    producer?: { producerId: string; expected?: ProducerState; next: ProducerState };
  };
  messages?: StoredMessage[];
  recordPatch: StreamRecordPatch; // REQUIRED
}

type StorageAppendResult =
  | { status: "appended"; record: StreamRecord }
  | {
      status: "precondition-failed";
      record: StreamRecord | null;
      reason: "offset" | "closed" | "producer";
    };
```

Rules:

- **All-or-nothing.** On any failed precondition, write nothing — no message, no
  record patch, no producer change — and return `precondition-failed` with
  `reason` naming the precondition that failed and `record` set to the latest
  record (so core can decide whether to retry).
- **`reason` is required.** When multiple preconditions fail simultaneously,
  attribute in the order **offset → closed → producer**. A backend with opaque
  conditional writes (a single conditional `UPDATE`) may derive `reason`
  best-effort from a post-failure re-read under concurrency, reporting
  `"offset"` when the failure cannot be attributed (e.g. the record was
  concurrently purged); the single-writer case must be exact — the conformance
  kit tests exactly that.
- **Producer CAS: absent `expected` means "must not exist yet".** It is an
  insert-if-absent, NOT "don't check, just set" — fail with reason `"producer"`
  when `expected` is absent but a state already exists.
- **Patches set values; they never delete them.** An absent `StreamRecordPatch`
  field means "leave unchanged". Clearing a field is deliberately
  inexpressible (it would behave differently over JSON vs structured clone);
  core never clears.
- **No effects beyond the plan.** Expiry scheduling is core's job: core calls
  `scheduleExpiry` / `cancelExpiry` back on the adapter after your commit
  returns. Do not schedule timers from inside `append`.

### TTL "touch": a lifecycle-only append

A TTL renewal is a valid `append` whose `recordPatch` carries **only** lifecycle
state (the new `expiresAtMs`) and does **not** advance `currentOffset` or the compatibility `counter`
or add messages. Core's `ExpiryPolicy.touch` issues exactly this on a sliding-TTL
read:

```ts
await adapter.append(streamId, {
  preconditions: { expectedOffset: record.currentOffset },
  recordPatch: { lifecycle: { expiresAtMs } }, // no offset/counter advance
});
```

Adapters must accept this shape (it is an ordinary append with an empty message
set and a lifecycle-only patch), and — importantly — it must **not** trip a parked
`awaitChange`: a touch wakes waiters, but because it advances nothing observable
(`currentOffset` / `closed` / `softDeleted` are unchanged) a level-triggered
`awaitChange` re-checks and keeps waiting.

## `awaitChange` — the level-triggered live wait (required)

`awaitChange` lets a live reader block until change-relevant state advances. It is
**required** — core wires in no polling fallback. If your backend can wake
cheaply (an internal notifier, a Durable Object alarm/SSE), wake immediately. If
it cannot, implement `awaitChange` by polling your own durable reads. Either
way, use the exported loop: `runAwaitChangeLoop` is the contract-faithful
level-triggered loop every first-party adapter uses — you supply `readRecord`
(sync or async) and `waitForWake` (for a pure polling adapter, a plain sleep),
and it handles the re-read/diff/park cycle, budget accounting, and caps.

```ts
interface StreamChangeSnapshot {
  present: boolean;
  currentOffset: Offset;
  closed: boolean;
  softDeleted: boolean;
}
interface AwaitChangeOptions {
  fromOffset: Offset;
  observedClosed?: boolean;
  observedSoftDeleted?: boolean;
  timeoutMs: number;
}
type AwaitChangeResult =
  | { status: "changed"; snapshot: StreamChangeSnapshot }
  | { status: "timeout"; snapshot: StreamChangeSnapshot };
```

The **level-triggered contract** every implementation must satisfy:

1. **Re-read current durable state first** and build a `StreamChangeSnapshot`.
2. **Return `changed` immediately** if the snapshot differs from what the caller
   observed — purged (`!present`), newly `softDeleted`, newly `closed`, or
   `currentOffset` **not equal to** `fromOffset` (inequality, not just advance:
   a lower offset means the stream was purged and re-created while parked).
3. Otherwise **register a waiter and block until `timeoutMs`**. On wake, re-check
   (1–2); return `changed` if it now differs, else keep waiting within the
   remaining budget, returning `timeout` with the latest snapshot when it expires.
4. A successful, state-advancing intent (`append` / `create` / `fork` / `delete`)
   **wakes local waiters after durable facts are visible.**
5. **Cap each park if your read→register window is not atomic w.r.t. wakes.**
   The entry re-read (step 1) catches commits that land _before_ `awaitChange`
   is called, but if there is async I/O between your re-read and your waiter
   registration (the natural shape for any network-DB backend), a wake fired in
   that window can be lost. Such implementations MUST bound each park
   (`parkCapMs`) so a missed wake is repaired within the cap — the loop becomes
   bounded-stale by construction instead of stalling to the full `timeoutMs`.
6. **`timeout` means "no relevant change observed yet"** — it does NOT promise
   the full budget elapsed. Adapters MAY return `timeout` early (bounded parks
   are encouraged for remote backends); callers MUST re-park rather than assume
   the budget was consumed.

Core exports the loop plus the primitives it is built from, so every
`awaitChange` — native or polling — decides "did anything relevant change?"
identically:

```ts
import { runAwaitChangeLoop } from "@streamsy/core";
// buildChangeSnapshot / changeSnapshotDiffers are also exported for custom loops.

awaitChange(streamId: StreamId, options: AwaitChangeOptions): Promise<AwaitChangeResult> {
  return runAwaitChangeLoop(
    {
      readRecord: () => this.readRecord(streamId),        // may be async
      waitForWake: (ms) => this.notifier.waitForWake(ms), // or a plain sleep when polling
      // totalCapMs: 1500,  // remote backends: cap the total wait per call
      // parkCapMs: 250,    // lossy/async wake sources: repair missed wakes within the cap
    },
    options,
  );
}
```

Step 1's re-read removes the _pre-call_ lost-notify race: a commit landing
between the caller's observation and the `awaitChange` call is caught by the
entry re-read, so the wake in step 4 is only a **latency optimization** —
correctness (data) never depends on it. A wake racing your own registration
mid-flight is the remaining latency hazard, and rule 5's park cap bounds it.

Notes:

- **The wake bus is internal.** There is no public `notify` on the seam; wake your
  own waiters from inside successful mutations.
- **Cancellation stays caller-local.** Core races your wait against the caller's
  `AbortSignal` (`raceAbortAwaitChange`) on its own side; the signal never reaches
  the adapter. Bound `timeoutMs` so a lost race settles promptly — a remote adapter
  (e.g. a Durable Object) should cap the total budget (`totalCapMs`) so a single
  call never strands the actor; early `timeout` returns are licensed (rule 6).

## `create` / `fork` / `delete` — lifecycle intents

```ts
type StorageCreateResult = { status: "created"; record } | { status: "exists"; record };
type StorageForkResult =
  | { status: "created"; record }
  | { status: "exists"; record }
  | { status: "fork-source-gone" };
type StorageDeleteResult =
  | { status: "purged" }
  | { status: "retained-soft-deleted" }
  | { status: "not-found" }
  | { status: "gone" };
```

- **`create`** materializes the plan's record (and any initial messages)
  atomically. The record is the **single source of truth** — a created-closed
  stream arrives with `lifecycle.closed` / `closedAt` already folded in by core;
  persist it as given. A second create of the same id is idempotent (`exists`
  carries the existing record so core can classify it).
- **`fork`** is optional and **capability-by-presence**: the **presence of the
  method is the capability**, so no flag is needed. Define it if your backend can
  atomically validate the source and create the child with its materialized
  prefix in one transaction — **child creation is the atomic boundary**; the
  lineage edge may be part of that transaction OR a convergent, idempotent saga
  (see below). **Omit it and forks are unsupported** for that backend — there is
  **no core fork fallback**; the protocol surfaces a fork intent (`create` with
  `forkedFrom`) as the structured `not-supported` result. A forkless adapter is
  still a valid minimal adapter for every non-fork operation. On `exists`,
  return the existing child record — core runs the same config-match idempotency
  as create, so a racing byte-identical fork converges instead of conflicting —
  and repair the lineage edge if the existing child's `forkedFrom` matches
  (healing an earlier fork that committed the child but lost the edge).
- **`delete`** executes core's lineage policy and reports what happened: `purged`
  (no dependents — also cascade-reclaims now-orphaned soft-deleted ancestors),
  `retained-soft-deleted` (has dependents), `not-found`, or `gone` (already
  soft-deleted). The caller does not choose soft-vs-purge; the policy does.

Cross-stream operations (fork-edge registration, delete/GC cascades) are
convergent, idempotent sagas — a retry or later pass repairs a missing edge or
finishes reclaiming an already-soft-deleted ancestor. Only the child-record
creation itself must be atomic; the first-party Durable Object adapter's fork is
exactly this shape (atomic child commit, then edge add, with exists-repair).

## Expiry scheduling — `scheduleExpiry` / `cancelExpiry`

The expiry seam has a deliberate division of labour:

- **Lazy expiry carries correctness.** Core checks `lifecycle.expiresAtMs` on
  every access (`expireIfNeeded`) and deletes an expired stream before serving
  it. A stream past its deadline is never observable, even with no timers at
  all.
- **Timers make reclaim timely.** `scheduleExpiry(streamId, at)` asks the
  adapter to arrange a wakeup at epoch-ms `at` (replacing any earlier schedule
  for that stream); `cancelExpiry(streamId)` drops it. A **no-op implementation
  is legal** — storage is reclaimed lazily on next access instead of promptly.
- **The return path is a constructor convention, not a seam method.** A fired
  timer must reach core as a call to `protocol.handleScheduledExpiry(streamId)`
  — core then re-checks the deadline (a slid deadline is re-armed, not dropped)
  and issues the actual `delete({ reason: "expiry" })`. First-party adapters
  take an `onScheduledExpiry` callback at construction and wire it to the
  protocol:

```ts
// Adapter construction:
const adapter = createExampleStorageAdapter({
  onScheduledExpiry: (streamId) => protocol.handleScheduledExpiry(streamId),
});
// Inside the adapter, a fired timer simply calls options.onScheduledExpiry(id).
```

Never delete the stream directly from your timer — the deadline may have slid
forward (a sliding-TTL read renews it after the timer was armed); core owns that
decision and re-arms the timer when needed.

The conformance kit cannot exercise timer firing generically (the return path is
off-seam), so it only proves the methods exist and accept calls; correctness is
carried by core's lazy checks.

## Unsupported features

A minimal adapter must still provide every method. For a **sub-method** feature
you cannot support (one that capability-by-presence cannot express — e.g.
producer CAS lives inside `AppendPlan` with no method to omit), throw the typed
storage-level error; the HTTP dispatch layer catches it and returns the public
structured `not-supported` result (400 with a `stream-not-supported` header)
instead of an internal error:

```ts
import { unsupported } from "@streamsy/core"; // throws NotSupportedError

append(streamId, plan) {
  if (plan.preconditions.producer) throw unsupported("producers");
  // ...
}
```

Any **other** thrown error is treated as internal (500). Whole-method
capabilities use presence instead: `fork` is the only optional method.

## Conformance

Core exports a reusable contract kit. Run it against your adapter to prove it
satisfies the seam: append preconditions/atomicity (including the
absent-`expected` producer CAS), offset windowing semantics, the level-triggered
`awaitChange` (entry re-read, wake-on-mutation, offset-regression, non-advancing
wakes — the interleaved lost-wake torture lives in `runAwaitChangeLoop`'s own
tests, where the race is controllable), `create`/`fork`/`delete`, and
caller-local cancellation. Every case runs through a `structuredClone` wrapper,
so the serializability invariant is enforced mechanically:

```ts
import { runStorageAdapterContract } from "@streamsy/core";
import { describe, it } from "vitest"; // or "bun:test"

describe("example adapter — storage contract", () => {
  // `makeAdapter` must return a fresh, isolated adapter on every call.
  runStorageAdapterContract(() => createExampleStorageAdapter(), { it });
});
```

The kit also drives a **forkless adapter** (an adapter with `fork` omitted) so the
minimal-adapter floor is held to the same contract. Fork-dependent cases are
deliberately skipped for a forkless adapter because `fork` is
capability-by-presence — its protocol fork intents are `not-supported`, not a
fallback — so the kit never assumes the capability is present. (`awaitChange`, by
contrast, is required, so the kit always exercises it.)

## Protocol wiring

Applications pass the adapter on the protocol dependency object:

```ts
const protocol = createStreamProtocol({
  storage: { adapter: createExampleStorageAdapter() },
  longPollTimeoutMs: 1500,
});
```

Core resolves per-stream calls with `bindStream(adapter, id)` and exposes
protocol-bound streams via `protocol.create(...)` / `protocol.get(...)`. The
storage adapter stays an internal persistence boundary; it is not the
HTTP/application protocol object.

## Durable Object guidance

The Durable Object package uses one storage object per public stream id. The
host-facing entrypoint is `createDurableObjectStorageAdapter({ namespace })`; it
routes each `streamId` to `namespace.get(namespace.idFromName(streamId))` and calls
the bound stub directly — **no forwarding proxy**. The Durable Object methods are
self-initializing: the first per-stream call binds the object to its id (persisted
on the actor), so there is no separate `init` round-trip. Because the whole seam is
flat serializable data, each call maps onto a stub RPC with no handle round-trip
and no non-serializable value to strip.
