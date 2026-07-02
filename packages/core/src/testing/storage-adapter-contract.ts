/**
 * Reusable storage-adapter contract kit.
 *
 * Every adapter (memory, SQLite, Durable Object, and any third-party backend)
 * must satisfy the same flat {@link StorageAdapter} seam contract below the
 * public HTTP/protocol layer. This kit encodes that contract as a set of
 * behavioural cases so each backend is held to it identically.
 *
 * Runner-agnostic by design: the kit imports no test framework. The caller
 * passes a minimal `{ it }` harness (vitest's `it`, Bun's `it`, or any
 * `(name, fn) => void`), so the same cases run under whichever runtime a given
 * adapter needs (vitest for memory/DO-routing, `bun:test` for `bun:sqlite`).
 *
 * `makeAdapter` MUST return a fresh, isolated adapter on every call — each case
 * builds its own adapter and uses fixed stream ids.
 *
 * Serializability is enforced mechanically: every case runs through a wrapper
 * that `structuredClone`s every argument and result crossing the seam, so a
 * value that cannot survive a Durable Object RPC (a closure, an `AbortSignal`,
 * a live handle) fails the kit on any backend.
 *
 * Coverage (ADR 0001 Stage 4):
 *   - `append`: preconditions (`offset` / `closed` / `producer`, including the
 *     absent-`expected` "must not exist" CAS) fail with the correct reason and
 *     write nothing (atomicity); success advances the tail and folds `closed`;
 *     a lifecycle-only TTL "touch" renews without advancing.
 *   - reads: `listMessages` `after`/`until` windowing (exclusive-after,
 *     inclusive-until) over lexicographic offset order.
 *   - `awaitChange`: immediate-change, timeout, wake-on-append / close /
 *     soft-delete / purge, offset-regression (purge → re-create) detection, the
 *     lost-notify entry re-read guard (a commit landing before registration is
 *     caught by the level-triggered re-read; interleaved-wake torture lives in
 *     the `runAwaitChangeLoop` unit tests, where the race is controllable), and
 *     "a non-advancing wake re-checks and keeps waiting".
 *   - `create` / `fork` / `delete`: idempotent create, fork as a
 *     capability-by-presence (omitting it is a valid configuration — the
 *     protocol surfaces fork intents as `not-supported`, there is no core
 *     fallback) with `fork-source-gone` and record-carrying `exists`, delete
 *     `purged` / `retained-soft-deleted` / `not-found` / `gone` with cascade
 *     reclaim.
 *   - cancellation: `raceAbortAwaitChange` composed over a real adapter wait
 *     resolves timeout-shaped on abort and never rejects the caller.
 *
 * Deliberate gap: `scheduleExpiry` / `cancelExpiry` timer firing is not
 * kit-testable — the fired-timer return path (`onScheduledExpiry` →
 * `protocol.handleScheduledExpiry`) is an off-seam constructor convention the
 * kit cannot wire generically. The kit only proves the methods exist and accept
 * calls; expiry correctness is carried by lazy `expireIfNeeded` in core.
 */
import type { AppendPlan, StorageAdapter, StorageAppendResult } from "../types/storage-adapter.ts";
import type {
  AwaitChangeOptions,
  AwaitChangeResult,
  StreamId,
  StreamRecord,
} from "../types/storage.ts";
import { allocate, formatCounter, ZERO_OFFSET } from "../protocol/helpers/offset-generator.ts";
import { buildChangeSnapshot } from "../protocol/helpers/change-snapshot.ts";
import { raceAbortAwaitChange } from "../protocol/helpers/race-abort.ts";

/** Minimal test-runner surface. Satisfied by vitest's and `bun:test`'s `it`. */
export interface StorageAdapterContractHarness {
  it(name: string, fn: () => void | Promise<void>): void;
}

/** Build a fresh, isolated adapter for one contract case. */
export type MakeStorageAdapter = () => StorageAdapter | Promise<StorageAdapter>;

const TEXT_TYPE = "text/plain";
const OFFSET_1 = formatCounter(1);

/**
 * Mechanical enforcement of the seam invariant: every argument and result must
 * survive `structuredClone` (the Durable Object RPC boundary). The wrapper
 * clones on the way in AND on the way out, so a non-serializable value crossing
 * the seam throws on every backend — including in-process ones that would
 * otherwise pass by reference.
 */
const clone = <T>(value: T): T => structuredClone(value);

function withSerializedSeam(adapter: StorageAdapter): StorageAdapter {
  const wrapped: StorageAdapter = {
    getRecord: async (streamId) => clone(await adapter.getRecord(clone(streamId))),
    listMessages: async (streamId, options) =>
      clone(await adapter.listMessages(clone(streamId), clone(options))),
    getProducerState: async (streamId, producerId) =>
      clone(await adapter.getProducerState(clone(streamId), clone(producerId))),
    append: async (streamId, plan) => clone(await adapter.append(clone(streamId), clone(plan))),
    awaitChange: async (streamId, options) =>
      clone(await adapter.awaitChange(clone(streamId), clone(options))),
    scheduleExpiry: (streamId, at) => adapter.scheduleExpiry(clone(streamId), clone(at)),
    cancelExpiry: (streamId) => adapter.cancelExpiry(clone(streamId)),
    create: async (plan) => clone(await adapter.create(clone(plan))),
    delete: async (plan) => clone(await adapter.delete(clone(plan))),
  };
  // Capability-by-presence: only expose `fork` when the underlying adapter has it.
  const fork = adapter.fork;
  if (fork) {
    wrapped.fork = async (plan) => clone(await fork.call(adapter, clone(plan)));
  }
  return wrapped;
}

/** Wrap `makeAdapter` so every case exercises the serializability wrapper. */
function serialized(makeAdapter: MakeStorageAdapter): MakeStorageAdapter {
  return async () => withSerializedSeam(await makeAdapter());
}

function encode(value: string): Uint8Array {
  return new TextEncoder().encode(value);
}

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(`storage-adapter-contract: ${message}`);
}

function assertStatus(result: { status: string }, expected: string, label: string): void {
  if (result.status !== expected) {
    throw new Error(
      `storage-adapter-contract: ${label} — expected status "${expected}", got "${result.status}"`,
    );
  }
}

function newRecord(id: StreamId, forkedFrom?: StreamId): StreamRecord {
  return {
    id,
    config: { contentType: TEXT_TYPE, createdAt: 0 },
    lifecycle: forkedFrom ? { forkedFrom, forkOffset: ZERO_OFFSET } : {},
    currentOffset: ZERO_OFFSET,
    counter: 0,
  };
}

async function currentRecord(adapter: StorageAdapter, id: StreamId): Promise<StreamRecord> {
  const record = await adapter.getRecord(id);
  assert(record, `expected record for stream "${id}"`);
  return record;
}

/** Build an append plan that advances the tail by `texts.length` messages. */
function advancePlan(record: StreamRecord, texts: string[]): AppendPlan {
  const allocation = allocate(record.counter, texts.length);
  return {
    preconditions: { expectedOffset: record.currentOffset, expectedClosed: false },
    messages: texts.map((value, index) => ({
      data: encode(value),
      offset: allocation.offsets[index]!,
      timestamp: 0,
    })),
    recordPatch: { currentOffset: allocation.nextOffset, counter: allocation.endCounter },
  };
}

async function appendTexts(
  adapter: StorageAdapter,
  id: StreamId,
  texts: string[],
): Promise<StorageAppendResult> {
  const record = await currentRecord(adapter, id);
  return adapter.append(id, advancePlan(record, texts));
}

async function createStream(adapter: StorageAdapter, id: StreamId): Promise<void> {
  const result = await adapter.create({ record: newRecord(id) });
  assertStatus(result, "created", `create("${id}")`);
}

/** Bind the adapter's required level-triggered `awaitChange` to a stream id. */
function awaitChangeFor(
  adapter: StorageAdapter,
): (id: StreamId, options: AwaitChangeOptions) => Promise<AwaitChangeResult> {
  return (id, options) => adapter.awaitChange(id, options);
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Let a freshly-started `awaitChange` register its waiter before we mutate. */
function tick(): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, 0));
}

/** Resolves `true` if `promise` is still unsettled after `ms`. */
function staysPending(promise: Promise<unknown>, ms: number): Promise<boolean> {
  return Promise.race([
    promise.then(
      () => false,
      () => false,
    ),
    delay(ms).then(() => true),
  ]);
}

function runAppendContract(
  makeAdapter: MakeStorageAdapter,
  harness: StorageAdapterContractHarness,
) {
  harness.it("append advances the tail and folds close", async () => {
    const adapter = await makeAdapter();
    await createStream(adapter, "s");

    const appended = await appendTexts(adapter, "s", ["a", "b"]);
    assertStatus(appended, "appended", "append");
    assert(appended.status === "appended", "append narrows");
    assert(appended.record.counter === 2, "counter advanced to 2");
    assert(appended.record.currentOffset === formatCounter(2), "offset advanced");
    assert((await adapter.listMessages("s")).length === 2, "two messages listable");

    const record = await currentRecord(adapter, "s");
    const closed = await adapter.append("s", {
      preconditions: { expectedOffset: record.currentOffset, expectedClosed: false },
      messages: [],
      recordPatch: { lifecycle: { closed: true, closedAt: 0 } },
    });
    assertStatus(closed, "appended", "close append");
    assert(
      closed.status === "appended" && closed.record.lifecycle.closed === true,
      "closed folded",
    );
  });

  harness.it(
    "append rejects a stale expectedOffset with reason 'offset' and writes nothing",
    async () => {
      const adapter = await makeAdapter();
      await createStream(adapter, "s");
      await appendTexts(adapter, "s", ["a"]);

      const stale = await adapter.append("s", {
        preconditions: { expectedOffset: ZERO_OFFSET, expectedClosed: false },
        messages: [{ data: encode("b"), offset: formatCounter(2), timestamp: 0 }],
        recordPatch: { currentOffset: formatCounter(2), counter: 2 },
      });
      assertStatus(stale, "precondition-failed", "stale offset");
      assert(stale.status === "precondition-failed" && stale.reason === "offset", "reason offset");
      const record = await currentRecord(adapter, "s");
      assert(record.currentOffset === OFFSET_1 && record.counter === 1, "tail unchanged");
      assert((await adapter.listMessages("s")).length === 1, "no extra message written");
    },
  );

  harness.it(
    "append rejects a violated expectedClosed with reason 'closed' and writes nothing",
    async () => {
      const adapter = await makeAdapter();
      await createStream(adapter, "s");

      const failed = await adapter.append("s", {
        preconditions: { expectedOffset: ZERO_OFFSET, expectedClosed: true },
        messages: [{ data: encode("a"), offset: OFFSET_1, timestamp: 0 }],
        recordPatch: { currentOffset: OFFSET_1, counter: 1 },
      });
      assertStatus(failed, "precondition-failed", "closed precondition");
      assert(
        failed.status === "precondition-failed" && failed.reason === "closed",
        "reason closed",
      );
      const record = await currentRecord(adapter, "s");
      assert(record.currentOffset === ZERO_OFFSET && record.counter === 0, "tail unchanged");
      assert((await adapter.listMessages("s")).length === 0, "no message written");
    },
  );

  harness.it(
    "append rejects a failed producer CAS with reason 'producer' and writes nothing",
    async () => {
      const adapter = await makeAdapter();
      await createStream(adapter, "s");

      // Establish a producer at {epoch:1,lastSeq:0} alongside a real append.
      const seeded = await adapter.append("s", {
        preconditions: {
          expectedOffset: ZERO_OFFSET,
          expectedClosed: false,
          producer: { producerId: "p", expected: undefined, next: { epoch: 1, lastSeq: 0 } },
        },
        messages: [{ data: encode("a"), offset: OFFSET_1, timestamp: 0 }],
        recordPatch: { currentOffset: OFFSET_1, counter: 1 },
      });
      assertStatus(seeded, "appended", "seed producer");

      const record = await currentRecord(adapter, "s");
      // Offset/closed preconditions hold; only the producer CAS is stale.
      const failed = await adapter.append("s", {
        preconditions: {
          expectedOffset: record.currentOffset,
          expectedClosed: false,
          producer: {
            producerId: "p",
            expected: { epoch: 9, lastSeq: 9 },
            next: { epoch: 2, lastSeq: 0 },
          },
        },
        messages: [{ data: encode("b"), offset: formatCounter(2), timestamp: 0 }],
        recordPatch: { currentOffset: formatCounter(2), counter: 2 },
      });
      assertStatus(failed, "precondition-failed", "producer CAS");
      assert(
        failed.status === "precondition-failed" && failed.reason === "producer",
        "reason producer",
      );
      const after = await currentRecord(adapter, "s");
      assert(
        after.currentOffset === OFFSET_1 && after.counter === 1,
        "tail unchanged on CAS failure",
      );
      assert((await adapter.listMessages("s")).length === 1, "no message written on CAS failure");
      const producer = await adapter.getProducerState("s", "p");
      assert(producer?.epoch === 1 && producer.lastSeq === 0, "producer state unchanged");
    },
  );

  harness.it(
    "append rejects an absent-expected producer CAS when the producer already exists",
    async () => {
      const adapter = await makeAdapter();
      await createStream(adapter, "s");

      // Establish the producer at {epoch:1,lastSeq:0}.
      const seeded = await adapter.append("s", {
        preconditions: {
          expectedOffset: ZERO_OFFSET,
          expectedClosed: false,
          producer: { producerId: "p", next: { epoch: 1, lastSeq: 0 } },
        },
        messages: [{ data: encode("a"), offset: OFFSET_1, timestamp: 0 }],
        recordPatch: { currentOffset: OFFSET_1, counter: 1 },
      });
      assertStatus(seeded, "appended", "seed producer");

      // Absent `expected` means "the producer must not exist yet" — NOT "don't
      // check, just set". A second insert-if-absent must fail the whole append.
      const record = await currentRecord(adapter, "s");
      const failed = await adapter.append("s", {
        preconditions: {
          expectedOffset: record.currentOffset,
          expectedClosed: false,
          producer: { producerId: "p", next: { epoch: 2, lastSeq: 0 } },
        },
        messages: [{ data: encode("b"), offset: formatCounter(2), timestamp: 0 }],
        recordPatch: { currentOffset: formatCounter(2), counter: 2 },
      });
      assertStatus(failed, "precondition-failed", "absent-expected CAS on existing producer");
      assert(
        failed.status === "precondition-failed" && failed.reason === "producer",
        "reason producer",
      );
      const producer = await adapter.getProducerState("s", "p");
      assert(producer?.epoch === 1 && producer.lastSeq === 0, "producer state unchanged");
      assert((await adapter.listMessages("s")).length === 1, "no message written");
    },
  );

  harness.it("append accepts a lifecycle-only TTL touch without advancing the tail", async () => {
    const adapter = await makeAdapter();
    await createStream(adapter, "s");
    const before = await currentRecord(adapter, "s");

    const touched = await adapter.append("s", {
      preconditions: { expectedOffset: before.currentOffset, expectedClosed: false },
      recordPatch: { lifecycle: { expiresAtMs: 1_000_000 } },
    });
    assertStatus(touched, "appended", "touch");
    assert(touched.status === "appended", "touch narrows");
    assert(touched.record.currentOffset === before.currentOffset, "touch keeps offset");
    assert(touched.record.counter === before.counter, "touch keeps counter");
    assert(touched.record.lifecycle.expiresAtMs === 1_000_000, "touch renewed deadline");
    assert((await adapter.listMessages("s")).length === 0, "touch wrote no messages");
  });
}

function runReadContract(makeAdapter: MakeStorageAdapter, harness: StorageAdapterContractHarness) {
  harness.it(
    "listMessages windows by lexicographic offset order: exclusive after, inclusive until",
    async () => {
      const adapter = await makeAdapter();
      await createStream(adapter, "s");
      const appended = await appendTexts(adapter, "s", ["a", "b", "c", "d"]);
      assertStatus(appended, "appended", "seed messages");

      const all = await adapter.listMessages("s");
      assert(all.length === 4, "four messages listable");
      // Offsets are fixed-width strings; lexicographic order IS offset order.
      const sorted = all.map((m) => m.offset).toSorted();
      assert(
        all.every((m, i) => m.offset === sorted[i]),
        "messages returned in lexicographic offset order",
      );

      // `after` is EXCLUSIVE: the message at `after` itself is not returned.
      const afterFirst = await adapter.listMessages("s", { after: formatCounter(1) });
      assert(afterFirst.length === 3, "after excludes the boundary message");
      assert(afterFirst[0]!.offset === formatCounter(2), "after starts past the boundary");

      // `until` is INCLUSIVE: the message at `until` is returned.
      const untilThird = await adapter.listMessages("s", { until: formatCounter(3) });
      assert(untilThird.length === 3, "until includes the boundary message");
      assert(untilThird[2]!.offset === formatCounter(3), "until ends at the boundary");

      // Combined window plus limit.
      const window = await adapter.listMessages("s", {
        after: formatCounter(1),
        until: formatCounter(3),
        limit: 1,
      });
      assert(window.length === 1 && window[0]!.offset === formatCounter(2), "window + limit");
    },
  );
}

function runAwaitChangeContract(
  makeAdapter: MakeStorageAdapter,
  harness: StorageAdapterContractHarness,
) {
  harness.it(
    "awaitChange returns 'changed' immediately when the tail already advanced",
    async () => {
      const adapter = await makeAdapter();
      const awaitChange = awaitChangeFor(adapter);
      await createStream(adapter, "s");
      await appendTexts(adapter, "s", ["a"]);

      const result = await awaitChange("s", {
        fromOffset: ZERO_OFFSET,
        observedClosed: false,
        observedSoftDeleted: false,
        timeoutMs: 1000,
      });
      assertStatus(result, "changed", "immediate change");
      assert(result.snapshot.currentOffset === OFFSET_1, "snapshot reflects the new tail");
    },
  );

  harness.it("awaitChange times out with a fresh snapshot when nothing changes", async () => {
    const adapter = await makeAdapter();
    const awaitChange = awaitChangeFor(adapter);
    await createStream(adapter, "s");
    const record = await currentRecord(adapter, "s");

    const result = await awaitChange("s", {
      fromOffset: record.currentOffset,
      observedClosed: false,
      observedSoftDeleted: false,
      timeoutMs: 80,
    });
    assertStatus(result, "timeout", "timeout");
    assert(result.snapshot.present === true, "snapshot present");
    assert(result.snapshot.currentOffset === ZERO_OFFSET, "snapshot at the parked tail");
  });

  harness.it("awaitChange wakes a parked waiter on a committing append", async () => {
    const adapter = await makeAdapter();
    const awaitChange = awaitChangeFor(adapter);
    await createStream(adapter, "s");
    const record = await currentRecord(adapter, "s");

    const pending = awaitChange("s", {
      fromOffset: record.currentOffset,
      observedClosed: false,
      observedSoftDeleted: false,
      timeoutMs: 2000,
    });
    await tick();
    await appendTexts(adapter, "s", ["a"]);

    const result = await pending;
    assertStatus(result, "changed", "wake on append");
    assert(result.snapshot.currentOffset === OFFSET_1, "snapshot reflects the append");
  });

  harness.it("awaitChange wakes a parked waiter on a close transition", async () => {
    const adapter = await makeAdapter();
    const awaitChange = awaitChangeFor(adapter);
    await createStream(adapter, "s");
    const record = await currentRecord(adapter, "s");

    const pending = awaitChange("s", {
      fromOffset: record.currentOffset,
      observedClosed: false,
      observedSoftDeleted: false,
      timeoutMs: 2000,
    });
    await tick();
    const closed = await adapter.append("s", {
      preconditions: { expectedOffset: record.currentOffset, expectedClosed: false },
      messages: [],
      recordPatch: { lifecycle: { closed: true, closedAt: 0 } },
    });
    assertStatus(closed, "appended", "close append");

    const result = await pending;
    assertStatus(result, "changed", "wake on close");
    assert(result.snapshot.closed === true, "snapshot reports closed");
  });

  harness.it("awaitChange wakes a parked waiter on a soft-delete transition", async () => {
    const adapter = await makeAdapter();
    // Soft-delete requires a dependent, and `fork` seeds one. `fork` is
    // capability-by-presence: a forkless adapter cannot reach the soft-delete
    // path (its fork intents are `not-supported`), so this case only applies to
    // fork-capable adapters.
    if (!adapter.fork) return;
    const awaitChange = awaitChangeFor(adapter);
    await createStream(adapter, "parent");
    const forked = await adapter.fork({
      child: newRecord("child", "parent"),
      sourceId: "parent",
      precondition: { sourceLiveAtOffset: ZERO_OFFSET },
    });
    assertStatus(forked, "created", "fork child");
    const record = await currentRecord(adapter, "parent");

    const pending = awaitChange("parent", {
      fromOffset: record.currentOffset,
      observedClosed: false,
      observedSoftDeleted: false,
      timeoutMs: 2000,
    });
    await tick();
    const deleted = await adapter.delete({ streamId: "parent", reason: "delete" });
    assertStatus(deleted, "retained-soft-deleted", "soft delete parent");

    const result = await pending;
    assertStatus(result, "changed", "wake on soft-delete");
    assert(result.snapshot.softDeleted === true, "snapshot reports soft-deleted");
  });

  harness.it("awaitChange wakes a parked waiter on a purge", async () => {
    const adapter = await makeAdapter();
    const awaitChange = awaitChangeFor(adapter);
    await createStream(adapter, "s");
    const record = await currentRecord(adapter, "s");

    const pending = awaitChange("s", {
      fromOffset: record.currentOffset,
      observedClosed: false,
      observedSoftDeleted: false,
      timeoutMs: 2000,
    });
    await tick();
    const deleted = await adapter.delete({ streamId: "s", reason: "delete" });
    assertStatus(deleted, "purged", "purge");

    const result = await pending;
    assertStatus(result, "changed", "wake on purge");
    assert(result.snapshot.present === false, "snapshot reports the record gone");
  });

  harness.it(
    "awaitChange treats an offset regression (purge then re-create) as changed",
    async () => {
      const adapter = await makeAdapter();
      const awaitChange = awaitChangeFor(adapter);
      await createStream(adapter, "s");
      await appendTexts(adapter, "s", ["a", "b"]);
      const parked = await currentRecord(adapter, "s");

      // Purge and re-create BEFORE parking: the entry re-read sees a present
      // record whose offset is LOWER than the parked position. Level-triggered
      // means inequality — not just advance — is "something happened".
      const deleted = await adapter.delete({ streamId: "s", reason: "delete" });
      assertStatus(deleted, "purged", "purge");
      await createStream(adapter, "s");

      const result = await awaitChange("s", {
        fromOffset: parked.currentOffset,
        observedClosed: false,
        observedSoftDeleted: false,
        timeoutMs: 200,
      });
      assertStatus(result, "changed", "offset regression detected");
      assert(result.snapshot.currentOffset === ZERO_OFFSET, "snapshot shows the new incarnation");
    },
  );

  harness.it(
    "awaitChange survives the lost-notify race (a commit before registration)",
    async () => {
      const adapter = await makeAdapter();
      const awaitChange = awaitChangeFor(adapter);
      await createStream(adapter, "s");

      // The advance — and its internal wake — both happen BEFORE awaitChange is
      // even called, so any wake fired here has no waiter to deliver to. The only
      // way to return "changed" is the level-triggered re-read on entry. This is a
      // deterministic edge-trigger guard, not an interleaved-hooks torture test: it
      // proves an edge-only waiter would hang, but does not exercise a wake that
      // races registration mid-flight.
      await appendTexts(adapter, "s", ["a"]);

      const result = await awaitChange("s", {
        fromOffset: ZERO_OFFSET,
        observedClosed: false,
        observedSoftDeleted: false,
        timeoutMs: 200,
      });
      assertStatus(result, "changed", "lost-notify re-check");
      assert(result.snapshot.currentOffset === OFFSET_1, "re-read caught the missed commit");
    },
  );

  harness.it(
    "awaitChange ignores a non-advancing wake and resolves on the real advance",
    async () => {
      const adapter = await makeAdapter();
      const awaitChange = awaitChangeFor(adapter);
      await createStream(adapter, "s");
      const parked = await currentRecord(adapter, "s");

      const pending = awaitChange("s", {
        fromOffset: parked.currentOffset,
        observedClosed: false,
        observedSoftDeleted: false,
        timeoutMs: 4000,
      });
      await tick();

      // A lifecycle-only touch commits (and wakes waiters) without advancing the
      // tail — the parked waiter must re-check, see nothing relevant, and re-park.
      const touched = await adapter.append("s", {
        preconditions: { expectedOffset: parked.currentOffset, expectedClosed: false },
        recordPatch: { lifecycle: { expiresAtMs: 1_000_000 } },
      });
      assertStatus(touched, "appended", "touch");
      assert(await staysPending(pending, 60), "waiter did not resolve on a non-advancing wake");

      await appendTexts(adapter, "s", ["a"]);
      const result = await pending;
      assertStatus(result, "changed", "resolve on real advance");
      assert(result.snapshot.currentOffset === OFFSET_1, "snapshot reflects the advance");
    },
  );
}

function runLifecycleContract(
  makeAdapter: MakeStorageAdapter,
  harness: StorageAdapterContractHarness,
) {
  harness.it("create is idempotent (second create returns 'exists')", async () => {
    const adapter = await makeAdapter();
    const created = await adapter.create({ record: newRecord("s") });
    assertStatus(created, "created", "first create");
    const again = await adapter.create({ record: newRecord("s") });
    assertStatus(again, "exists", "second create");
    assert(again.status === "exists" && again.record.id === "s", "exists returns the record");
  });

  harness.it("create persists a record that arrives already closed", async () => {
    const adapter = await makeAdapter();
    // The record is the single source of truth: core pre-folds a created-closed
    // stream into `lifecycle` before building the plan. Adapters persist as-is.
    const closedRecord: StreamRecord = {
      ...newRecord("s"),
      lifecycle: { closed: true, closedAt: 0 },
    };
    const created = await adapter.create({ record: closedRecord });
    assertStatus(created, "created", "create closed record");
    const record = await currentRecord(adapter, "s");
    assert(record.lifecycle.closed === true, "stream created closed");
    assert(record.lifecycle.closedAt === 0, "closedAt persisted");
  });

  harness.it(
    "fork is capability-by-presence: omitting it is a valid, unsupported configuration",
    async () => {
      const adapter = await makeAdapter();
      // `fork` is optional and its PRESENCE is the capability — there is NO core
      // fork fallback. A forkless adapter is a valid minimal adapter: the
      // protocol surfaces its fork intents (create-with-`forkedFrom`) as the
      // structured `not-supported` result. A fork-capable adapter exposes a
      // callable `fork`, which the cases below then exercise.
      if (adapter.fork === undefined) {
        assert(
          typeof adapter.create === "function" && typeof adapter.delete === "function",
          "a forkless adapter still provides the required create/delete intents",
        );
        return;
      }
      assert(
        typeof adapter.fork === "function",
        "fork capability is a callable method when present",
      );
    },
  );

  harness.it(
    "fork creates a child and is idempotent; a missing source is 'fork-source-gone'",
    async () => {
      const adapter = await makeAdapter();
      // Capability-by-presence: a forkless adapter does not support fork (its
      // protocol fork intents return `not-supported`), so the fork behaviour
      // below only applies when `fork` is implemented.
      if (!adapter.fork) return;
      await createStream(adapter, "parent");

      const forked = await adapter.fork({
        child: newRecord("child", "parent"),
        sourceId: "parent",
        precondition: { sourceLiveAtOffset: ZERO_OFFSET },
      });
      assertStatus(forked, "created", "fork");
      assert((await adapter.getRecord("child")) !== null, "child materialized");

      const again = await adapter.fork({
        child: newRecord("child", "parent"),
        sourceId: "parent",
        precondition: { sourceLiveAtOffset: ZERO_OFFSET },
      });
      assertStatus(again, "exists", "idempotent fork");
      // `exists` carries the existing child record so core can run the same
      // config-match idempotency as create (racing identical forks converge).
      assert(
        again.status === "exists" && again.record.id === "child",
        "fork exists returns the existing child record",
      );
      assert(
        again.status === "exists" && again.record.lifecycle.forkedFrom === "parent",
        "existing child record carries its lineage",
      );

      const gone = await adapter.fork({
        child: newRecord("orphan", "missing"),
        sourceId: "missing",
        precondition: { sourceLiveAtOffset: ZERO_OFFSET },
      });
      assertStatus(gone, "fork-source-gone", "missing source");
    },
  );

  harness.it("delete purges a stream with no dependents", async () => {
    const adapter = await makeAdapter();
    await createStream(adapter, "s");
    const deleted = await adapter.delete({ streamId: "s", reason: "delete" });
    assertStatus(deleted, "purged", "purge");
    assert((await adapter.getRecord("s")) === null, "record gone after purge");
  });

  harness.it("delete is 'not-found' for an absent stream", async () => {
    const adapter = await makeAdapter();
    const deleted = await adapter.delete({ streamId: "missing", reason: "delete" });
    assertStatus(deleted, "not-found", "not-found");
  });

  harness.it("delete soft-deletes an ancestor with dependents, then cascade-purges", async () => {
    const adapter = await makeAdapter();
    // Cascade soft-delete is reachable only via a fork-seeded dependent;
    // capability-by-presence means a forkless adapter never enters this path.
    if (!adapter.fork) return;
    await createStream(adapter, "parent");
    const forked = await adapter.fork({
      child: newRecord("child", "parent"),
      sourceId: "parent",
      precondition: { sourceLiveAtOffset: ZERO_OFFSET },
    });
    assertStatus(forked, "created", "fork child");

    const retained = await adapter.delete({ streamId: "parent", reason: "delete" });
    assertStatus(retained, "retained-soft-deleted", "soft delete parent");
    assert(
      (await adapter.getRecord("parent"))?.lifecycle.softDeleted === true,
      "parent soft-deleted",
    );

    // Deleting the soft-deleted ancestor again with reason "delete" is "gone".
    const gone = await adapter.delete({ streamId: "parent", reason: "delete" });
    assertStatus(gone, "gone", "re-delete soft-deleted ancestor");

    const purged = await adapter.delete({ streamId: "child", reason: "delete" });
    assertStatus(purged, "purged", "purge child");
    assert((await adapter.getRecord("child")) === null, "child gone");
    assert((await adapter.getRecord("parent")) === null, "parent cascade-purged");
  });
}

function runCancellationContract(
  makeAdapter: MakeStorageAdapter,
  harness: StorageAdapterContractHarness,
) {
  harness.it(
    "raceAbortAwaitChange resolves timeout-shaped on caller abort without rejecting",
    async () => {
      const adapter = await makeAdapter();
      const awaitChange = awaitChangeFor(adapter);
      await createStream(adapter, "s");
      const record = await currentRecord(adapter, "s");
      const fallback = buildChangeSnapshot(record);

      const controller = new AbortController();
      // The underlying adapter wait is never told about the signal; it settles by
      // its own (short) timeout. The race resolves immediately on abort.
      const wait = awaitChange("s", {
        fromOffset: record.currentOffset,
        observedClosed: false,
        observedSoftDeleted: false,
        timeoutMs: 150,
      });
      const raced = raceAbortAwaitChange(wait, fallback, controller.signal);
      controller.abort();

      const result = await raced;
      assertStatus(result, "timeout", "aborted race");
      assert(result.snapshot === fallback, "abort surfaces the caller's fallback snapshot");
      // Drain the underlying wait so it cannot leak across into another case.
      await wait;
    },
  );
}

/**
 * Register the full storage-adapter contract against `makeAdapter` using the
 * provided test harness. Wrap in the caller's own `describe` for grouping:
 *
 * ```ts
 * import { it } from "vitest";
 * describe("memory adapter contract", () => {
 *   runStorageAdapterContract(() => createMemoryStorageAdapter(), { it });
 * });
 * ```
 */
export function runStorageAdapterContract(
  makeAdapter: MakeStorageAdapter,
  harness: StorageAdapterContractHarness,
): void {
  // Every case runs through the serializing wrapper (see `withSerializedSeam`),
  // so the seam's structured-clone invariant is enforced mechanically.
  const make = serialized(makeAdapter);
  runAppendContract(make, harness);
  runReadContract(make, harness);
  runAwaitChangeContract(make, harness);
  runLifecycleContract(make, harness);
  runCancellationContract(make, harness);
}
