/**
 * In-memory Durable Object namespace + stub for tests.
 *
 * Mirrors the real `DurableObjectStreamStorage` RPC surface (every stream-facing
 * method takes `streamId` first and self-initializes on first access) so the flat
 * `createDurableObjectStorageAdapter` routing, lineage edges, and `awaitChange`
 * wake bus can be exercised under vitest WITHOUT the `cloudflare:workers` runtime.
 *
 * This is an adapter-routing harness, not workerd: it does NOT exercise real RPC
 * structured-clone serialization. Real-DO conformance (including the original
 * `AbortSignal` `DataCloneError` boundary) is covered by the deployed conformance
 * suite and `conformance-tests/src/do.sse-final-append-diagnostic.test.ts`.
 */
import { buildChangeSnapshot, changeSnapshotDiffers } from "@streamsy/core";
import type {
  AppendPlan,
  AwaitChangeOptions,
  AwaitChangeResult,
  ListMessagesOptions,
  ProducerState,
  StoredMessage,
  StreamId,
  StreamRecord,
  StreamRecordPatch,
} from "@streamsy/core";
import type { DurableObjectStorageAdapterOptions } from "../adapter.ts";
import type { DurableObjectStreamStorage } from "../storage.ts";

type FactoryNamespace = DurableObjectStorageAdapterOptions["namespace"];
type FactoryStub = DurableObjectStub<DurableObjectStreamStorage>;

/**
 * The stream-facing RPC surface of the real Durable Object, taken from the real
 * class rather than restated. `FakeStub` declares `implements StreamStorageRpc`,
 * so a parameter or return type that drifts on `DurableObjectStreamStorage`
 * fails this package's typecheck instead of being hidden by the RPC boundary
 * in `asStub` below.
 */
type StreamStorageRpc = Pick<
  DurableObjectStreamStorage,
  | "getRecord"
  | "append"
  | "applyMutation"
  | "listMessages"
  | "getProducerState"
  | "awaitChange"
  | "scheduleExpiry"
  | "cancelExpiry"
  | "purgeSelf"
  | "softDelete"
  | "addChildEdge"
  | "dropChildEdge"
  | "countChildEdges"
>;

type FailureReason = "offset" | "closed" | "producer";

interface WritePlan {
  createRecord?: StreamRecord;
  preconditions: AppendPlan["preconditions"];
  messages?: StoredMessage[];
  recordPatch?: StreamRecordPatch;
}

type WriteResult =
  | { status: "committed"; record: StreamRecord }
  | { status: "precondition-failed"; record: StreamRecord | null; reason?: FailureReason };

interface FakeStubState {
  boundId?: StreamId;
  initCalls: StreamId[];
  record: StreamRecord | null;
  messages: StoredMessage[];
  producers: Map<string, ProducerState>;
  children: Set<StreamId>;
  expiry: { at?: number; cancelled: boolean };
  awaitOptions: AwaitChangeOptions[];
}

class FakeStub implements StreamStorageRpc {
  readonly state: FakeStubState;
  private waiters = new Set<() => void>();

  constructor(state: FakeStubState) {
    this.state = state;
  }

  private ensureInit(streamId: StreamId): void {
    if (this.state.boundId && this.state.boundId !== streamId) {
      throw new Error(`Durable Object already initialized for stream ${this.state.boundId}`);
    }
    if (!this.state.boundId) {
      this.state.boundId = streamId;
      this.state.initCalls.push(streamId);
    }
  }

  async getRecord(streamId: StreamId): Promise<StreamRecord | null> {
    this.ensureInit(streamId);
    return this.state.record;
  }

  async applyMutation(streamId: StreamId, plan: WritePlan): Promise<WriteResult> {
    this.ensureInit(streamId);
    let record = this.state.record;
    if (plan.createRecord) {
      if (plan.createRecord.id !== streamId)
        throw new Error(
          `Record id ${plan.createRecord.id} does not match bound stream ${streamId}`,
        );
      if (record) return { status: "precondition-failed", record };
      this.state.record = plan.createRecord;
      record = plan.createRecord;
    }
    if (!record) return { status: "precondition-failed", record: null };
    if (
      plan.preconditions.expectedOffset !== undefined &&
      plan.preconditions.expectedOffset !== record.currentOffset
    )
      return { status: "precondition-failed", record, reason: "offset" };
    if (
      plan.preconditions.expectedClosed !== undefined &&
      plan.preconditions.expectedClosed !== (record.lifecycle.closed === true)
    )
      return { status: "precondition-failed", record, reason: "closed" };

    const producer = plan.preconditions.producer;
    if (producer) {
      const current = this.state.producers.get(producer.producerId);
      if (!producerStatesEqual(current, producer.expected))
        return { status: "precondition-failed", record, reason: "producer" };
    }

    if (plan.messages) this.state.messages.push(...plan.messages);
    let updated = record;
    if (plan.recordPatch) {
      updated = mergeRecord(record, plan.recordPatch);
      this.state.record = updated;
    }
    if (producer) this.state.producers.set(producer.producerId, producer.next);
    this.wake();
    return { status: "committed", record: updated };
  }

  async append(streamId: StreamId, plan: AppendPlan) {
    const out = await this.applyMutation(streamId, {
      preconditions: plan.preconditions,
      messages: plan.messages,
      recordPatch: plan.recordPatch,
    });
    if (out.status === "committed") return { status: "appended" as const, record: out.record };
    // Mirrors the real class: `reason` is required on the seam, so an
    // unattributable failure reports "offset" per the seam contract.
    return {
      status: "precondition-failed" as const,
      record: out.record,
      reason: out.reason ?? "offset",
    };
  }

  async listMessages(
    streamId: StreamId,
    options: ListMessagesOptions = {},
  ): Promise<StoredMessage[]> {
    this.ensureInit(streamId);
    let out = this.state.messages;
    if (options.after) out = out.filter((m) => m.offset > options.after!);
    if (options.until) out = out.filter((m) => m.offset <= options.until!);
    if (options.limit !== undefined) out = out.slice(0, options.limit);
    return out;
  }

  async getProducerState(
    streamId: StreamId,
    producerId: string,
  ): Promise<ProducerState | undefined> {
    this.ensureInit(streamId);
    return this.state.producers.get(producerId);
  }

  async purgeSelf(streamId: StreamId): Promise<void> {
    this.ensureInit(streamId);
    this.state.record = null;
    this.state.messages = [];
    this.state.producers.clear();
    this.state.children.clear();
    this.state.expiry.cancelled = true;
    this.wake();
  }

  async softDelete(streamId: StreamId): Promise<void> {
    this.ensureInit(streamId);
    if (!this.state.record) throw new Error("Stream not found");
    this.state.record = mergeRecord(this.state.record, { lifecycle: { softDeleted: true } });
    this.wake();
  }

  async addChildEdge(streamId: StreamId, childId: StreamId): Promise<void> {
    this.ensureInit(streamId);
    this.state.children.add(childId);
  }

  async dropChildEdge(streamId: StreamId, childId: StreamId): Promise<void> {
    this.ensureInit(streamId);
    this.state.children.delete(childId);
  }

  async countChildEdges(streamId: StreamId): Promise<number> {
    this.ensureInit(streamId);
    return this.state.children.size;
  }

  async awaitChange(streamId: StreamId, options: AwaitChangeOptions): Promise<AwaitChangeResult> {
    this.ensureInit(streamId);
    this.state.awaitOptions.push(options);
    const start = Date.now();
    while (true) {
      const snapshot = buildChangeSnapshot(this.state.record);
      if (changeSnapshotDiffers(snapshot, options)) return { status: "changed", snapshot };
      const remaining = options.timeoutMs - (Date.now() - start);
      if (remaining <= 0) return { status: "timeout", snapshot };
      await this.waitForWake(remaining);
    }
  }

  private waitForWake(timeoutMs: number): Promise<void> {
    return new Promise<void>((resolve) => {
      const finish = () => {
        clearTimeout(timer);
        this.waiters.delete(finish);
        resolve();
      };
      const timer = setTimeout(finish, timeoutMs);
      this.waiters.add(finish);
    });
  }

  private wake(): void {
    const snapshot = [...this.waiters];
    this.waiters.clear();
    for (const finish of snapshot) finish();
  }

  async scheduleExpiry(streamId: StreamId, at: number): Promise<void> {
    this.ensureInit(streamId);
    this.state.expiry.at = at;
    this.state.expiry.cancelled = false;
  }

  async cancelExpiry(streamId: StreamId): Promise<void> {
    this.ensureInit(streamId);
    this.state.expiry.cancelled = true;
  }
}

/**
 * Complete `DurableObjectId` for the fake. Ids in this harness are the stream id
 * itself, which is what `idFromName` is given and what `get` routes on, so
 * `toString()` round-trips the name and `equals` compares that round-trip.
 */
class FakeDurableObjectId implements DurableObjectId {
  constructor(readonly name: string) {}

  toString(): string {
    return this.name;
  }

  equals(other: DurableObjectId): boolean {
    return other.toString() === this.name;
  }
}

/**
 * The one place the fake crosses into workerd's RPC types.
 *
 * `DurableObjectStub<T>` is `Fetcher` intersected with every method of `T`
 * rewritten to an `Rpc.Result`: a custom thenable that is also a `Provider` for
 * pipelining and resolves to a `Disposable`. `DurableObjectStreamStorage`
 * additionally carries the nominal `__DURABLE_OBJECT_BRAND` of
 * `cloudflare:workers`. Neither is producible by an in-process object, so no
 * fake can be assignable to this type and the conversion is asserted once, here.
 *
 * The assertion covers only that RPC wrapping. The method surface itself is
 * checked: `FakeStub implements StreamStorageRpc`, which is derived from the
 * real class. Structured-clone serialization is out of scope for this harness
 * and is covered by the deployed conformance suite (see the file header).
 */
function asStub(stub: FakeStub): FactoryStub {
  // SAFETY: `FakeStub` implements the real stream RPC surface; this intersection adds only
  // workerd's unavailable Fetcher, Provider, Disposable, and nominal brand types at the test seam.
  return stub as FakeStub & FactoryStub;
}

export interface FakeNamespace {
  namespace: FactoryNamespace;
  stubFor: (name: string) => FakeStub;
  has: (name: string) => boolean;
}

export function createFakeNamespace(): FakeNamespace {
  const stubs = new Map<string, FakeStub>();
  const ids = new Map<string, FakeDurableObjectId>();

  function ensureStub(name: string): FakeStub {
    let stub = stubs.get(name);
    if (!stub) {
      stub = new FakeStub({
        initCalls: [],
        record: null,
        messages: [],
        producers: new Map(),
        children: new Set(),
        expiry: { cancelled: false },
        awaitOptions: [],
      });
      stubs.set(name, stub);
    }
    return stub;
  }

  function ensureId(name: string): FakeDurableObjectId {
    let id = ids.get(name);
    if (!id) {
      id = new FakeDurableObjectId(name);
      ids.set(name, id);
    }
    return id;
  }

  /**
   * Every member of `DurableObjectNamespace` is implemented, so the value is
   * assignable without conversion. The two the adapter never uses raise instead
   * of returning a plausible-looking stub: `newUniqueId` and `jurisdiction` have
   * no meaning for a name-routed in-memory map, and a silent stand-in would let
   * a routing regression pass as a green test.
   */
  const namespace: FactoryNamespace = {
    idFromName: (name) => ensureId(name),
    idFromString: (id) => ensureId(id),
    get: (id) => asStub(ensureStub(id.toString())),
    getByName: (name) => asStub(ensureStub(name)),
    newUniqueId: () => {
      throw new Error("createFakeNamespace: newUniqueId is not supported; route by stream id");
    },
    jurisdiction: () => {
      throw new Error("createFakeNamespace: jurisdiction is not supported");
    },
  };

  return {
    namespace,
    stubFor: (name: string) => ensureStub(name),
    has: (name: string) => stubs.has(name),
  };
}

function mergeRecord(record: StreamRecord, patch: StreamRecordPatch): StreamRecord {
  return {
    ...record,
    config: { ...record.config, ...patch.config },
    lifecycle: { ...record.lifecycle, ...patch.lifecycle },
    currentOffset: patch.currentOffset ?? record.currentOffset,
    counter: patch.counter ?? record.counter,
  };
}

function producerStatesEqual(
  left: ProducerState | undefined,
  right: ProducerState | undefined,
): boolean {
  if (!left || !right) return left === right;
  return left.epoch === right.epoch && left.lastSeq === right.lastSeq;
}
