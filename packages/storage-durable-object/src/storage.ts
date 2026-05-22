import { DurableObject } from "cloudflare:workers";
import { StreamProtocol } from "@streamsy/core";
import type {
  ListMessagesOptions,
  ProducerState,
  StoredMessage,
  StreamEventType,
  StreamRecord,
  StreamRecordPatch,
  StreamStoreAdapter,
  WaitForEventOptions,
  WaitForEventResult,
} from "@streamsy/core";

const LONG_POLL_TIMEOUT_MS = 1500;

export interface DurableObjectStreamStoreEnv {
  STREAM_DO: DurableObjectNamespace<DurableObjectStreamStorage>;
}

/**
 * Namespace-level adapter used by core. Routes each operation to the stream's
 * Durable Object.
 *
 * Atomicity model:
 * - For single-stream mutations (create with initial data, append, close,
 *   producer-id state) the adapter serializes the protocol's read-modify-write
 *   sequence by routing `withLock` to a per-DO in-memory lock chain (see acquire/release on the
 *   DO). Because each lock key is per-stream, the lock targets one DO and
 *   leverages the DO's single-threaded RPC dispatch.
 * - Multi-stream operations (fork creation, purge cascade) are NOT atomic
 *   across DOs. The `transaction` capability is intentionally not provided.
 *   Failures between parent/child updates can leave the fork graph in an
 *   inconsistent state; this is documented and accepted for this pass.
 */
export class DurableObjectStreamStoreAdapter implements StreamStoreAdapter {
  constructor(private namespace: DurableObjectNamespace<DurableObjectStreamStorage>) {}

  private stub(streamId: string): DurableObjectStub<DurableObjectStreamStorage> {
    return this.namespace.get(this.namespace.idFromName(streamId));
  }

  /** Lock keys are `stream:<streamId>` or `producer:<streamId>:<producerId>`. */
  private streamIdFromLockKey(key: string): string {
    if (key.startsWith("stream:")) return key.slice("stream:".length);
    if (key.startsWith("producer:")) {
      const rest = key.slice("producer:".length);
      const lastColon = rest.lastIndexOf(":");
      return lastColon >= 0 ? rest.slice(0, lastColon) : rest;
    }
    throw new Error(`Unrecognized lock key: ${key}`);
  }

  get(streamId: string) {
    return this.stub(streamId).get(streamId);
  }
  create(record: StreamRecord) {
    return this.stub(record.id).create(record);
  }
  update(streamId: string, patch: StreamRecordPatch) {
    return this.stub(streamId).update(streamId, patch);
  }
  delete(streamId: string) {
    return this.stub(streamId).delete(streamId);
  }
  append(streamId: string, messages: StoredMessage[]) {
    return this.stub(streamId).append(streamId, messages);
  }
  list(streamId: string, options?: ListMessagesOptions) {
    return this.stub(streamId).list(streamId, options);
  }
  deleteMessages(streamId: string) {
    return this.stub(streamId).deleteMessages(streamId);
  }
  getProducerState(streamId: string, producerId: string) {
    return this.stub(streamId).getProducerState(streamId, producerId);
  }
  setProducerState(streamId: string, producerId: string, state: ProducerState) {
    return this.stub(streamId).setProducerState(streamId, producerId, state);
  }
  deleteProducerStates(streamId: string) {
    return this.stub(streamId).deleteProducerStates(streamId);
  }
  incrementChildRefCount(parentId: string) {
    return this.stub(parentId).incrementChildRefCount(parentId);
  }
  decrementChildRefCount(parentId: string) {
    return this.stub(parentId).decrementChildRefCount(parentId);
  }
  waitForEvent(streamId: string, options: WaitForEventOptions) {
    return this.stub(streamId).waitForEvent(streamId, options);
  }
  notify(streamId: string, type: StreamEventType) {
    return this.stub(streamId).notify(streamId, type);
  }
  scheduleExpiry(streamId: string, at: number) {
    return this.stub(streamId).scheduleExpiry(streamId, at);
  }
  cancelExpiry(streamId: string) {
    return this.stub(streamId).cancelExpiry(streamId);
  }

  async withLock<T>(key: string, fn: () => Promise<T>): Promise<T> {
    const streamId = this.streamIdFromLockKey(key);
    const stub = this.stub(streamId);
    const token = await stub.acquireLock(key);
    try {
      return await fn();
    } finally {
      await stub.releaseLock(key, token);
    }
  }
}

/** Per-stream Durable Object: durable fact storage plus DO runtime capabilities. */
export class DurableObjectStreamStorage
  extends DurableObject<DurableObjectStreamStoreEnv>
  implements StreamStoreAdapter
{
  private waiters = new Set<(result: WaitForEventResult) => void>();
  private lockChain = new Map<string, Promise<void>>();
  private lockReleasers = new Map<string, () => void>();

  override async alarm(): Promise<void> {
    const record = await this.getOwnRecord();
    if (!record) return;
    if (!this.env.STREAM_DO) {
      // Required for fork-aware GC. Worker bindings must expose STREAM_DO.
      throw new Error("DurableObjectStreamStorage.alarm requires env.STREAM_DO binding");
    }
    const protocol = new StreamProtocol(new DurableObjectStreamStoreAdapter(this.env.STREAM_DO));
    await protocol.handleScheduledExpiry(record.id);
  }

  async get(_streamId: string): Promise<StreamRecord | null> {
    return this.getOwnRecord();
  }

  async create(record: StreamRecord) {
    const existing = await this.getOwnRecord();
    if (existing) return { status: "exists" as const, record: existing };
    await this.ctx.storage.kv.put("record", record);
    return { status: "created" as const };
  }

  async update(_streamId: string, patch: StreamRecordPatch): Promise<StreamRecord> {
    const existing = await this.mustRecord();
    const updated: StreamRecord = {
      ...existing,
      config: { ...existing.config, ...patch.config },
      lifecycle: { ...existing.lifecycle, ...patch.lifecycle },
      currentOffset: patch.currentOffset ?? existing.currentOffset,
      counter: patch.counter ?? existing.counter,
    };
    this.ctx.storage.kv.put("record", updated);
    return updated;
  }

  async delete(_streamId: string): Promise<void> {
    await this.ctx.storage.deleteAll();
  }

  async append(_streamId: string, messages: StoredMessage[]): Promise<void> {
    for (const msg of messages) this.ctx.storage.kv.put(`message:${msg.offset}`, msg);
  }

  async list(_streamId: string, options: ListMessagesOptions = {}): Promise<StoredMessage[]> {
    const listOptions: DurableObjectListOptions = { prefix: "message:" };
    if (options.after) listOptions.startAfter = `message:${options.after}`;
    const entries = this.ctx.storage.kv.list<StoredMessage>(listOptions);
    const messages: StoredMessage[] = [];
    for (const [, value] of entries) {
      if (options.until && value.offset > options.until) break;
      messages.push(value);
      if (options.limit !== undefined && messages.length >= options.limit) break;
    }
    return messages;
  }

  async deleteMessages(_streamId: string): Promise<void> {
    const entries = this.ctx.storage.kv.list({ prefix: "message:" });
    for (const [key] of entries) this.ctx.storage.kv.delete(key);
  }

  async getProducerState(
    _streamId: string,
    producerId: string,
  ): Promise<ProducerState | undefined> {
    return this.ctx.storage.kv.get<ProducerState>(`producer:${producerId}`);
  }

  async setProducerState(
    _streamId: string,
    producerId: string,
    state: ProducerState,
  ): Promise<void> {
    this.ctx.storage.kv.put(`producer:${producerId}`, state);
  }

  async deleteProducerStates(_streamId: string): Promise<void> {
    const entries = this.ctx.storage.kv.list({ prefix: "producer:" });
    for (const [key] of entries) this.ctx.storage.kv.delete(key);
  }

  async incrementChildRefCount(streamId: string): Promise<number> {
    const record = await this.mustRecord();
    const next = record.lifecycle.childRefCount + 1;
    await this.update(streamId, { lifecycle: { childRefCount: next } });
    return next;
  }

  async decrementChildRefCount(streamId: string): Promise<number> {
    const record = await this.mustRecord();
    const next = Math.max(0, record.lifecycle.childRefCount - 1);
    await this.update(streamId, { lifecycle: { childRefCount: next } });
    return next;
  }

  async waitForEvent(_streamId: string, options: WaitForEventOptions): Promise<WaitForEventResult> {
    return new Promise((resolve) => {
      const timeout = setTimeout(
        () => finish({ status: "timeout" }),
        Math.min(options.timeoutMs, LONG_POLL_TIMEOUT_MS),
      );
      const finish = (result: WaitForEventResult) => {
        clearTimeout(timeout);
        this.waiters.delete(finish);
        resolve(result);
      };
      this.waiters.add(finish);
      options.signal?.addEventListener("abort", () => finish({ status: "aborted" }), {
        once: true,
      });
    });
  }

  notify(_streamId: string, type: StreamEventType): void {
    const waiters = [...this.waiters];
    this.waiters.clear();
    for (const waiter of waiters) waiter({ status: "notified", type });
  }

  async scheduleExpiry(_streamId: string, at: number): Promise<void> {
    await this.ctx.storage.setAlarm(at);
  }
  async cancelExpiry(_streamId: string): Promise<void> {
    await this.ctx.storage.deleteAlarm();
  }

  /**
   * Acquire an in-DO lock. The DO is single-threaded so the lock chain Map is
   * safe to mutate from concurrent RPC calls. Returns a token used to release.
   */
  async acquireLock(key: string): Promise<string> {
    while (this.lockChain.has(key)) await this.lockChain.get(key);
    const token = `${key}:${Date.now()}:${Math.random().toString(36).slice(2)}`;
    let release!: () => void;
    this.lockChain.set(key, new Promise<void>((resolve) => (release = resolve)));
    this.lockReleasers.set(token, release);
    return token;
  }

  async releaseLock(key: string, token: string): Promise<void> {
    const release = this.lockReleasers.get(token);
    if (!release) return;
    this.lockReleasers.delete(token);
    this.lockChain.delete(key);
    release();
  }

  private async getOwnRecord(): Promise<StreamRecord | null> {
    return this.ctx.storage.kv.get<StreamRecord>("record") ?? null;
  }
  private async mustRecord(): Promise<StreamRecord> {
    const record = await this.getOwnRecord();
    if (!record) throw new Error("Stream not found");
    return record;
  }
}
