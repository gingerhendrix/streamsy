import { DurableObject } from "cloudflare:workers";
import { StreamProtocol } from "@streamsy/core";
import { createDurableObjectStreamFactory } from "./factory.ts";
import type {
  ListMessagesOptions,
  ProducerState,
  StoredMessage,
  StreamEventType,
  StreamRecord,
  StreamRecordPatch,
  WaitForEventOptions,
  WaitForEventResult,
} from "@streamsy/core";

const LONG_POLL_TIMEOUT_MS = 1500;

export interface DurableObjectStreamStoreEnv {
  STREAM_DO: DurableObjectNamespace<DurableObjectStreamStorage>;
}

/** Per-stream Durable Object: durable fact storage plus DO runtime capabilities. */
export class DurableObjectStreamStorage extends DurableObject<DurableObjectStreamStoreEnv> {
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
    const factory = createDurableObjectStreamFactory({ namespace: this.env.STREAM_DO });
    const protocol = new StreamProtocol({ storage: { factory } });
    await protocol.handleScheduledExpiry(record.id);
  }

  async get(): Promise<StreamRecord | null> {
    return this.getOwnRecord();
  }

  async create(record: StreamRecord) {
    const existing = await this.getOwnRecord();
    if (existing) return { status: "exists" as const, record: existing };
    await this.ctx.storage.kv.put("record", record);
    return { status: "created" as const };
  }

  async update(patch: StreamRecordPatch): Promise<StreamRecord> {
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

  async deleteStream(): Promise<void> {
    await this.ctx.storage.deleteAll();
  }

  async appendToStream(messages: StoredMessage[]): Promise<void> {
    for (const msg of messages) this.ctx.storage.kv.put(`message:${msg.offset}`, msg);
  }

  async list(options: ListMessagesOptions = {}): Promise<StoredMessage[]> {
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

  async deleteMessages(): Promise<void> {
    const entries = this.ctx.storage.kv.list({ prefix: "message:" });
    for (const [key] of entries) this.ctx.storage.kv.delete(key);
  }

  async getProducerState(producerId: string): Promise<ProducerState | undefined> {
    return this.ctx.storage.kv.get<ProducerState>(`producer:${producerId}`);
  }

  async setProducerState(producerId: string, state: ProducerState): Promise<void> {
    this.ctx.storage.kv.put(`producer:${producerId}`, state);
  }

  async deleteProducerStates(): Promise<void> {
    const entries = this.ctx.storage.kv.list({ prefix: "producer:" });
    for (const [key] of entries) this.ctx.storage.kv.delete(key);
  }

  async incrementChildRefCount(): Promise<number> {
    const record = await this.mustRecord();
    const next = record.lifecycle.childRefCount + 1;
    await this.update({ lifecycle: { childRefCount: next } });
    return next;
  }

  async decrementChildRefCount(): Promise<number> {
    const record = await this.mustRecord();
    const next = Math.max(0, record.lifecycle.childRefCount - 1);
    await this.update({ lifecycle: { childRefCount: next } });
    return next;
  }

  async waitForEvent(options: WaitForEventOptions): Promise<WaitForEventResult> {
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

  notify(type: StreamEventType): void {
    const waiters = [...this.waiters];
    this.waiters.clear();
    for (const waiter of waiters) waiter({ status: "notified", type });
  }

  async scheduleExpiry(at: number): Promise<void> {
    await this.ctx.storage.setAlarm(at);
  }
  async cancelExpiry(): Promise<void> {
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
