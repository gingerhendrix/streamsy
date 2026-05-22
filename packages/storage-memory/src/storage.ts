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

interface MemoryEntry {
  record: StreamRecord;
  messages: StoredMessage[];
  producers: Map<string, ProducerState>;
}

/** Minimal in-memory fact store plus in-process runtime capabilities. */
export class MemoryStreamStore implements StreamStoreAdapter {
  private entries = new Map<string, MemoryEntry>();
  private waiters = new Map<string, Set<(result: WaitForEventResult) => void>>();
  private timers = new Map<string, ReturnType<typeof setTimeout>>();
  private locks = new Map<string, Promise<void>>();

  async get(streamId: string): Promise<StreamRecord | null> {
    return clone(this.entries.get(streamId)?.record ?? null);
  }

  async create(record: StreamRecord) {
    const existing = this.entries.get(record.id);
    if (existing) return { status: "exists" as const, record: clone(existing.record) };
    this.entries.set(record.id, { record: clone(record), messages: [], producers: new Map() });
    return { status: "created" as const };
  }

  async update(streamId: string, patch: StreamRecordPatch): Promise<StreamRecord> {
    const entry = this.must(streamId);
    entry.record = {
      ...entry.record,
      config: { ...entry.record.config, ...patch.config },
      lifecycle: { ...entry.record.lifecycle, ...patch.lifecycle },
      currentOffset: patch.currentOffset ?? entry.record.currentOffset,
      counter: patch.counter ?? entry.record.counter,
    };
    return clone(entry.record);
  }

  async delete(streamId: string): Promise<void> {
    this.entries.delete(streamId);
    await this.cancelExpiry(streamId);
  }

  async append(streamId: string, messages: StoredMessage[]): Promise<void> {
    this.must(streamId).messages.push(...clone(messages));
  }

  async list(streamId: string, options: ListMessagesOptions = {}): Promise<StoredMessage[]> {
    const messages = this.entries.get(streamId)?.messages ?? [];
    let out = messages;
    if (options.after) out = out.filter((m) => m.offset > options.after!);
    if (options.until) out = out.filter((m) => m.offset <= options.until!);
    if (options.limit !== undefined) out = out.slice(0, options.limit);
    return clone(out);
  }

  async deleteMessages(streamId: string): Promise<void> {
    const entry = this.entries.get(streamId);
    if (entry) entry.messages = [];
  }

  async getProducerState(streamId: string, producerId: string): Promise<ProducerState | undefined> {
    return clone(this.entries.get(streamId)?.producers.get(producerId));
  }

  async setProducerState(
    streamId: string,
    producerId: string,
    state: ProducerState,
  ): Promise<void> {
    this.must(streamId).producers.set(producerId, clone(state));
  }

  async deleteProducerStates(streamId: string): Promise<void> {
    this.entries.get(streamId)?.producers.clear();
  }

  async incrementChildRefCount(parentId: string): Promise<number> {
    const entry = this.must(parentId);
    const next = entry.record.lifecycle.childRefCount + 1;
    entry.record = {
      ...entry.record,
      lifecycle: { ...entry.record.lifecycle, childRefCount: next },
    };
    return next;
  }

  async decrementChildRefCount(parentId: string): Promise<number> {
    const entry = this.must(parentId);
    const next = Math.max(0, entry.record.lifecycle.childRefCount - 1);
    entry.record = {
      ...entry.record,
      lifecycle: { ...entry.record.lifecycle, childRefCount: next },
    };
    return next;
  }

  async withLock<T>(key: string, fn: () => Promise<T>): Promise<T> {
    while (this.locks.has(key)) await this.locks.get(key);
    let release!: () => void;
    this.locks.set(key, new Promise<void>((resolve) => (release = resolve)));
    try {
      return await fn();
    } finally {
      this.locks.delete(key);
      release();
    }
  }

  async waitForEvent(streamId: string, options: WaitForEventOptions): Promise<WaitForEventResult> {
    return new Promise((resolve) => {
      const timeout = setTimeout(() => finish({ status: "timeout" }), options.timeoutMs);
      const finish = (result: WaitForEventResult) => {
        clearTimeout(timeout);
        this.waiters.get(streamId)?.delete(finish);
        resolve(result);
      };
      this.waiters.set(streamId, this.waiters.get(streamId) ?? new Set());
      this.waiters.get(streamId)!.add(finish);
      options.signal?.addEventListener("abort", () => finish({ status: "aborted" }), {
        once: true,
      });
    });
  }

  notify(streamId: string, type: StreamEventType): void {
    const waiters = [...(this.waiters.get(streamId) ?? [])];
    this.waiters.delete(streamId);
    for (const waiter of waiters) waiter({ status: "notified", type });
  }

  scheduleExpiry(streamId: string, at: number, callback?: () => Promise<void>): void {
    void this.cancelExpiry(streamId);
    if (!callback) return;
    const delay = Math.max(0, at - Date.now());
    this.timers.set(
      streamId,
      setTimeout(() => void callback(), delay),
    );
  }

  async cancelExpiry(streamId: string): Promise<void> {
    const timer = this.timers.get(streamId);
    if (timer) clearTimeout(timer);
    this.timers.delete(streamId);
  }

  private must(streamId: string): MemoryEntry {
    const entry = this.entries.get(streamId);
    if (!entry) throw new Error(`Stream not found: ${streamId}`);
    return entry;
  }
}

function clone<T>(value: T): T {
  if (value === null || value === undefined) return value;
  return structuredClone(value);
}
