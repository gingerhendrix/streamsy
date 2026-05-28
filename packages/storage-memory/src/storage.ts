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

interface MemoryEntry {
  record: StreamRecord;
  messages: StoredMessage[];
  producers: Map<string, ProducerState>;
}

/** Minimal in-memory fact store plus in-process runtime capabilities. */
export class MemoryStreamState {
  private entries = new Map<string, MemoryEntry>();
  private waiters = new Map<string, Set<(result: WaitForEventResult) => void>>();
  private timers = new Map<string, ReturnType<typeof setTimeout>>();
  private locks = new Map<string, Promise<void>>();

  async get(id: string): Promise<StreamRecord | null> {
    return clone(this.entries.get(id)?.record ?? null);
  }

  async create(record: StreamRecord) {
    const existing = this.entries.get(record.id);
    if (existing) return { status: "exists" as const, record: clone(existing.record) };
    this.entries.set(record.id, { record: clone(record), messages: [], producers: new Map() });
    return { status: "created" as const };
  }

  async update(id: string, patch: StreamRecordPatch): Promise<StreamRecord> {
    const entry = this.must(id);
    entry.record = {
      ...entry.record,
      config: { ...entry.record.config, ...patch.config },
      lifecycle: { ...entry.record.lifecycle, ...patch.lifecycle },
      currentOffset: patch.currentOffset ?? entry.record.currentOffset,
      counter: patch.counter ?? entry.record.counter,
    };
    return clone(entry.record);
  }

  async deleteStream(id: string): Promise<void> {
    this.entries.delete(id);
    await this.cancelExpiry(id);
  }

  async appendToStream(id: string, messages: StoredMessage[]): Promise<void> {
    this.must(id).messages.push(...clone(messages));
  }

  async list(id: string, options: ListMessagesOptions = {}): Promise<StoredMessage[]> {
    const messages = this.entries.get(id)?.messages ?? [];
    let out = messages;
    if (options.after) out = out.filter((m) => m.offset > options.after!);
    if (options.until) out = out.filter((m) => m.offset <= options.until!);
    if (options.limit !== undefined) out = out.slice(0, options.limit);
    return clone(out);
  }

  async deleteMessages(id: string): Promise<void> {
    const entry = this.entries.get(id);
    if (entry) entry.messages = [];
  }

  async getProducerState(id: string, producerId: string): Promise<ProducerState | undefined> {
    return clone(this.entries.get(id)?.producers.get(producerId));
  }

  async setProducerState(id: string, producerId: string, state: ProducerState): Promise<void> {
    this.must(id).producers.set(producerId, clone(state));
  }

  async deleteProducerStates(id: string): Promise<void> {
    this.entries.get(id)?.producers.clear();
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

  async waitForEvent(id: string, options: WaitForEventOptions): Promise<WaitForEventResult> {
    return new Promise((resolve) => {
      const timeout = setTimeout(() => finish({ status: "timeout" }), options.timeoutMs);
      const finish = (result: WaitForEventResult) => {
        clearTimeout(timeout);
        this.waiters.get(id)?.delete(finish);
        resolve(result);
      };
      this.waiters.set(id, this.waiters.get(id) ?? new Set());
      this.waiters.get(id)!.add(finish);
      options.signal?.addEventListener("abort", () => finish({ status: "aborted" }), {
        once: true,
      });
    });
  }

  notify(id: string, type: StreamEventType): void {
    const waiters = [...(this.waiters.get(id) ?? [])];
    this.waiters.delete(id);
    for (const waiter of waiters) waiter({ status: "notified", type });
  }

  scheduleExpiry(id: string, at: number, callback?: () => Promise<void>): void {
    void this.cancelExpiry(id);
    if (!callback) return;
    const delay = Math.max(0, at - Date.now());
    this.timers.set(
      id,
      setTimeout(() => void callback(), delay),
    );
  }

  async cancelExpiry(id: string): Promise<void> {
    const timer = this.timers.get(id);
    if (timer) clearTimeout(timer);
    this.timers.delete(id);
  }

  private must(id: string): MemoryEntry {
    const entry = this.entries.get(id);
    if (!entry) throw new Error(`Stream not found: ${id}`);
    return entry;
  }
}

function clone<T>(value: T): T {
  if (value === null || value === undefined) return value;
  return structuredClone(value);
}
