import type {
  CreateStreamRecordResult,
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

/**
 * Shared in-memory backing tables and process-local runtime state.
 *
 * `MemoryStreamState` is intentionally not the protocol-facing adapter. It is
 * just the owner of backing maps; `stream(id)` returns a stream-oriented handle
 * that is composed from simple bound stores.
 */
export class MemoryStreamState {
  private entries = new Map<string, MemoryEntry>();
  private waiters = new Map<string, Set<(result: WaitForEventResult) => void>>();
  private timers = new Map<string, ReturnType<typeof setTimeout>>();
  private locks = new Map<string, Promise<void>>();

  stream(id: string): MemoryStreamStores {
    return new MemoryStreamStores(id, this);
  }

  getEntry(id: string): MemoryEntry | undefined {
    return this.entries.get(id);
  }

  mustEntry(id: string): MemoryEntry {
    const entry = this.entries.get(id);
    if (!entry) throw new Error(`Stream not found: ${id}`);
    return entry;
  }

  setEntry(record: StreamRecord): CreateStreamRecordResult {
    const existing = this.entries.get(record.id);
    if (existing) return { status: "exists", record: clone(existing.record) };
    this.entries.set(record.id, { record: clone(record), messages: [], producers: new Map() });
    return { status: "created" };
  }

  deleteEntry(id: string): void {
    this.entries.delete(id);
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

  waitForEvent(id: string, options: WaitForEventOptions): Promise<WaitForEventResult> {
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
}

/** Bound, stream-oriented collection of simple memory stores. */
export class MemoryStreamStores {
  readonly records: MemoryRecordStore;
  readonly messages: MemoryMessageStore;
  readonly producers: MemoryProducerStore;
  readonly references: MemoryReferenceStore;
  readonly mutations: MemoryMutationCoordinator;
  readonly events: MemoryEventHub;
  readonly expiry: MemoryExpiryScheduler;

  constructor(
    readonly id: string,
    state: MemoryStreamState,
  ) {
    this.records = new MemoryRecordStore(id, state);
    this.messages = new MemoryMessageStore(id, state);
    this.producers = new MemoryProducerStore(id, state);
    this.references = new MemoryReferenceStore(id, state);
    this.mutations = new MemoryMutationCoordinator(id, state);
    this.events = new MemoryEventHub(id, state);
    this.expiry = new MemoryExpiryScheduler(id, state);
  }
}

export class MemoryRecordStore {
  constructor(
    private readonly id: string,
    private readonly state: MemoryStreamState,
  ) {}

  async getRecord(): Promise<StreamRecord | null> {
    return clone(this.state.getEntry(this.id)?.record ?? null);
  }

  async createRecord(record: StreamRecord): Promise<CreateStreamRecordResult> {
    if (record.id !== this.id) {
      throw new Error(`Record id ${record.id} does not match bound stream ${this.id}`);
    }
    return this.state.setEntry(record);
  }

  async updateRecord(patch: StreamRecordPatch): Promise<StreamRecord> {
    const entry = this.state.mustEntry(this.id);
    entry.record = {
      ...entry.record,
      config: { ...entry.record.config, ...patch.config },
      lifecycle: { ...entry.record.lifecycle, ...patch.lifecycle },
      currentOffset: patch.currentOffset ?? entry.record.currentOffset,
      counter: patch.counter ?? entry.record.counter,
    };
    return clone(entry.record);
  }

  async deleteRecord(): Promise<void> {
    this.state.deleteEntry(this.id);
    await this.state.cancelExpiry(this.id);
  }
}

export class MemoryMessageStore {
  constructor(
    private readonly id: string,
    private readonly state: MemoryStreamState,
  ) {}

  async appendMessages(messages: StoredMessage[]): Promise<void> {
    this.state.mustEntry(this.id).messages.push(...clone(messages));
  }

  async listMessages(options: ListMessagesOptions = {}): Promise<StoredMessage[]> {
    const messages = this.state.getEntry(this.id)?.messages ?? [];
    let out = messages;
    if (options.after) out = out.filter((m) => m.offset > options.after!);
    if (options.until) out = out.filter((m) => m.offset <= options.until!);
    if (options.limit !== undefined) out = out.slice(0, options.limit);
    return clone(out);
  }

  async deleteMessages(): Promise<void> {
    const entry = this.state.getEntry(this.id);
    if (entry) entry.messages = [];
  }
}

export class MemoryProducerStore {
  constructor(
    private readonly id: string,
    private readonly state: MemoryStreamState,
  ) {}

  async getProducerState(producerId: string): Promise<ProducerState | undefined> {
    return clone(this.state.getEntry(this.id)?.producers.get(producerId));
  }

  async setProducerState(producerId: string, producerState: ProducerState): Promise<void> {
    this.state.mustEntry(this.id).producers.set(producerId, clone(producerState));
  }

  async deleteProducerStates(): Promise<void> {
    this.state.getEntry(this.id)?.producers.clear();
  }
}

export class MemoryReferenceStore {
  constructor(
    private readonly id: string,
    private readonly state: MemoryStreamState,
  ) {}

  async incrementChildRefCount(): Promise<number> {
    const entry = this.state.mustEntry(this.id);
    const next = entry.record.lifecycle.childRefCount + 1;
    entry.record = {
      ...entry.record,
      lifecycle: { ...entry.record.lifecycle, childRefCount: next },
    };
    return next;
  }

  async decrementChildRefCount(): Promise<number> {
    const entry = this.state.mustEntry(this.id);
    const next = Math.max(0, entry.record.lifecycle.childRefCount - 1);
    entry.record = {
      ...entry.record,
      lifecycle: { ...entry.record.lifecycle, childRefCount: next },
    };
    return next;
  }
}

export class MemoryMutationCoordinator {
  constructor(
    private readonly id: string,
    private readonly state: MemoryStreamState,
  ) {}

  withMutationLock<T>(fn: () => Promise<T>): Promise<T> {
    return this.state.withLock(`stream:${this.id}`, fn);
  }
}

export class MemoryEventHub {
  constructor(
    private readonly id: string,
    private readonly state: MemoryStreamState,
  ) {}

  waitForEvent(options: WaitForEventOptions): Promise<WaitForEventResult> {
    return this.state.waitForEvent(this.id, options);
  }

  notify(type: StreamEventType): void {
    this.state.notify(this.id, type);
  }
}

export class MemoryExpiryScheduler {
  constructor(
    private readonly id: string,
    private readonly state: MemoryStreamState,
  ) {}

  scheduleExpiry(at: number, callback?: () => Promise<void>): void {
    this.state.scheduleExpiry(this.id, at, callback);
  }

  cancelExpiry(): Promise<void> {
    return this.state.cancelExpiry(this.id);
  }
}

function clone<T>(value: T): T {
  if (value === null || value === undefined) return value;
  return structuredClone(value);
}
