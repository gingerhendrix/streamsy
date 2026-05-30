import { DurableObject } from "cloudflare:workers";
import { StreamProtocol } from "@streamsy/core";
import { createDurableObjectStreamFactory } from "./factory.ts";
import { STREAM_ID_KEY } from "./lib/keys.ts";
import { MessageStore } from "./stores/message-store.ts";
import { ProducerStore } from "./stores/producer-store.ts";
import { RecordStore } from "./stores/record-store.ts";
import { AlarmScheduler } from "./utils/alarm-scheduler.ts";
import { DurableObjectLock } from "./utils/lock.ts";
import { DurableObjectNotifier } from "./utils/notifier.ts";
import type {
  CreateStreamRecordResult,
  ListMessagesOptions,
  ProducerState,
  StoredMessage,
  Stream,
  StreamEventType,
  StreamId,
  StreamRecord,
  StreamRecordPatch,
  WaitForEventOptions,
  WaitForEventResult,
} from "@streamsy/core";

export interface DurableObjectStreamStoreEnv {
  STREAM_DO: DurableObjectNamespace<DurableObjectStreamStorage>;
}

/** Per-stream Durable Object: durable fact storage plus DO runtime capabilities. */
export class DurableObjectStreamStorage
  extends DurableObject<DurableObjectStreamStoreEnv>
  implements Stream
{
  id!: StreamId;

  private readonly records: RecordStore;
  private readonly messages: MessageStore;
  private readonly producers: ProducerStore;
  private readonly lock = new DurableObjectLock();
  private readonly notifier = new DurableObjectNotifier();
  private readonly alarms: AlarmScheduler;

  constructor(ctx: DurableObjectState, env: DurableObjectStreamStoreEnv) {
    super(ctx, env);
    this.records = new RecordStore(() => this.requireStreamId(), ctx.storage.kv);
    this.messages = new MessageStore(this.records, ctx.storage.kv);
    this.producers = new ProducerStore(this.records, ctx.storage.kv);
    this.alarms = new AlarmScheduler(ctx.storage);
  }

  async init(streamId: StreamId): Promise<void> {
    const existing = await this.ctx.storage.kv.get<StreamId>(STREAM_ID_KEY);
    if (existing && existing !== streamId) {
      throw new Error(`Durable Object already initialized for stream ${existing}`);
    }
    if (!existing) await this.ctx.storage.kv.put(STREAM_ID_KEY, streamId);
    this.id = streamId;
  }

  private async requireStreamId(): Promise<StreamId> {
    if (this.id) return this.id;
    const stored = await this.ctx.storage.kv.get<StreamId>(STREAM_ID_KEY);
    if (!stored) throw new Error("Durable Object stream is not initialized");
    this.id = stored;
    return stored;
  }

  override async alarm(): Promise<void> {
    const record = await this.records.getRecord();
    if (!record) return;
    if (!this.env.STREAM_DO) {
      // Required for fork-aware GC. Worker bindings must expose STREAM_DO.
      throw new Error("DurableObjectStreamStorage.alarm requires env.STREAM_DO binding");
    }
    const factory = createDurableObjectStreamFactory({ namespace: this.env.STREAM_DO });
    const protocol = new StreamProtocol({ storage: { factory } });
    await protocol.handleScheduledExpiry(record.id);
  }

  getRecord(): Promise<StreamRecord | null> {
    return this.records.getRecord();
  }

  createRecord(record: StreamRecord): Promise<CreateStreamRecordResult> {
    return this.records.createRecord(record);
  }

  updateRecord(patch: StreamRecordPatch): Promise<StreamRecord> {
    return this.records.updateRecord(patch);
  }

  async deleteRecord(): Promise<void> {
    await this.records.deleteRecord();
    await this.messages.deleteMessages();
    await this.producers.deleteProducerStates();
    await this.alarms.cancel();
  }

  appendMessages(messages: StoredMessage[]): Promise<void> {
    return this.messages.appendMessages(messages);
  }

  listMessages(options?: ListMessagesOptions): Promise<StoredMessage[]> {
    return this.messages.listMessages(options);
  }

  deleteMessages(): Promise<void> {
    return this.messages.deleteMessages();
  }

  getProducerState(producerId: string): Promise<ProducerState | undefined> {
    return this.producers.getProducerState(producerId);
  }

  setProducerState(producerId: string, state: ProducerState): Promise<void> {
    return this.producers.setProducerState(producerId, state);
  }

  deleteProducerStates(): Promise<void> {
    return this.producers.deleteProducerStates();
  }

  incrementChildRefCount(): Promise<number> {
    return this.records.incrementChildRefCount();
  }

  decrementChildRefCount(): Promise<number> {
    return this.records.decrementChildRefCount();
  }

  withMutationLock<T>(fn: () => Promise<T>): Promise<T> {
    // Cloudflare RPC callbacks execute outside this Durable Object and may call
    // back into the same stub. Holding an in-object lock around the callback
    // would deadlock those nested storage calls, so the direct stub surface
    // invokes the callback directly and relies on the Durable Object's
    // per-method single-threaded execution for storage mutations.
    return fn();
  }

  waitForEvent(options: WaitForEventOptions): Promise<WaitForEventResult> {
    return this.notifier.waitForEvent(options);
  }

  notify(type: StreamEventType): void {
    return this.notifier.notify(type);
  }

  scheduleExpiry(at: number, callback?: () => Promise<void>): Promise<void> {
    void callback;
    return this.alarms.schedule(at);
  }

  cancelExpiry(): Promise<void> {
    return this.alarms.cancel();
  }

  async acquireLock(key: string): Promise<string> {
    return this.lock.acquire(key);
  }

  async releaseLock(key: string, token: string): Promise<void> {
    void key;
    return this.lock.release(token);
  }
}
