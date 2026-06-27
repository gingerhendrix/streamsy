import { DurableObject } from "cloudflare:workers";
import { StreamProtocol } from "@streamsy/core";
import { createDurableObjectStreamFactory } from "./factory.ts";
import {
  CHILD_PREFIX,
  RECORD_KEY,
  STREAM_ID_KEY,
  childKey,
  messageKey,
  producerKey,
} from "./lib/keys.ts";
import { MessageStore } from "./stores/message-store.ts";
import { ProducerStore } from "./stores/producer-store.ts";
import { RecordStore } from "./stores/record-store.ts";
import { AlarmScheduler } from "./utils/alarm-scheduler.ts";
import { DurableObjectNotifier } from "./utils/notifier.ts";
import type {
  CommitResult,
  ListMessagesOptions,
  MutationPlan,
  ProducerState,
  StoredMessage,
  StreamEventType,
  StreamId,
  StreamRecord,
  WaitForEventOptions,
  WaitForEventResult,
} from "@streamsy/core";

export interface DurableObjectStreamStoreEnv {
  STREAM_DO: DurableObjectNamespace<DurableObjectStreamStorage>;
}

/** Per-stream Durable Object: durable fact storage plus DO runtime capabilities. */
export class DurableObjectStreamStorage extends DurableObject<DurableObjectStreamStoreEnv> {
  id!: StreamId;

  private readonly records: RecordStore;
  private readonly messages: MessageStore;
  private readonly producers: ProducerStore;
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
    const existing = this.ctx.storage.kv.get<StreamId>(STREAM_ID_KEY);
    if (existing && existing !== streamId) {
      throw new Error(`Durable Object already initialized for stream ${existing}`);
    }
    if (!existing) this.ctx.storage.kv.put(STREAM_ID_KEY, streamId);
    this.id = streamId;
  }

  private async requireStreamId(): Promise<StreamId> {
    if (this.id) return this.id;
    const stored = this.ctx.storage.kv.get<StreamId>(STREAM_ID_KEY);
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

  async getRecord(): Promise<StreamRecord | null> {
    return this.records.getRecord();
  }

  async commit(plan: MutationPlan): Promise<CommitResult> {
    const id = await this.requireStreamId();
    let record = (this.ctx.storage.kv.get<StreamRecord>(RECORD_KEY) ?? null) as StreamRecord | null;

    if (plan.createRecord) {
      if (plan.createRecord.id !== id) {
        throw new Error(`Record id ${plan.createRecord.id} does not match bound stream ${id}`);
      }
      if (record) return { status: "precondition-failed", record };
      this.ctx.storage.kv.put(RECORD_KEY, plan.createRecord);
      record = plan.createRecord;
    }

    if (!record) return { status: "precondition-failed", record: null };
    if (
      plan.preconditions.expectedOffset !== undefined &&
      plan.preconditions.expectedOffset !== record.currentOffset
    )
      return { status: "precondition-failed", record };
    if (
      plan.preconditions.expectedClosed !== undefined &&
      plan.preconditions.expectedClosed !== (record.lifecycle.closed === true)
    )
      return { status: "precondition-failed", record };

    const producer = plan.preconditions.producer;
    if (producer) {
      const current = this.ctx.storage.kv.get<ProducerState>(producerKey(producer.producerId));
      if (!producerStatesEqual(current, producer.expected))
        return { status: "precondition-failed", record };
    }

    if (plan.appendMessages) {
      for (const message of plan.appendMessages) {
        this.ctx.storage.kv.put(messageKey(message.offset), message);
      }
    }

    let updated = record;
    if (plan.recordPatch) {
      updated = {
        ...record,
        config: { ...record.config, ...plan.recordPatch.config },
        lifecycle: { ...record.lifecycle, ...plan.recordPatch.lifecycle },
        currentOffset: plan.recordPatch.currentOffset ?? record.currentOffset,
        counter: plan.recordPatch.counter ?? record.counter,
      };
      this.ctx.storage.kv.put(RECORD_KEY, updated);
    }
    if (producer) this.ctx.storage.kv.put(producerKey(producer.producerId), producer.next);

    return { status: "committed", record: updated };
  }

  async purgeSelf(): Promise<void> {
    await this.records.deleteRecord();
    await this.messages.deleteMessages();
    await this.producers.deleteProducerStates();
    await this.deleteChildEdges();
    await this.alarms.cancel();
  }

  async softDelete(): Promise<void> {
    await this.records.updateRecord({ lifecycle: { softDeleted: true } });
  }

  async listMessages(options?: ListMessagesOptions): Promise<StoredMessage[]> {
    await this.requireStreamId();
    return this.messages.listMessages(options);
  }

  getProducerState(producerId: string): Promise<ProducerState | undefined> {
    return this.producers.getProducerState(producerId);
  }

  async waitForEvent(options: WaitForEventOptions): Promise<WaitForEventResult> {
    await this.requireStreamId();
    return this.notifier.waitForEvent(options);
  }

  notify(type: StreamEventType): void {
    return this.notifier.notify(type);
  }

  scheduleExpiry(at: number): Promise<void> {
    return this.alarms.schedule(at);
  }

  cancelExpiry(): Promise<void> {
    return this.alarms.cancel();
  }

  async addChildEdge(childId: StreamId): Promise<void> {
    this.ctx.storage.kv.put(childKey(childId), true);
  }

  async dropChildEdge(childId: StreamId): Promise<void> {
    this.ctx.storage.kv.delete(childKey(childId));
  }

  async countChildEdges(): Promise<number> {
    let count = 0;
    for (const _ of this.ctx.storage.kv.list({ prefix: CHILD_PREFIX })) count++;
    return count;
  }

  private async deleteChildEdges(): Promise<void> {
    const entries = this.ctx.storage.kv.list({ prefix: CHILD_PREFIX });
    for (const [key] of entries) this.ctx.storage.kv.delete(key);
  }
}

function producerStatesEqual(
  left: ProducerState | undefined,
  right: ProducerState | undefined,
): boolean {
  if (!left || !right) return left === right;
  return left.epoch === right.epoch && left.lastSeq === right.lastSeq;
}
