import { DurableObject } from "cloudflare:workers";
import { StreamProtocol } from "@streamsy/core";
import { createDurableObjectStorageAdapter } from "./adapter.ts";
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
import { runAwaitChangeLoop } from "@streamsy/core";
import { DurableObjectNotifier } from "./utils/notifier.ts";
import type {
  AppendPlan,
  AwaitChangeOptions,
  AwaitChangeResult,
  ListMessagesOptions,
  ProducerState,
  StorageAppendResult,
  StoredMessage,
  StreamId,
  StreamRecord,
  StreamRecordPatch,
} from "@streamsy/core";

export interface DurableObjectStreamStoreEnv {
  STREAM_DO: DurableObjectNamespace<DurableObjectStreamStorage>;
}

type FailureReason = "offset" | "closed" | "producer";

/**
 * Adapter-internal write engine input. Superset of `AppendPlan` with an optional
 * `createRecord` — `create`/`fork` reuse the engine to insert the record, while
 * the public `append` never carries `createRecord`.
 */
interface WritePlan {
  createRecord?: StreamRecord;
  preconditions: AppendPlan["preconditions"];
  messages?: StoredMessage[];
  recordPatch?: StreamRecordPatch;
}

type WriteResult =
  | { status: "committed"; record: StreamRecord }
  | { status: "precondition-failed"; record: StreamRecord | null; reason?: FailureReason };

/**
 * Per-stream Durable Object: durable fact storage plus DO runtime capabilities.
 *
 * Every stream-facing RPC method takes `streamId` as its first argument and is
 * self-initializing — the first call binds the DO to its id (persisted under
 * `STREAM_ID_KEY`), so there is no separate `init` round-trip. This matches the
 * flat `StorageAdapter` seam, whose routing already passes the id to each call.
 */
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

  /**
   * Bind this Durable Object to its stream id on first access (lazy self-init),
   * keyed on the name the DO was routed by. Subsequent calls validate the id.
   */
  private ensureInit(streamId: StreamId): void {
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
    const adapter = createDurableObjectStorageAdapter({ namespace: this.env.STREAM_DO });
    const protocol = new StreamProtocol({ storage: { adapter } });
    await protocol.handleScheduledExpiry(record.id);
  }

  async getRecord(streamId: StreamId): Promise<StreamRecord | null> {
    this.ensureInit(streamId);
    return this.records.getRecord();
  }

  /** Public append intent (no `createRecord`). */
  async append(streamId: StreamId, plan: AppendPlan): Promise<StorageAppendResult> {
    const out = await this.applyMutation(streamId, {
      preconditions: plan.preconditions,
      messages: plan.messages,
      recordPatch: plan.recordPatch,
    });
    if (out.status === "committed") return { status: "appended", record: out.record };
    // `reason` is required on the seam; an unattributable failure (the record
    // was concurrently purged) reports "offset" per the seam contract.
    return { status: "precondition-failed", record: out.record, reason: out.reason ?? "offset" };
  }

  /**
   * Adapter-internal write engine shared by `append` (no `createRecord`) and the
   * `create`/`fork` intents (with `createRecord`). One serialized actor turn is
   * the atomic boundary.
   */
  async applyMutation(streamId: StreamId, plan: WritePlan): Promise<WriteResult> {
    this.ensureInit(streamId);
    let record = (this.ctx.storage.kv.get<StreamRecord>(RECORD_KEY) ?? null) as StreamRecord | null;

    if (plan.createRecord) {
      if (plan.createRecord.id !== streamId) {
        throw new Error(
          `Record id ${plan.createRecord.id} does not match bound stream ${streamId}`,
        );
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
      return { status: "precondition-failed", record, reason: "offset" };
    if (
      plan.preconditions.expectedClosed !== undefined &&
      plan.preconditions.expectedClosed !== (record.lifecycle.closed === true)
    )
      return { status: "precondition-failed", record, reason: "closed" };

    const producer = plan.preconditions.producer;
    if (producer) {
      const current = this.ctx.storage.kv.get<ProducerState>(producerKey(producer.producerId));
      if (!producerStatesEqual(current, producer.expected))
        return { status: "precondition-failed", record, reason: "producer" };
    }

    if (plan.messages) {
      for (const message of plan.messages) {
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

    // Wake DO-local `awaitChange` waiters now that durable facts are visible.
    // Over-waking is safe: a woken waiter re-reads and re-parks if nothing
    // relevant changed.
    this.notifier.wake();
    return { status: "committed", record: updated };
  }

  async purgeSelf(streamId: StreamId): Promise<void> {
    this.ensureInit(streamId);
    await this.records.deleteRecord();
    await this.messages.deleteMessages();
    await this.producers.deleteProducerStates();
    await this.deleteChildEdges();
    await this.alarms.cancel();
    this.notifier.wake();
  }

  async softDelete(streamId: StreamId): Promise<void> {
    this.ensureInit(streamId);
    await this.records.updateRecord({ lifecycle: { softDeleted: true } });
    this.notifier.wake();
  }

  async listMessages(streamId: StreamId, options?: ListMessagesOptions): Promise<StoredMessage[]> {
    this.ensureInit(streamId);
    return this.messages.listMessages(options);
  }

  getProducerState(streamId: StreamId, producerId: string): Promise<ProducerState | undefined> {
    this.ensureInit(streamId);
    return this.producers.getProducerState(producerId);
  }

  /**
   * Level-triggered live wait. Fully serializable in and out — no `AbortSignal`
   * or other non-serializable value crosses the RPC boundary. Re-reads durable
   * state first (so a write that landed between the caller's observation and this
   * call is never missed), then parks on the DO-local wake bus until the state
   * advances or the budget expires. The total budget is capped at the DO
   * long-poll cap, so a single RPC never strands the actor for a longer caller
   * timeout — core re-issues `awaitChange` on its next poll cycle.
   */
  async awaitChange(streamId: StreamId, options: AwaitChangeOptions): Promise<AwaitChangeResult> {
    this.ensureInit(streamId);
    return runAwaitChangeLoop(
      {
        readRecord: () =>
          (this.ctx.storage.kv.get<StreamRecord>(RECORD_KEY) ?? null) as StreamRecord | null,
        waitForWake: (timeoutMs) => this.notifier.waitForWake(timeoutMs),
        totalCapMs: this.notifier.longPollTimeoutMs,
      },
      options,
    );
  }

  scheduleExpiry(streamId: StreamId, at: number): Promise<void> {
    this.ensureInit(streamId);
    return this.alarms.schedule(at);
  }

  cancelExpiry(streamId: StreamId): Promise<void> {
    this.ensureInit(streamId);
    return this.alarms.cancel();
  }

  async addChildEdge(streamId: StreamId, childId: StreamId): Promise<void> {
    this.ensureInit(streamId);
    this.ctx.storage.kv.put(childKey(childId), true);
  }

  async dropChildEdge(streamId: StreamId, childId: StreamId): Promise<void> {
    this.ensureInit(streamId);
    this.ctx.storage.kv.delete(childKey(childId));
  }

  async countChildEdges(streamId: StreamId): Promise<number> {
    this.ensureInit(streamId);
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
