import type { ProducerState } from "@streamsy/core";
import { PRODUCER_PREFIX, producerKey } from "../lib/keys.ts";
import type { RecordStore } from "./record-store.ts";

type DurableObjectKv = DurableObjectStorage["kv"];

export class ProducerStore {
  constructor(
    private readonly records: RecordStore,
    private readonly kv: DurableObjectKv,
  ) {}

  async getProducerState(producerId: string): Promise<ProducerState | undefined> {
    return this.kv.get<ProducerState>(producerKey(producerId));
  }

  async setProducerState(producerId: string, state: ProducerState): Promise<void> {
    await this.records.requireRecord();
    this.kv.put(producerKey(producerId), state);
  }

  async deleteProducerStates(): Promise<void> {
    const entries = this.kv.list({ prefix: PRODUCER_PREFIX });
    for (const [key] of entries) this.kv.delete(key);
  }
}
