import type { ProducerState } from "@streamsy/core";
import { clone } from "./clone.ts";
import type { MemoryRecordStore } from "./record-store.ts";

export class MemoryProducerStore {
  private readonly producers = new Map<string, ProducerState>();

  constructor(private readonly records: MemoryRecordStore) {}

  async getProducerState(producerId: string): Promise<ProducerState | undefined> {
    return clone(this.producers.get(producerId));
  }

  async setProducerState(producerId: string, producerState: ProducerState): Promise<void> {
    this.records.requireRecord();
    this.producers.set(producerId, clone(producerState));
  }

  async deleteProducerStates(): Promise<void> {
    this.producers.clear();
  }
}
