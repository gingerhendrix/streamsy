import type { ProducerState } from "../../../types/storage.ts";
import { clone } from "../lib/clone.ts";
import type { RecordStore } from "./record-store.ts";

export class ProducerStore {
  private readonly producers = new Map<string, ProducerState>();

  constructor(private readonly records: RecordStore) {}

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
