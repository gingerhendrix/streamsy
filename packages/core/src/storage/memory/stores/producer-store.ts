import type { ProducerState } from "../../../types/storage.ts";
import { clone } from "../lib/clone.ts";
import type { RecordStore } from "./record-store.ts";

export class ProducerStore {
  private readonly producers = new Map<string, ProducerState>();

  constructor(private readonly records: RecordStore) {}

  getProducerState(producerId: string): ProducerState | undefined {
    return clone(this.producers.get(producerId));
  }

  setProducerState(producerId: string, producerState: ProducerState): void {
    this.records.requireRecord();
    this.producers.set(producerId, clone(producerState));
  }

  deleteProducerStates(): void {
    this.producers.clear();
  }
}
