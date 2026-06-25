import type { ProducerState } from "../../../types/storage.ts";
import { clone } from "../lib/clone.ts";
import type { RecordStore } from "./record-store.ts";

export class ProducerStore {
  private readonly producers = new Map<string, ProducerState>();

  constructor(private readonly records: RecordStore) {}

  async getProducerState(producerId: string): Promise<ProducerState | undefined> {
    return this.getProducerStateSync(producerId);
  }

  getProducerStateSync(producerId: string): ProducerState | undefined {
    return clone(this.producers.get(producerId));
  }

  setProducerStateSync(producerId: string, producerState: ProducerState): void {
    this.records.requireRecord();
    this.producers.set(producerId, clone(producerState));
  }

  deleteProducerStatesSync(): void {
    this.producers.clear();
  }
}
