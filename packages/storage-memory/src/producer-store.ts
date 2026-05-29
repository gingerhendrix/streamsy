import type { ProducerState } from "@streamsy/core";
import type { MemoryStream } from "./stream.ts";
import { clone } from "./memory-entry.ts";

export class MemoryProducerStore {
  constructor(private readonly stream: MemoryStream) {}

  async getProducerState(producerId: string): Promise<ProducerState | undefined> {
    return clone(this.stream.entry?.producers.get(producerId));
  }

  async setProducerState(producerId: string, producerState: ProducerState): Promise<void> {
    this.stream.mustEntry().producers.set(producerId, clone(producerState));
  }

  async deleteProducerStates(): Promise<void> {
    this.stream.entry?.producers.clear();
  }
}
