import type { LineageStore } from "../strategies/index.ts";
import type { StreamId, StreamRecord } from "../../types/storage.ts";
import type { MemoryStreamState } from "./state.ts";

export class MemoryLineageStore implements LineageStore {
  constructor(private readonly state: MemoryStreamState) {}

  async getRecord(id: StreamId): Promise<StreamRecord | null> {
    return this.state.getExistingStream(id)?.getRecord() ?? null;
  }

  async purgeSelf(id: StreamId): Promise<void> {
    this.state.getExistingStream(id)?.purgeSelf();
  }

  async softDelete(id: StreamId): Promise<void> {
    this.state.getExistingStream(id)?.softDelete();
  }

  async addEdge(parent: StreamId, child: StreamId): Promise<void> {
    this.state.addEdge(parent, child);
  }

  async dropEdge(parent: StreamId, child: StreamId): Promise<void> {
    this.state.dropEdge(parent, child);
  }

  async countDependents(parent: StreamId): Promise<number> {
    return this.state.countDependents(parent);
  }
}
