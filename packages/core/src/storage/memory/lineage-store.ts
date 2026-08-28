import type { LineageStore } from "../strategies/index.ts";
import type { StreamId, StreamRecord } from "../../types/storage.ts";
import type { MemoryStreamState } from "./state.ts";

export class MemoryLineageStore implements LineageStore {
  constructor(private readonly state: MemoryStreamState) {}

  async getRecord(id: StreamId): Promise<StreamRecord | null> {
    return this.state.getExistingStream(id)?.getRecord() ?? null;
  }

  async purgeSelf(id: StreamId, expectedExpiresAtMs?: number): Promise<boolean> {
    const stream = this.state.getExistingStream(id);
    const record = stream?.getRecord();
    if (
      !stream ||
      !record ||
      record.lifecycle.expiresAtMs !== (expectedExpiresAtMs ?? record.lifecycle.expiresAtMs)
    )
      return false;
    stream.purgeSelf();
    return true;
  }

  async softDelete(id: StreamId, expectedExpiresAtMs?: number): Promise<boolean> {
    const stream = this.state.getExistingStream(id);
    const record = stream?.getRecord();
    if (
      !stream ||
      !record ||
      record.lifecycle.expiresAtMs !== (expectedExpiresAtMs ?? record.lifecycle.expiresAtMs)
    )
      return false;
    stream.softDelete();
    return true;
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
