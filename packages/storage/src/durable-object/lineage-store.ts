import type { LineageStore, StreamId, StreamRecord } from "@streamsy/core";
import type { DurableObjectStreamStorage } from "./storage.ts";

type DurableObjectStreamStub = DurableObjectStub<DurableObjectStreamStorage>;

export class DurableObjectLineageStore implements LineageStore {
  constructor(private readonly namespace: DurableObjectNamespace<DurableObjectStreamStorage>) {}

  async getRecord(id: StreamId): Promise<StreamRecord | null> {
    return this.stub(id).getRecord(id);
  }

  async purgeSelf(id: StreamId, expectedExpiresAtMs?: number): Promise<boolean> {
    return this.stub(id).purgeSelf(id, expectedExpiresAtMs);
  }

  async softDelete(id: StreamId, expectedExpiresAtMs?: number): Promise<boolean> {
    return this.stub(id).softDelete(id, expectedExpiresAtMs);
  }

  async addEdge(parent: StreamId, child: StreamId): Promise<void> {
    await this.stub(parent).addChildEdge(parent, child);
  }

  async dropEdge(parent: StreamId, child: StreamId): Promise<void> {
    await this.stub(parent).dropChildEdge(parent, child);
  }

  async countDependents(parent: StreamId): Promise<number> {
    return this.stub(parent).countChildEdges(parent);
  }

  private stub(streamId: StreamId): DurableObjectStreamStub {
    return this.namespace.get(this.namespace.idFromName(streamId));
  }
}
