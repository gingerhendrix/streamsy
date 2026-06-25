import type { LineageStore, StreamId, StreamRecord } from "@streamsy/core";
import type { DurableObjectStreamStorage } from "./storage.ts";

type DurableObjectStreamStub = DurableObjectStub<DurableObjectStreamStorage>;

export class DurableObjectLineageStore implements LineageStore {
  constructor(private readonly namespace: DurableObjectNamespace<DurableObjectStreamStorage>) {}

  async getRecord(id: StreamId): Promise<StreamRecord | null> {
    return this.stub(id).getRecord();
  }

  async purgeSelf(id: StreamId): Promise<void> {
    await this.stub(id).purgeSelf();
  }

  async softDelete(id: StreamId): Promise<void> {
    await this.stub(id).softDelete();
  }

  async addEdge(parent: StreamId, child: StreamId): Promise<void> {
    await this.stub(parent).addChildEdge(child);
  }

  async dropEdge(parent: StreamId, child: StreamId): Promise<void> {
    await this.stub(parent).dropChildEdge(child);
  }

  async countDependents(parent: StreamId): Promise<number> {
    return this.stub(parent).countChildEdges();
  }

  private stub(streamId: StreamId): DurableObjectStreamStub {
    return this.namespace.get(this.namespace.idFromName(streamId));
  }
}
