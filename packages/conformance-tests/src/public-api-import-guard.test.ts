import { describe, expect, it } from "vitest";
import {
  createHttpHandler,
  createReadOnlyHttpHandler,
  createStreamProtocol,
  ZERO_OFFSET,
  bindStream,
  type ProtocolStream,
  type StorageAdapter,
  type StreamProtocolFactory,
  type BoundStream,
  createMemoryStorageAdapter,
  cascadeReclaim,
  copyOnForkReclaim,
  plainPurge,
  refCountLineage,
  reverseIndexLineage,
  ttlOnlyReclaim,
  compareOffsets,
  runAwaitChangeLoop,
  type AppendPlan,
  type CreatePlan,
  type DeletePlan,
  type ForkPlan,
  type StorageAppendResult,
  type StorageCreateResult,
  type StorageDeleteResult,
  type StorageForkResult,
  type LineagePolicy,
  type LineageStore,
} from "@streamsy/core";
import { createJsonProtocol, JsonProtocol } from "@streamsy/json";
import { createDurableStateProtocol, DurableStateProtocol } from "@streamsy/state";

type DurableObjectApi = typeof import("@streamsy/storage-durable-object");

describe("public API import guard", () => {
  it("exposes protocol factories and the flat storage adapter seam", async () => {
    const adapter: StorageAdapter = createMemoryStorageAdapter();
    const protocol: StreamProtocolFactory = createStreamProtocol({ storage: { adapter } });
    const handler = createHttpHandler({ protocol });
    const readOnlyHandler = createReadOnlyHttpHandler({ protocol });
    const plan: AppendPlan = { preconditions: {}, recordPatch: {} };
    const bound: BoundStream = bindStream(adapter, "guard");
    const createPlan: CreatePlan | undefined = undefined;
    const forkPlan: ForkPlan | undefined = undefined;
    const deletePlan: DeletePlan | undefined = undefined;
    const appendResult: StorageAppendResult | undefined = undefined;
    const createResult: StorageCreateResult | undefined = undefined;
    const forkResult: StorageForkResult | undefined = undefined;
    const deleteResult: StorageDeleteResult | undefined = undefined;
    void appendResult;
    void createResult;
    void forkResult;
    void deleteResult;
    const lineagePolicy: LineagePolicy | undefined = undefined;
    const lineageStore: LineageStore | undefined = undefined;
    void bound;
    void createPlan;
    void forkPlan;
    void deletePlan;
    void lineagePolicy;
    void lineageStore;
    expect(plan.preconditions).toEqual({});
    expect(ZERO_OFFSET).toBe("0000000000000000_0000000000000000");
    expect(handler.fetch).toBeTypeOf("function");
    expect(readOnlyHandler.fetch).toBeTypeOf("function");
    expect(cascadeReclaim).toBeTypeOf("function");
    expect(plainPurge).toBeTypeOf("function");
    expect(refCountLineage).toBeTypeOf("function");
    expect(reverseIndexLineage).toBeTypeOf("function");
    expect(copyOnForkReclaim).toBeTypeOf("function");
    expect(ttlOnlyReclaim).toBeTypeOf("function");
    expect(bindStream).toBeTypeOf("function");
    expect(compareOffsets).toBeTypeOf("function");
    expect(runAwaitChangeLoop).toBeTypeOf("function");
    const created = await protocol.create("guard", {});
    expect(created.status).toBe("created");
    if (created.status === "created") {
      const stream: ProtocolStream = created.stream;
      expect(stream.id).toBe("guard");
    }
    const durableObjectExports = [
      "createDurableObjectStorageAdapter",
      "DurableObjectStreamStorage",
    ] satisfies Array<keyof DurableObjectApi>;
    expect(durableObjectExports).toContain("createDurableObjectStorageAdapter");
    expect(durableObjectExports).toContain("DurableObjectStreamStorage");
    const json = createJsonProtocol(protocol, {
      encode: (value: unknown) => value,
      decode: (value: unknown) => value,
    });
    expect(json).toBeInstanceOf(JsonProtocol);
    const durable = createDurableStateProtocol(protocol, {});
    expect(durable).toBeInstanceOf(DurableStateProtocol);
  });
});
