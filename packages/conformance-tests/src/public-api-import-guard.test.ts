import { describe, expect, it } from "vitest";
import {
  createHttpHandler,
  createReadOnlyHttpHandler,
  createStreamProtocol,
  ZERO_OFFSET,
  type ProtocolStream,
  type StreamFactory,
  type StreamProtocolFactory,
  createMemoryStreamFactory,
  cascadeReclaim,
  copyOnForkReclaim,
  plainPurge,
  refCountLineage,
  reverseIndexLineage,
  ttlOnlyReclaim,
  type AfterCommitEffects,
  type CommitResult,
  type CreateCommit,
  type CreatePlan,
  type DeleteCommit,
  type DeletePlan,
  type ForkCommit,
  type ForkPlan,
  type LineagePolicy,
  type LineageStore,
  type MutationPlan,
} from "@streamsy/core";
import { createJsonProtocol, JsonProtocol } from "@streamsy/json";
import { createDurableStateProtocol, DurableStateProtocol } from "@streamsy/state";

type DurableObjectApi = typeof import("@streamsy/storage-durable-object");

describe("public API import guard", () => {
  it("exposes protocol factories and storage factories", async () => {
    const factory: StreamFactory = createMemoryStreamFactory();
    const protocol: StreamProtocolFactory = createStreamProtocol({ storage: { factory } });
    const handler = createHttpHandler({ protocol });
    const readOnlyHandler = createReadOnlyHttpHandler({ protocol });
    const effects: AfterCommitEffects = { notify: "message" };
    const plan: MutationPlan = { preconditions: {}, afterCommit: effects };
    const commitResult: CommitResult = { status: "precondition-failed", record: null };
    const createPlan: CreatePlan | undefined = undefined;
    const createCommit: CreateCommit | undefined = undefined;
    const forkPlan: ForkPlan | undefined = undefined;
    const forkCommit: ForkCommit | undefined = undefined;
    const deletePlan: DeletePlan | undefined = undefined;
    const deleteCommit: DeleteCommit | undefined = undefined;
    const lineagePolicy: LineagePolicy | undefined = undefined;
    const lineageStore: LineageStore | undefined = undefined;
    void commitResult;
    void createPlan;
    void createCommit;
    void forkPlan;
    void forkCommit;
    void deletePlan;
    void deleteCommit;
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
    const created = await protocol.create("guard", {});
    expect(created.status).toBe("created");
    if (created.status === "created") {
      const stream: ProtocolStream = created.stream;
      expect(stream.id).toBe("guard");
    }
    const durableObjectExports = [
      "createDurableObjectStreamFactory",
      "DurableObjectStreamStorage",
    ] satisfies Array<keyof DurableObjectApi>;
    expect(durableObjectExports).toContain("createDurableObjectStreamFactory");
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
