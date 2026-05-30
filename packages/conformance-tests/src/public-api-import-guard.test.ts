import { describe, expect, it } from "vitest";
import {
  createHttpHandler,
  createReadOnlyHttpHandler,
  createStreamProtocol,
  ZERO_OFFSET,
  type ProtocolStream,
  type StreamFactory,
  type StreamProtocolFactory,
} from "@streamsy/core";
import { createMemoryStreamFactory } from "@streamsy/storage-memory";
import {
  createDurableObjectStreamFactory,
  DurableObjectStreamStorage,
} from "@streamsy/storage-durable-object";

describe("public API import guard", () => {
  it("exposes protocol factories and storage factories", async () => {
    const factory: StreamFactory = createMemoryStreamFactory();
    const protocol: StreamProtocolFactory = createStreamProtocol({ storage: { factory } });
    const handler = createHttpHandler({ protocol });
    const readOnlyHandler = createReadOnlyHttpHandler({ protocol });
    expect(ZERO_OFFSET).toBe("-1");
    expect(handler.fetch).toBeTypeOf("function");
    expect(readOnlyHandler.fetch).toBeTypeOf("function");
    const created = await protocol.create("guard", {});
    expect(created.status).toBe("created");
    if (created.status === "created") {
      const stream: ProtocolStream = created.stream;
      expect(stream.id).toBe("guard");
    }
    expect(createDurableObjectStreamFactory).toBeTypeOf("function");
    expect(DurableObjectStreamStorage).toBeTypeOf("function");
  });
});
