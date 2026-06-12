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
} from "@streamsy/core";
import { createJsonProtocol, JsonProtocol } from "@streamsy/json";
import { createDurableStateProtocol, DurableStateProtocol } from "@streamsy/state";
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
    const json = createJsonProtocol(protocol, {
      encode: (value: unknown) => value,
      decode: (value: unknown) => value,
    });
    expect(json).toBeInstanceOf(JsonProtocol);
    const durable = createDurableStateProtocol(protocol, {});
    expect(durable).toBeInstanceOf(DurableStateProtocol);
  });
});
