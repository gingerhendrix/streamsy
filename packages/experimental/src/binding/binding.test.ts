import { officialProtocolClient, protocolPathUrl } from "@streamsy/client";
import {
  createHttpHandler,
  createMemoryStorageAdapter,
  directProtocolClient,
  StreamProtocol,
  type ClientReadResult,
  type JsonValue,
  type ReadStreamOptions,
  type StreamProtocolClient,
  type StreamProtocolHandle,
} from "@streamsy/core";
import { describe, expect, it, vi } from "vitest";
import { streamIdentity } from "../causal.ts";
import { appendBoundStream, bindStream, readBoundStream } from "./binding.ts";

const noRetry = { initialDelay: 1, maxDelay: 1, multiplier: 1, maxRetries: 0 };

function unusedHandleOperation(): Promise<never> {
  return Promise.reject(new Error("unused client operation"));
}

function directHarness(identityName = "logical-orders", streamId = "physical/orders") {
  const protocol = new StreamProtocol({ storage: { adapter: createMemoryStorageAdapter() } });
  const client = directProtocolClient(protocol);
  const binding = bindStream({ identity: streamIdentity(identityName), client, streamId });
  return { binding, client };
}

function fetchHarness(identityName = "logical-orders", streamId = "physical/orders") {
  const protocol = new StreamProtocol({ storage: { adapter: createMemoryStorageAdapter() } });
  const handler = createHttpHandler({ protocol, pathPrefix: "/streams" });
  const fetch: typeof globalThis.fetch = Object.assign(
    (input: Parameters<typeof globalThis.fetch>[0], init?: RequestInit) =>
      handler.fetch(new Request(input, init)),
    { preconnect: globalThis.fetch.preconnect },
  );
  const client = officialProtocolClient({
    urlFor: (id) => protocolPathUrl("https://stream.test/streams", id),
    fetch,
    backoffOptions: noRetry,
    warnOnHttp: false,
  });
  const binding = bindStream({ identity: streamIdentity(identityName), client, streamId });
  return { binding, client };
}

describe("fixed-client binding", () => {
  it("mints an exact direct acknowledgement in the configured identity domain", async () => {
    const { binding, client } = directHarness();
    expect(binding.identity.name).not.toBe(binding.streamId);
    await binding.client.stream(binding.streamId).create({ contentType: "text/plain" });

    const result = await appendBoundStream(binding, "created", { contentType: "text/plain" });

    expect(result).toMatchObject({
      status: "appended",
      offset: expect.any(String),
      ack: { identity: { name: "logical-orders" }, position: expect.any(String) },
    });
    if (result.status !== "appended") throw new Error("expected appended");
    expect(result.ack.position).toBe(result.offset);
    await client.close();
  });

  it("mints an exact fetch acknowledgement without deriving identity from its URL", async () => {
    const { binding, client } = fetchHarness("mesh-name", "url/path");
    await binding.client.stream(binding.streamId).create({ contentType: "text/plain" });

    const result = await appendBoundStream(binding, "created", { contentType: "text/plain" });

    expect(result).toMatchObject({
      status: "appended",
      offset: expect.any(String),
      ack: { identity: { name: "mesh-name" }, position: expect.any(String) },
    });
    if (result.status !== "appended") throw new Error("expected appended");
    expect(result.ack.position).toBe(result.offset);
    await client.close();
  });

  it("passes -1 and now through unchanged to the fixed handle read", async () => {
    const readCalls = vi.fn((_options?: ReadStreamOptions) => undefined);
    const read = <T extends JsonValue = JsonValue>(
      options?: ReadStreamOptions,
    ): Promise<ClientReadResult<T>> => {
      readCalls(options);
      return Promise.resolve({ status: "not-found" });
    };
    const handle: StreamProtocolHandle = {
      id: "physical",
      head: unusedHandleOperation,
      create: unusedHandleOperation,
      append: unusedHandleOperation,
      appendJsonBatch: unusedHandleOperation,
      close: unusedHandleOperation,
      read,
    };
    const stream = vi.fn((_streamId: string) => handle);
    const client: StreamProtocolClient = {
      stream,
      close: vi.fn(async () => undefined),
    };
    const binding = bindStream({
      identity: streamIdentity("logical"),
      client,
      streamId: "physical",
    });

    await readBoundStream(binding, { offset: "-1" });
    await readBoundStream(binding, { offset: "now", live: "long-poll" });

    expect(stream).toHaveBeenNthCalledWith(1, "physical");
    expect(stream).toHaveBeenNthCalledWith(2, "physical");
    expect(readCalls).toHaveBeenNthCalledWith(1, { offset: "-1" });
    expect(readCalls).toHaveBeenNthCalledWith(2, { offset: "now", live: "long-poll" });
  });

  it.each([
    ["direct", directHarness],
    ["fetch", fetchHarness],
  ] as const)("keeps a producer duplicate explicit on %s", async (_name, harness) => {
    const { binding, client } = harness();
    await binding.client.stream(binding.streamId).create({ contentType: "text/plain" });
    const options = {
      contentType: "text/plain",
      producer: { producerId: "lane", producerEpoch: 1, producerSeq: 0 },
    };
    expect(await appendBoundStream(binding, "original", options)).toMatchObject({
      status: "appended",
      ack: expect.any(Object),
    });

    const duplicate = await appendBoundStream(binding, "changed", options);

    expect(duplicate).toMatchObject({
      status: "duplicate",
      offset: expect.any(String),
      producerEpoch: 1,
      producerSeq: 0,
    });
    expect(duplicate).not.toHaveProperty("ack");
    await client.close();
  });

  it("does not mint an acknowledgement when fetch success lacks an exact offset", async () => {
    const requests: Request[] = [];
    const fetch: typeof globalThis.fetch = Object.assign(
      vi.fn(async (input: Parameters<typeof globalThis.fetch>[0], init?: RequestInit) => {
        requests.push(new Request(input, init));
        return new Response(null, { status: 204 });
      }),
      { preconnect: globalThis.fetch.preconnect },
    );
    const client = officialProtocolClient({
      urlFor: () => "https://stream.test/missing-offset",
      fetch,
      backoffOptions: noRetry,
    });
    const binding = bindStream({
      identity: streamIdentity("logical"),
      client,
      streamId: "physical",
    });

    const result = await appendBoundStream(binding, "data", { contentType: "text/plain" });

    expect(result).toMatchObject({ status: "error", code: "parse-error", retryable: false });
    expect(result).not.toHaveProperty("ack");
    expect(requests.map((request) => request.method)).toEqual(["POST"]);
    await client.close();
  });
});
