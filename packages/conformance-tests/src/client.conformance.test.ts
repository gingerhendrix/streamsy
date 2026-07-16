import {
  createHttpHandler,
  createMemoryStorageAdapter,
  directProtocolClient,
  StreamProtocol,
} from "@streamsy/core";
import { officialProtocolClient, protocolPathUrl } from "@streamsy/client";
import { describe } from "vitest";
import { runStreamProtocolClientContract } from "./stream-protocol-client-contract.ts";

function memoryProtocol(): StreamProtocol {
  return new StreamProtocol({
    storage: { adapter: createMemoryStorageAdapter() },
    longPollTimeoutMs: 50,
  });
}

describe("StreamProtocolClient contract: direct", () => {
  runStreamProtocolClientContract(() => ({ client: directProtocolClient(memoryProtocol()) }));
});

describe("StreamProtocolClient contract: official loopback", () => {
  runStreamProtocolClientContract(() => {
    const protocol = memoryProtocol();
    const handler = createHttpHandler({ protocol, pathPrefix: "/streams" });
    const fetch = (async (input: Parameters<typeof globalThis.fetch>[0], init?: RequestInit) =>
      handler.fetch(new Request(input, init))) as typeof globalThis.fetch;
    return {
      client: officialProtocolClient({
        urlFor: (id) => protocolPathUrl("https://stream.test/streams", id),
        fetch,
        backoffOptions: { initialDelay: 1, maxDelay: 1, multiplier: 1, maxRetries: 0 },
        warnOnHttp: false,
      }),
    };
  });
});
