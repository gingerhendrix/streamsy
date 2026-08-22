import {
  createHttpHandler,
  createMemoryStorageAdapter,
  directProtocolClient,
  StreamProtocol,
} from "@streamsy/core";
import { officialProtocolClient, protocolPathUrl } from "@streamsy/client";
import { describe } from "vitest";
import { runStreamProtocolClientContract } from "./stream-protocol-client-contract.ts";

/**
 * Completes the loopback implementation into a full `typeof globalThis.fetch`.
 *
 * A host may hang extra members on the `fetch` global — Bun declares
 * `fetch.preconnect` — so a bare `(input, init) => Promise<Response>` arrow is
 * not assignable to that type under every type environment. Copying the host
 * global's own properties satisfies the type and carries those members through
 * at runtime, rather than asserting the difference away.
 */
function asFetch(
  impl: (...args: Parameters<typeof globalThis.fetch>) => Promise<Response>,
): typeof globalThis.fetch {
  return Object.assign(impl, globalThis.fetch);
}

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
    const fetch = asFetch(async (input, init) => handler.fetch(new Request(input, init)));
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
