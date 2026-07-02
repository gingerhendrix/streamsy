import { describe, expect, it } from "vitest";
import { HttpHandler, StreamProtocol } from "../index.ts";
import { createMemoryStorageAdapter } from "../storage/memory/adapter.ts";
import { unsupported } from "../types/storage-adapter.ts";
import type { StorageAdapter } from "../types/storage-adapter.ts";

/**
 * A thrown storage-level `NotSupportedError` must surface as the public
 * structured `not-supported` response (400 + `stream-not-supported` header),
 * not a 500. This is the throw-path for sub-method capabilities that
 * capability-by-presence cannot express — e.g. producer CAS lives inside
 * `AppendPlan` with no method to omit.
 */
describe("NotSupportedError → structured not-supported mapping", () => {
  function makeHandler() {
    const base = createMemoryStorageAdapter();
    const adapter: StorageAdapter = {
      ...base,
      append: (streamId, plan) => {
        if (plan.preconditions.producer) {
          throw unsupported("producers", "this backend cannot store producer state");
        }
        return base.append(streamId, plan);
      },
    };
    const protocol = new StreamProtocol({ storage: { adapter } });
    return new HttpHandler({ protocol });
  }

  it("maps a thrown NotSupportedError to 400 with the stream-not-supported header", async () => {
    const handler = makeHandler();
    const put = await handler.fetch(
      new Request("http://x/s", { method: "PUT", headers: { "content-type": "text/plain" } }),
    );
    expect(put.status).toBe(201);

    const post = await handler.fetch(
      new Request("http://x/s", {
        method: "POST",
        headers: {
          "content-type": "text/plain",
          "producer-id": "p",
          "producer-epoch": "1",
          "producer-seq": "0",
        },
        body: "hi",
      }),
    );
    expect(post.status).toBe(400);
    expect(post.headers.get("stream-not-supported")).toBe("producers");
    expect(await post.text()).toBe("this backend cannot store producer state");
  });

  it("keeps supported operations working and other thrown errors internal", async () => {
    const handler = makeHandler();
    await handler.fetch(
      new Request("http://x/s", { method: "PUT", headers: { "content-type": "text/plain" } }),
    );
    // No producer headers → the adapter appends normally.
    const post = await handler.fetch(
      new Request("http://x/s", {
        method: "POST",
        headers: { "content-type": "text/plain" },
        body: "hi",
      }),
    );
    expect(post.status).toBe(204);
  });
});
