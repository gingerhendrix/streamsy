import { describe, expect, it } from "vitest";
import { HttpHandler, StreamProtocol } from "../../index.ts";
import { createMemoryStreamFactory } from "../../storage/memory/factory.ts";

describe("HTTP services with bound protocol streams", () => {
  it("resolves streams through protocol get for existing-stream operations", async () => {
    const protocol = new StreamProtocol({ storage: { factory: createMemoryStreamFactory() } });
    const handler = new HttpHandler({ protocol });
    const put = await handler.fetch(
      new Request("http://x/example", { method: "PUT", headers: { "content-type": "text/plain" } }),
    );
    expect(put.status).toBe(201);
    const post = await handler.fetch(
      new Request("http://x/example", {
        method: "POST",
        headers: { "content-type": "text/plain" },
        body: "hi",
      }),
    );
    expect(post.status).toBe(204);
    const head = await handler.fetch(new Request("http://x/example", { method: "HEAD" }));
    expect(head.status).toBe(200);
    const get = await handler.fetch(new Request("http://x/example", { method: "GET" }));
    expect(get.status).toBe(200);
    expect(await get.text()).toBe("hi");
  });
});
