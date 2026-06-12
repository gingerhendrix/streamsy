import { describe, expect, it } from "vitest";
import { StreamProtocol } from "../../protocol.ts";
import { createMemoryStreamFactory } from "./factory.ts";

describe("memory protocol concurrency", () => {
  it("appends through a bound protocol stream", async () => {
    const protocol = new StreamProtocol({ storage: { factory: createMemoryStreamFactory() } });
    await protocol.create("s", { contentType: "text/plain" });
    const lookup = await protocol.get("s");
    expect(lookup.status).toBe("ok");
    if (lookup.status !== "ok") throw new Error("lookup failed");
    await Promise.all([
      lookup.stream.append({ contentType: "text/plain", data: new TextEncoder().encode("a") }),
      lookup.stream.append({ contentType: "text/plain", data: new TextEncoder().encode("b") }),
    ]);
    const read = await lookup.stream.read({});
    expect(read.status).toBe("ok");
    if (read.status !== "ok") throw new Error("read failed");
    expect(read.messages).toHaveLength(2);
  });
});
