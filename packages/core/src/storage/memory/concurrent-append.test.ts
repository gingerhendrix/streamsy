import { describe, expect, it } from "vitest";
import { StreamProtocol } from "../../protocol.ts";
import { createMemoryStreamFactory } from "./factory.ts";

describe("memory protocol concurrency", () => {
  it("retries concurrent non-CAS appends until all writers commit", async () => {
    const protocol = new StreamProtocol({ storage: { factory: createMemoryStreamFactory() } });
    await protocol.create("s", { contentType: "text/plain" });
    const lookup = await protocol.get("s");
    expect(lookup.status).toBe("ok");
    if (lookup.status !== "ok") throw new Error("lookup failed");
    const results = await Promise.all(
      Array.from({ length: 32 }, (_, i) =>
        lookup.stream.append({
          contentType: "text/plain",
          data: new TextEncoder().encode(`m${i}`),
        }),
      ),
    );
    expect(results.every((result) => result.status === "appended")).toBe(true);
    const read = await lookup.stream.read({});
    expect(read.status).toBe("ok");
    if (read.status !== "ok") throw new Error("read failed");
    expect(read.messages).toHaveLength(32);
    const offsets = read.messages.map((message) => message.offset);
    expect(new Set(offsets).size).toBe(32);
    expect(offsets).toEqual(
      Array.from({ length: 32 }, (_, i) => `${String(i + 1).padStart(16, "0")}_0000000000000000`),
    );
  });
});
