import { describe, expect, it } from "vitest";
import { StreamProtocol } from "../../protocol.ts";
import { createMemoryStorageAdapter } from "./adapter.ts";

describe("memory create", () => {
  it("is idempotent through protocol create", async () => {
    const protocol = new StreamProtocol({ storage: { adapter: createMemoryStorageAdapter() } });
    const first = await protocol.create("s", { contentType: "text/plain" });
    const second = await protocol.create("s", { contentType: "text/plain" });
    expect(first.status).toBe("created");
    expect(second.status).toBe("exists");
  });
});
