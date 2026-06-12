import { describe, expect, it } from "vitest";
import { ProtocolStream, StreamProtocol } from "../protocol.ts";
import { createMemoryStreamFactory } from "../storage/memory/factory.ts";

describe("StreamProtocol factory", () => {
  it("creates and then resolves a bound protocol stream", async () => {
    const protocol = new StreamProtocol({ storage: { factory: createMemoryStreamFactory() } });
    const created = await protocol.create("alpha", {
      contentType: "text/plain",
      initialData: new TextEncoder().encode("hello"),
    });
    expect(created.status).toBe("created");
    if (created.status !== "created") throw new Error("expected create");
    expect(created.stream).toBeInstanceOf(ProtocolStream);
    expect(created.stream.id).toBe("alpha");

    const lookup = await protocol.get("alpha");
    expect(lookup.status).toBe("ok");
    if (lookup.status !== "ok") throw new Error("expected lookup");
    expect(lookup.stream).toBeInstanceOf(ProtocolStream);
    const read = await lookup.stream.read({});
    expect(read.status).toBe("ok");
    if (read.status !== "ok") throw new Error("expected read");
    expect(new TextDecoder().decode(read.messages[0]!.data)).toBe("hello");
  });
});
