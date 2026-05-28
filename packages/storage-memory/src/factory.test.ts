import { describe, expect, it } from "vitest";
import { createMemoryStreamFactory } from "./index.ts";

describe("createMemoryStreamFactory", () => {
  it("returns storage streams bound to ids", async () => {
    const factory = createMemoryStreamFactory();
    const stream = await factory.getStream("s");
    expect(stream.id).toBe("s");
    expect(await stream.getRecord()).toBeNull();
  });
});
