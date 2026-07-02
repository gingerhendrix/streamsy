import { describe, expect, it } from "vitest";
import { isNotSupported, notSupported } from "../index.ts";

describe("notSupported", () => {
  it("creates structured not-supported results", () => {
    expect(notSupported("x")).toEqual({ status: "not-supported", feature: "x" });
    expect(isNotSupported(notSupported("x"))).toBe(true);
  });
});
