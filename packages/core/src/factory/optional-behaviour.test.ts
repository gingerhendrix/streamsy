import { describe, expect, it } from "vitest";
import { isNotSupportedError, notSupportedFromError, unsupported } from "../types/factory.ts";

describe("unsupported storage behavior", () => {
  it("uses typed errors that map to protocol not-supported results", () => {
    const error = unsupported("live-read", "adapter does not support long-poll reads");

    expect(isNotSupportedError(error)).toBe(true);
    expect(notSupportedFromError(error)).toEqual({
      status: "not-supported",
      feature: "live-read",
      message: "adapter does not support long-poll reads",
    });
  });
});
