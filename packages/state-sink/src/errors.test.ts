import { describe, expect, test } from "bun:test";
import { decodeStateSinkPublicError } from "./errors.ts";

describe("browser-safe state-sink public error decoding", () => {
  test("decodes closed resume literals", () => {
    expect(
      decodeStateSinkPublicError({
        _tag: "ResumeRejected",
        sink: "test.rows",
        reason: "contract-changed",
        recovery: "snapshot-then-live",
      }),
    ).toEqual({
      _tag: "ResumeRejected",
      sink: "test.rows",
      reason: "contract-changed",
      recovery: "snapshot-then-live",
    });
  });

  test("rejects missing and incorrectly typed required fields", () => {
    expect(() =>
      decodeStateSinkPublicError({
        _tag: "SinkUnauthorized",
        sink: "test.rows",
      }),
    ).toThrow("required must be a string");
    expect(() =>
      decodeStateSinkPublicError({
        _tag: "ProtocolVersionUnsupported",
        sink: "test.rows",
        supported: "1",
        received: "2",
        recovery: "snapshot-then-live",
      }),
    ).toThrow("supported must be a finite number");
  });

  test("rejects open recovery and reason values", () => {
    expect(() =>
      decodeStateSinkPublicError({
        _tag: "ResumeRejected",
        sink: "test.rows",
        reason: "signed-token-expired",
        recovery: "snapshot-then-live",
      }),
    ).toThrow("unknown state-sink resume rejection");
    expect(() =>
      decodeStateSinkPublicError({
        _tag: "ResumeRejected",
        sink: "test.rows",
        reason: "invalid-offset",
        recovery: "retry-token",
      }),
    ).toThrow("unknown state-sink recovery");
  });
});
