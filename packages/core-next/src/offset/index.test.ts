import { describe, expect, it } from "bun:test";
import { Schema } from "effect";
import { Offset } from "../schema/index.ts";
import { ZERO_OFFSET, next, isValid, compare } from "./index.ts";
describe("fixed offsets", () => {
  it("preserves the 33-character two-field wire format", () => {
    expect(ZERO_OFFSET).toBe(Offset.make("0000000000000000_0000000000000000"));
    expect(next(ZERO_OFFSET)).toBe(Offset.make("0000000000000001_0000000000000000"));
  });
  it("accepts only canonical boundaries at the schema edge", () => {
    for (const invalid of [
      "",
      "1_0",
      "now",
      "-1",
      "a/b",
      "a,b",
      "a&b",
      "a=b",
      "a?b",
      "0000000000000001_0",
    ]) {
      expect(isValid(invalid)).toBe(false);
      expect(() => Schema.decodeUnknownSync(Offset)(invalid)).toThrow();
    }
  });
  it("orders lexically including equality and reverse", () => {
    expect(compare(ZERO_OFFSET, next(ZERO_OFFSET))).toBe(-1);
    expect(compare(next(ZERO_OFFSET), ZERO_OFFSET)).toBe(1);
    expect(compare(ZERO_OFFSET, ZERO_OFFSET)).toBe(0);
  });
  it("advances beyond safe integer precision without duplicate offsets", () => {
    expect(next(Offset.make("9007199254740992_0000000000000000"))).toBe(
      Offset.make("9007199254740993_0000000000000000"),
    );
  });
  it("rejects overflow instead of widening the token", () => {
    expect(() => next(Offset.make("9999999999999999_0000000000000000"))).toThrow(RangeError);
  });
  it("preserves lexical advance from a nonzero second field", () => {
    expect(next(Offset.make("0000000000000041_0000000000000009"))).toBe(
      Offset.make("0000000000000042_0000000000000000"),
    );
  });
});
