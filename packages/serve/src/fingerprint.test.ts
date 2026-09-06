import { describe, expect, test } from "bun:test";
import { canonicalJson, contractFingerprint, documentEtag } from "./fingerprint.ts";

describe("canonical encoding", () => {
  test("sorts object keys and preserves array order", () => {
    expect(canonicalJson({ b: 1, a: [3, 1, 2] })).toBe('{"a":[3,1,2],"b":1}');
  });

  test("two values that differ only in property order encode identically", () => {
    expect(canonicalJson({ a: { y: 1, x: 2 }, b: null })).toBe(
      canonicalJson({ b: null, a: { x: 2, y: 1 } }),
    );
  });

  test("null is a value, not an absence", () => {
    expect(canonicalJson({ a: null })).toBe('{"a":null}');
    expect(canonicalJson(null)).toBe("null");
  });
});

describe("digests", () => {
  test("a fingerprint is eight lowercase hex characters and follows the value", () => {
    const one = contractFingerprint({ route: "/a", params: ["x"] });
    expect(one).toMatch(/^[0-9a-f]{8}$/);
    expect(contractFingerprint({ params: ["x"], route: "/a" })).toBe(one);
    expect(contractFingerprint({ route: "/b", params: ["x"] })).not.toBe(one);
  });

  test("an entity tag is a quoted sixteen-hex validator that changes with the bytes", () => {
    const tag = documentEtag('{"issues":1}');
    expect(tag).toMatch(/^"[0-9a-f]{16}"$/);
    expect(documentEtag('{"issues":1}')).toBe(tag);
    expect(documentEtag('{"issues":2}')).not.toBe(tag);
  });
});
