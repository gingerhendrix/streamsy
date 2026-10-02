/**
 * Unit coverage for the content-type-matcher helper extracted from
 * StreamProtocol. Pins case-insensitive matching, RFC 7231 parameter
 * stripping, and surrounding-whitespace tolerance.
 */

import { describe, it, expect } from "bun:test";
import { contentTypeMatches, normalizeContentType } from "../../src/policy/content-type-matcher.ts";

describe("normalizeContentType", () => {
  it("lowercases the type", () => {
    expect(normalizeContentType("Application/JSON")).toBe("application/json");
    expect(normalizeContentType("TEXT/PLAIN")).toBe("text/plain");
  });

  it("strips parameters after the first semicolon", () => {
    expect(normalizeContentType("application/json; charset=utf-8")).toBe("application/json");
    expect(normalizeContentType("text/plain;charset=us-ascii;boundary=x")).toBe("text/plain");
  });

  it("trims surrounding whitespace from the type portion", () => {
    expect(normalizeContentType("  application/json  ")).toBe("application/json");
    expect(normalizeContentType("application/json   ; charset=utf-8")).toBe("application/json");
  });
});

describe("contentTypeMatches", () => {
  it("rejects different types", () => {
    expect(contentTypeMatches("application/json", "text/plain")).toBe(false);
    expect(contentTypeMatches("application/octet-stream", "application/json")).toBe(false);
  });
});
