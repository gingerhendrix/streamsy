/**
 * Unit coverage for the content-type-matcher helper extracted from
 * StreamProtocol. Pins case-insensitive matching, RFC 7231 parameter
 * stripping, and surrounding-whitespace tolerance.
 */

import { describe, it, expect } from "vitest";
import {
  contentTypeMatches,
  normalizeContentType,
} from "../../../packages/core/src/protocol/helpers/content-type-matcher.ts";

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
  it("matches case-insensitively", () => {
    expect(contentTypeMatches("application/json", "APPLICATION/JSON")).toBe(true);
    expect(contentTypeMatches("Application/Json", "application/json")).toBe(true);
  });

  it("matches when one or both sides have parameters", () => {
    expect(contentTypeMatches("application/json", "application/json; charset=utf-8")).toBe(true);
    expect(
      contentTypeMatches("application/json; charset=utf-8", "application/json; charset=us-ascii"),
    ).toBe(true);
  });

  it("matches around incidental whitespace", () => {
    expect(contentTypeMatches("application/json   ", "  application/json")).toBe(true);
    expect(contentTypeMatches("application/json ; q=1", " application/json")).toBe(true);
  });

  it("rejects different types", () => {
    expect(contentTypeMatches("application/json", "text/plain")).toBe(false);
    expect(contentTypeMatches("application/octet-stream", "application/json")).toBe(false);
  });
});
