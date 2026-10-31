import { describe, expect, test } from "bun:test";
import { forwardedSection, hashFragment } from "../../src/lib/moved-pages.ts";

// Each case passes a raw `location.hash`, so the decode in `hashFragment` runs as it
// does in the browser.
describe("forwardedSection", () => {
  test("a kept fragment stays on its page", () => {
    expect(forwardedSection("servers/protocol", "#options")).toBeUndefined();
    expect(forwardedSection("servers/process", "#listener")).toBeUndefined();
  });

  test("a moved fragment forwards to another page", () => {
    expect(forwardedSection("servers/process", "#sqlite-storage")).toEqual({
      slug: "storage/sqlite",
      hash: "bun",
    });
    expect(forwardedSection("servers/cloudflare", "#wakeups")).toEqual({
      slug: "servers/cloudflare/alarms",
      hash: "deadlines",
    });
  });

  test("a moved fragment forwards to another heading on the same page", () => {
    expect(forwardedSection("servers/remote", "#limits")).toEqual({
      slug: "servers/remote",
      hash: "projections",
    });
  });

  test("a percent-encoded fragment is decoded before lookup", () => {
    expect(forwardedSection("servers/process", "#sqlite%2Dstorage")).toEqual({
      slug: "storage/sqlite",
      hash: "bun",
    });
    expect(forwardedSection("storage", "#caf%C3%A9")).toBeUndefined();
  });

  test("an unknown or empty fragment names no section", () => {
    expect(forwardedSection("servers/process", "#nope")).toBeUndefined();
    expect(forwardedSection("servers/process", "#")).toBeUndefined();
    expect(forwardedSection("servers/process", "")).toBeUndefined();
    expect(forwardedSection("servers/protocol", "#sqlite-storage")).toBeUndefined();
  });

  test("a malformed fragment names no section and does not throw", () => {
    expect(() => decodeURIComponent("%E0%A4%A")).toThrow(URIError);
    expect(forwardedSection("storage", "#%E0%A4%A")).toBeUndefined();
    expect(forwardedSection("servers/protocol", "#100%")).toBeUndefined();
    expect(forwardedSection("servers/process", "#sqlite-storage%")).toBeUndefined();
    expect(forwardedSection("servers/process", "#%")).toBeUndefined();
  });
});

describe("hashFragment", () => {
  test("strips the hash sign and decodes", () => {
    expect(hashFragment("#options")).toBe("options");
    expect(hashFragment("#caf%C3%A9")).toBe("café");
    expect(hashFragment("options")).toBe("options");
    expect(hashFragment("")).toBe("");
  });

  test("returns undefined for malformed percent-encoding", () => {
    expect(hashFragment("#%E0%A4%A")).toBeUndefined();
    expect(hashFragment("#100%")).toBeUndefined();
    expect(hashFragment("#%ZZ")).toBeUndefined();
  });
});
