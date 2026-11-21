import { describe, expect, it } from "vitest";

import { prefersJsonOverEventStream } from "./accept.ts";

const asks = (accept?: string): boolean =>
  prefersJsonOverEventStream(
    new Request("http://risk.test/v1/games/g/players/me/actions", {
      headers: accept === undefined ? {} : { accept },
    }),
  );

describe("actions representation negotiation", () => {
  it("streams unless the caller both accepts and prefers the JSON page", () => {
    // The stream is the contract: it wins silence, wildcards, and ties.
    expect(asks(undefined)).toBe(false);
    expect(asks("*/*")).toBe(false);
    expect(asks("application/json, text/event-stream")).toBe(false);
    expect(asks("text/event-stream")).toBe(false);

    expect(asks("application/json")).toBe(true);
    expect(asks("application/json, */*;q=0.1")).toBe(true);
  });

  it("treats media types case-insensitively", () => {
    // A header is not a substring to grep: `Application/JSON` is the same type.
    expect(asks("Application/JSON")).toBe(true);
    expect(asks("APPLICATION/JSON;Q=0.9, TEXT/EVENT-STREAM;Q=0.1")).toBe(true);
    expect(asks("Text/Event-Stream")).toBe(false);
  });

  it("honours q=0 as a refusal rather than a low preference", () => {
    // Mentioning the stream is not asking for it.
    expect(asks("text/event-stream;q=0, application/json")).toBe(true);
    expect(asks("application/json;q=0")).toBe(false);
    expect(asks("application/json;q=0, text/event-stream;q=0")).toBe(false);
  });

  it("applies wildcard ranges only where nothing more specific matched", () => {
    // The explicit refusal beats the generous wildcard beside it.
    expect(asks("*/*, text/event-stream;q=0")).toBe(true);
    // …and an explicit stream preference beats a wildcard that would cover JSON.
    expect(asks("*/*;q=0.5, text/event-stream")).toBe(false);
    expect(asks("application/*, text/event-stream;q=0")).toBe(true);
  });

  it("ignores malformed entries instead of failing the request", () => {
    expect(asks("garbage")).toBe(false);
    expect(asks(",,application/json,")).toBe(true);
    expect(asks("application/json;q=notanumber")).toBe(true);
  });
});
