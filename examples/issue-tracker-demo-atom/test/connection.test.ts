import { afterEach, describe, expect, it, vi } from "vitest";
import { trackConnection } from "../src/connection.ts";

const answer = (status: number) => Promise.resolve(new Response(null, { status }));

describe("trackConnection", () => {
  afterEach(() => vi.unstubAllGlobals());

  it("goes offline on a gateway status and back online on the next answer", () => {
    const connection = trackConnection();

    vi.stubGlobal("fetch", () => answer(502));

    return connection
      .fetch("/streams/workspace/main")
      .then(() => {
        expect(connection.isOnline()).toBe(false);
        vi.stubGlobal("fetch", () => answer(404));

        return connection.fetch("/streams/workspace/main");
      })
      .then(() => {
        expect(connection.isOnline()).toBe(true);
      });
  });
});
