// oxlint-disable effecttsgo/async-function -- Named Web/Bun boundary owns native request, response and server disposal operations.
import { Effect } from "effect";
import { HttpServerRequest } from "effect/http";
import { describe, expect, it } from "bun:test";
import * as Etags from "../../src/http/etag-builder.ts";
import { discardRequestBody } from "../../src/http/request-body-discard.ts";

describe("HTTP EtagBuilder", () => {
  it("varies the etag with start offset and closure flag", () => {
    const etags = Etags;
    expect(etags.forCatchUp("/s", "-1", "1_0", true)).not.toEqual(
      etags.forCatchUp("/s", "-1", "1_0", false),
    );
    expect(etags.forCatchUp("/s", "0_0", "1_0", false)).not.toEqual(
      etags.forCatchUp("/s", "1_0", "1_0", false),
    );
  });
});

const streamingRequest = (chunks: number, onCancel: () => void) => {
  let pulls = 0;
  const request = new Request("http://x/s", {
    method: "POST",
    body: new ReadableStream<Uint8Array>({
      pull: (controller) => {
        pulls += 1;
        if (pulls <= chunks) controller.enqueue(new Uint8Array([pulls]));
        else controller.close();
      },
      cancel: onCancel,
    }),
  });
  return { request, pulls: () => pulls };
};

describe("HTTP discardRequestBody", () => {
  it("leaves a locked body alone", async () => {
    const { request } = streamingRequest(2, () => undefined);
    const reader = request.body?.getReader();
    if (!reader) throw new Error("expected a body reader");
    await Effect.runPromise(discardRequestBody(HttpServerRequest.fromWeb(request)));
    expect((await reader.read()).value).toEqual(new Uint8Array([1]));
    reader.releaseLock();
  });

  it("succeeds when the body is absent or fails while reading", async () => {
    const empty = new Request("http://x/s", { method: "POST" });
    await Effect.runPromise(discardRequestBody(HttpServerRequest.fromWeb(empty)));
    const failing = new Request("http://x/s", {
      method: "POST",
      body: new ReadableStream<Uint8Array>({
        pull: () => Promise.reject(new Error("no")),
      }),
    });
    await Effect.runPromise(discardRequestBody(HttpServerRequest.fromWeb(failing)));
  });
});
