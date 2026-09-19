// oxlint-disable effecttsgo/async-function -- Named Web/Bun boundary owns native request, response and server disposal operations.
import { Effect } from "effect";
import { HttpServerRequest } from "effect/unstable/http";
import { describe, expect, it } from "bun:test";
import * as Etags from "../http/etag-builder.ts";
import * as MessageBody from "../http/message-body-codec.ts";
import * as ProducerHeaders from "../http/producer-header-parser.ts";
import { readQueryParser } from "../http/read-query-parser.ts";
import { isValid } from "../offset/index.ts";
import { requestBodyReader } from "../http/request-body-reader.ts";
import * as Responses from "../http/responses.ts";
import * as SseEvents from "../http/sse-event-encoder.ts";
import { streamPath } from "../http/stream-path-service.ts";

const enc = new TextEncoder();
const dec = new TextDecoder();

describe("HTTP MessageBodyCodec", () => {
  it("preserves JSON message text inside an array wrapper", () => {
    const codec = MessageBody;
    const body = codec.encodeHttpBody(
      [{ data: enc.encode('{"a":1}') }, { data: enc.encode('{"b":2}') }],
      "application/json",
    );
    expect(body).toBe('[{"a":1},{"b":2}]');
  });

  it("concatenates text and binary bodies", () => {
    const codec = MessageBody;
    expect(
      codec.encodeHttpBody([{ data: enc.encode("a") }, { data: enc.encode("b") }], "text/plain"),
    ).toBe("ab");
    const binary = codec.encodeHttpBody(
      [{ data: new Uint8Array([1]) }, { data: new Uint8Array([2, 3]) }],
      "application/octet-stream",
    );
    // A binary body must reach `fetch` as a real `ArrayBuffer`, not a view or a
    // shared buffer, so the boundary type is asserted before the bytes are read.
    expect(binary).toBeInstanceOf(ArrayBuffer);
    if (!(binary instanceof ArrayBuffer)) throw new TypeError("expected an ArrayBuffer body");
    expect(Array.from(new Uint8Array(binary))).toEqual([1, 2, 3]);
  });

  it("returns content-type-shaped empty bodies", () => {
    const codec = MessageBody;
    expect(codec.emptyBodyForContentType("application/json")).toBe("[]");
    expect(codec.emptyBodyForContentType("text/plain")).toBe("");
    expect(codec.emptyBodyForContentType("application/octet-stream")).toBe("");
  });
});

describe("HTTP ProducerHeaderParser", () => {
  it("distinguishes absent, valid, partial, and overflow producer headers", () => {
    const parser = ProducerHeaders;
    expect(parser.parse(new Request("http://x/s"))).toEqual({ kind: "absent" });
    expect(parser.parse(new Request("http://x/s", { headers: { "producer-id": "p" } }))).toEqual({
      kind: "invalid",
    });
    expect(
      parser.parse(
        new Request("http://x/s", {
          headers: { "producer-id": "p", "producer-epoch": "1", "producer-seq": "2" },
        }),
      ),
    ).toEqual({ kind: "ok", producer: { producerId: "p", producerEpoch: 1, producerSeq: 2 } });
    expect(
      parser.parse(
        new Request("http://x/s", {
          headers: {
            "producer-id": "p",
            "producer-epoch": String(Number.MAX_SAFE_INTEGER + 1),
            "producer-seq": "0",
          },
        }),
      ),
    ).toEqual({ kind: "invalid" });
  });
});

describe("HTTP streamPath", () => {
  it("strips configured prefixes and canonicalizes fork sources", () => {
    const path = streamPath("/api.v1");
    expect(path.strip("/api.v1/foo/bar")).toBe("foo/bar");
    expect(path.strip("/other/foo")).toBe("/other/foo");
    expect(path.canonicalizeForkSource("/api.v1/source")).toBe("source");
  });
});

describe("HTTP SseEventEncoder", () => {
  it("splits text lines and base64-encodes binary events", () => {
    const sse = SseEvents;
    const text = sse
      .dataEvent([{ data: enc.encode("a\nb") }], { isJson: false, isText: true, useBase64: false })
      .map((chunk) => dec.decode(chunk))
      .join("");
    expect(text).toBe("event: data\ndata:a\ndata:b\n\n");
    const binary = sse
      .dataEvent([{ data: new Uint8Array([1, 2, 3]) }], {
        isJson: false,
        isText: false,
        useBase64: true,
      })
      .map((chunk) => dec.decode(chunk))
      .join("");
    expect(binary).toBe("event: data\ndata:AQID\n\n");
  });

  it("emits a JSON array data event spanning multiple data: lines", () => {
    const sse = SseEvents;
    const text = sse
      .dataEvent([{ data: enc.encode('{"a":1}') }, { data: enc.encode('{"b":2}') }], {
        isJson: true,
        isText: false,
        useBase64: false,
      })
      .map((chunk) => dec.decode(chunk))
      .join("");
    expect(text).toBe('event: data\ndata:[\ndata:{"a":1},\ndata:{"b":2}\ndata:]\n\n');
  });

  it("formats control events as a single data: line of JSON", () => {
    const sse = SseEvents;
    const text = dec.decode(sse.controlEvent({ streamNextOffset: "1_0", upToDate: true }));
    expect(text).toBe('event: control\ndata:{"streamNextOffset":"1_0","upToDate":true}\n\n');
  });
});

describe("HTTP EtagBuilder", () => {
  it("varies the etag with start offset, next offset, and closure flag", () => {
    const etags = Etags;
    expect(etags.forCatchUp("/s", "-1", "1_0", false)).toBe(`"${btoa("/s")}:-1:1_0"`);
    expect(etags.forCatchUp("/s", "-1", "1_0", true)).toBe(`"${btoa("/s")}:-1:1_0:c"`);
    expect(etags.forCatchUp("/s", "0_0", "1_0", false)).not.toEqual(
      etags.forCatchUp("/s", "1_0", "1_0", false),
    );
  });
});

describe("HTTP readQueryParser", () => {
  it("rejects malformed offsets and accepts the documented sentinels", () => {
    const parser = readQueryParser((offset) => isValid(offset));
    const bad = parser.parse(new URL("http://x/s?offset=abc"));
    expect(bad.ok).toBe(false);
    if (!bad.ok) expect(bad.response.status).toBe(400);
    expect(parser.parse(new URL("http://x/s?offset=-1"))).toMatchObject({ ok: true, offset: "-1" });
    expect(parser.parse(new URL("http://x/s?offset=now"))).toMatchObject({
      ok: true,
      offset: "now",
    });
    const short = parser.parse(new URL("http://x/s?offset=1_0"));
    expect(short.ok).toBe(false);
    expect(
      parser.parse(new URL("http://x/s?offset=0000000000000001_0000000000000000")),
    ).toMatchObject({ ok: true });
  });

  it("classifies live mode and surfaces cursor", () => {
    const parser = readQueryParser((offset) => isValid(offset));
    expect(parser.parse(new URL("http://x/s?offset=-1&live=long-poll&cursor=1"))).toMatchObject({
      ok: true,
      live: "long-poll",
      cursor: "1",
    });
    expect(parser.parse(new URL("http://x/s?offset=-1&live=sse"))).toMatchObject({
      ok: true,
      live: "sse",
    });
    expect(parser.parse(new URL("http://x/s?offset=-1&live=other"))).toMatchObject({
      ok: true,
      live: undefined,
    });
  });
});

describe("HTTP requestBodyReader", () => {
  it("returns 413 for oversized bodies", async () => {
    const reader = requestBodyReader(2);
    const result = await Effect.runPromise(
      reader.read(
        HttpServerRequest.fromWeb(new Request("http://x/s", { method: "POST", body: "abcd" })),
      ),
    );
    expect(result.ok).toBe(false);
    if (!result.ok) expect(result.response.status).toBe(413);
  });

  it("returns the parsed body bytes when within limit", async () => {
    const reader = requestBodyReader(1024);
    const result = await Effect.runPromise(
      reader.read(
        HttpServerRequest.fromWeb(new Request("http://x/s", { method: "POST", body: "hi" })),
      ),
    );
    expect(result.ok).toBe(true);
    if (result.ok) expect(dec.decode(result.data)).toBe("hi");
  });
});

describe("HTTP HttpResponseFactory", () => {
  it("returns canonical statuses for the common error helpers", () => {
    const factory = Responses;
    expect(factory.notFound().status).toBe(404);
    expect(factory.gone().status).toBe(410);
    expect(factory.conflict("nope").status).toBe(409);
    expect(factory.payloadTooLarge().status).toBe(413);
    expect(factory.invalidJson().status).toBe(400);
    expect(factory.methodNotAllowed().status).toBe(405);
    expect(factory.internalError().status).toBe(500);
  });
});
