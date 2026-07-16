import { describe, expect, it } from "vitest";
import { EtagBuilder } from "../http/etag-builder.ts";
import { MessageBodyCodec } from "../http/message-body-codec.ts";
import { ProducerHeaderParser } from "../http/producer-header-parser.ts";
import { ReadQueryParser } from "../http/read-query-parser.ts";
import { defaultOffsetGenerator } from "../protocol/helpers/offset-generator.ts";
import { RequestBodyReader } from "../http/request-body-reader.ts";
import { HttpResponseFactory } from "../http/responses.ts";
import { SseEventEncoder } from "../http/sse-event-encoder.ts";
import { StreamPathService } from "../http/stream-path-service.ts";

const enc = new TextEncoder();
const dec = new TextDecoder();

describe("HTTP MessageBodyCodec", () => {
  it("preserves JSON message text inside an array wrapper", () => {
    const codec = new MessageBodyCodec();
    const body = codec.encodeHttpBody(
      [{ data: enc.encode('{"a":1}') }, { data: enc.encode('{"b":2}') }],
      "application/json",
    );
    expect(body).toBe('[{"a":1},{"b":2}]');
  });

  it("concatenates text and binary bodies", () => {
    const codec = new MessageBodyCodec();
    expect(
      codec.encodeHttpBody([{ data: enc.encode("a") }, { data: enc.encode("b") }], "text/plain"),
    ).toBe("ab");
    expect(
      Array.from(
        new Uint8Array(
          codec.encodeHttpBody(
            [{ data: new Uint8Array([1]) }, { data: new Uint8Array([2, 3]) }],
            "application/octet-stream",
          ) as ArrayBuffer,
        ),
      ),
    ).toEqual([1, 2, 3]);
  });

  it("returns content-type-shaped empty bodies", () => {
    const codec = new MessageBodyCodec();
    expect(codec.emptyBodyForContentType("application/json")).toBe("[]");
    expect(codec.emptyBodyForContentType("text/plain")).toBe("");
    expect(codec.emptyBodyForContentType("application/octet-stream")).toBe("");
  });
});

describe("HTTP ProducerHeaderParser", () => {
  it("distinguishes absent, valid, partial, and overflow producer headers", () => {
    const parser = new ProducerHeaderParser();
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

describe("HTTP StreamPathService", () => {
  it("strips configured prefixes and canonicalizes fork sources", () => {
    const path = new StreamPathService("/api.v1");
    expect(path.strip("/api.v1/foo/bar")).toBe("foo/bar");
    expect(path.strip("/other/foo")).toBe("/other/foo");
    expect(path.canonicalizeForkSource("/api.v1/source")).toBe("source");
  });
});

describe("HTTP SseEventEncoder", () => {
  it("splits text lines and base64-encodes binary events", () => {
    const codec = new MessageBodyCodec();
    const sse = new SseEventEncoder(codec);
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
    const codec = new MessageBodyCodec();
    const sse = new SseEventEncoder(codec);
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
    const codec = new MessageBodyCodec();
    const sse = new SseEventEncoder(codec);
    const text = dec.decode(sse.controlEvent({ streamNextOffset: "1_0", upToDate: true }));
    expect(text).toBe('event: control\ndata:{"streamNextOffset":"1_0","upToDate":true}\n\n');
  });
});

describe("HTTP EtagBuilder", () => {
  it("varies the etag with start offset, next offset, and closure flag", () => {
    const etags = new EtagBuilder();
    expect(etags.forCatchUp("/s", "-1", "1_0", false)).toBe(`"${btoa("/s")}:-1:1_0"`);
    expect(etags.forCatchUp("/s", "-1", "1_0", true)).toBe(`"${btoa("/s")}:-1:1_0:c"`);
    expect(etags.forCatchUp("/s", "0_0", "1_0", false)).not.toEqual(
      etags.forCatchUp("/s", "1_0", "1_0", false),
    );
  });
});

describe("HTTP ReadQueryParser", () => {
  it("rejects malformed offsets and accepts the documented sentinels", () => {
    const parser = new ReadQueryParser((offset) => defaultOffsetGenerator.isValid(offset));
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
    const parser = new ReadQueryParser((offset) => defaultOffsetGenerator.isValid(offset));
    expect(parser.parse(new URL("http://x/s?offset=-1&live=long-poll&cursor=c1"))).toMatchObject({
      ok: true,
      live: "long-poll",
      cursor: "c1",
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

describe("HTTP RequestBodyReader", () => {
  it("returns 413 for oversized bodies", async () => {
    const reader = new RequestBodyReader(2, new HttpResponseFactory());
    const result = await reader.read(new Request("http://x/s", { method: "POST", body: "abcd" }));
    expect(result.ok).toBe(false);
    if (!result.ok) expect(result.response.status).toBe(413);
  });

  it("returns the parsed body bytes when within limit", async () => {
    const reader = new RequestBodyReader(1024, new HttpResponseFactory());
    const result = await reader.read(new Request("http://x/s", { method: "POST", body: "hi" }));
    expect(result.ok).toBe(true);
    if (result.ok) expect(dec.decode(result.data)).toBe("hi");
  });
});

describe("HTTP HttpResponseFactory", () => {
  it("adds nosniff and CORP defaults without overwriting explicit headers", () => {
    const factory = new HttpResponseFactory();
    const wrapped = factory.withSecurityHeaders(new Response("hi"));
    expect(wrapped.headers.get("x-content-type-options")).toBe("nosniff");
    expect(wrapped.headers.get("cross-origin-resource-policy")).toBe("cross-origin");

    const explicit = factory.withSecurityHeaders(
      new Response("hi", { headers: { "x-content-type-options": "custom" } }),
    );
    expect(explicit.headers.get("x-content-type-options")).toBe("custom");
  });

  it("returns canonical statuses for the common error helpers", () => {
    const factory = new HttpResponseFactory();
    expect(factory.notFound().status).toBe(404);
    expect(factory.gone().status).toBe(410);
    expect(factory.conflict("nope").status).toBe(409);
    expect(factory.payloadTooLarge().status).toBe(413);
    expect(factory.invalidJson().status).toBe(400);
    expect(factory.methodNotAllowed().status).toBe(405);
    expect(factory.internalError().status).toBe(500);
  });
});
