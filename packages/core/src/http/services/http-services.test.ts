import { describe, expect, it, vi } from "vitest";
import type {
  AppendOptions,
  AppendResult,
  CreateOptions,
  CreateResult,
  DeleteResult,
  MetadataResult,
  ReadLiveOptions,
  ReadLiveResult,
  ReadOptions,
  ReadResult,
  StreamProtocolInterface,
} from "../../types/protocol.ts";
import type { StoredMessage } from "../../types/storage.ts";
import { AppendHttpService } from "../../http/services/append-http-service.ts";
import { CreateHttpService } from "../../http/services/create-http-service.ts";
import { DeleteHttpService } from "../../http/services/delete-http-service.ts";
import { LongPollHttpService } from "../../http/services/long-poll-http-service.ts";
import { MetadataHttpService } from "../../http/services/metadata-http-service.ts";
import { ReadHttpService } from "../../http/services/read-http-service.ts";
import { SseHttpService } from "../../http/services/sse-http-service.ts";
import { HttpDispatchService } from "../../http/dispatch-service.ts";
import { EtagBuilder } from "../../http/etag-builder.ts";
import { MessageBodyCodec } from "../../http/message-body-codec.ts";
import { ProducerHeaderParser } from "../../http/producer-header-parser.ts";
import { ReadQueryParser } from "../../http/read-query-parser.ts";
import { RequestBodyReader } from "../../http/request-body-reader.ts";
import { HttpResponseFactory } from "../../http/responses.ts";
import { SseEventEncoder } from "../../http/sse-event-encoder.ts";
import { StreamPathService } from "../../http/stream-path-service.ts";
import type { Clock, HttpRouteContext } from "../../http/types.ts";

const enc = new TextEncoder();
const dec = new TextDecoder();

interface ProtocolStub {
  protocol: StreamProtocolInterface;
  createCalls: Array<{ streamId: string; options: CreateOptions }>;
  appendCalls: Array<{ streamId: string; options: AppendOptions }>;
  readCalls: Array<{ streamId: string; options: ReadOptions }>;
  readLiveCalls: Array<{ streamId: string; options: ReadLiveOptions }>;
  metadataCalls: string[];
  deleteCalls: string[];
  setCreate(result: CreateResult): void;
  setAppend(result: AppendResult): void;
  setRead(result: ReadResult): void;
  setReadLive(result: ReadLiveResult): void;
  setMetadata(result: MetadataResult): void;
  setDelete(result: DeleteResult): void;
}

function makeProtocolStub(): ProtocolStub {
  let createResult: CreateResult = {
    status: "created",
    nextOffset: "0_0",
    contentType: "application/octet-stream",
  };
  let appendResult: AppendResult = { status: "appended", nextOffset: "1_0" };
  let readResult: ReadResult = { status: "ok", messages: [], nextOffset: "0_0", upToDate: true };
  let readLiveResult: ReadLiveResult = {
    status: "timeout",
    messages: [],
    nextOffset: "0_0",
    upToDate: true,
    cursor: "c",
  };
  let metadataResult: MetadataResult = {
    status: "ok",
    contentType: "application/octet-stream",
    nextOffset: "0_0",
  };
  let deleteResult: DeleteResult = { status: "ok" };
  const createCalls: Array<{ streamId: string; options: CreateOptions }> = [];
  const appendCalls: Array<{ streamId: string; options: AppendOptions }> = [];
  const readCalls: Array<{ streamId: string; options: ReadOptions }> = [];
  const readLiveCalls: Array<{ streamId: string; options: ReadLiveOptions }> = [];
  const metadataCalls: string[] = [];
  const deleteCalls: string[] = [];
  const protocol: StreamProtocolInterface = {
    async create(streamId, options) {
      createCalls.push({ streamId, options });
      return createResult;
    },
    async append(streamId, options) {
      appendCalls.push({ streamId, options });
      return appendResult;
    },
    async read(streamId, options) {
      readCalls.push({ streamId, options });
      return readResult;
    },
    async readLive(streamId, options) {
      readLiveCalls.push({ streamId, options });
      return readLiveResult;
    },
    async metadata(streamId) {
      metadataCalls.push(streamId);
      return metadataResult;
    },
    async delete(streamId) {
      deleteCalls.push(streamId);
      return deleteResult;
    },
  };
  return {
    protocol,
    createCalls,
    appendCalls,
    readCalls,
    readLiveCalls,
    metadataCalls,
    deleteCalls,
    setCreate(result) {
      createResult = result;
    },
    setAppend(result) {
      appendResult = result;
    },
    setRead(result) {
      readResult = result;
    },
    setReadLive(result) {
      readLiveResult = result;
    },
    setMetadata(result) {
      metadataResult = result;
    },
    setDelete(result) {
      deleteResult = result;
    },
  };
}

function routeCtx(request: Request, streamId = "stream"): HttpRouteContext {
  return { request, url: new URL(request.url), streamId };
}

function message(offset: string, body: string): StoredMessage {
  return { offset, data: enc.encode(body), timestamp: 1 };
}

function deps(stub: ProtocolStub) {
  const responses = new HttpResponseFactory();
  const bodyCodec = new MessageBodyCodec();
  const longPoll = new LongPollHttpService({ protocol: stub.protocol, responses, bodyCodec });
  const clock: Clock = {
    now: () => new Date("2026-01-01T00:00:00.000Z").getTime(),
    date: (value?: number | string) => new Date(value ?? "2026-01-01T00:00:00.000Z"),
  };
  const sse = new SseHttpService({
    protocol: stub.protocol,
    responses,
    sseEvents: new SseEventEncoder(bodyCodec),
    clock,
  });
  return { responses, bodyCodec, longPoll, sse };
}

async function readStream(response: Response): Promise<string> {
  return dec.decode(await response.arrayBuffer());
}

describe("CreateHttpService", () => {
  it("passes create options and normalizes empty JSON arrays as no initial data", async () => {
    const stub = makeProtocolStub();
    stub.setCreate({
      status: "created",
      nextOffset: "0_0",
      contentType: "application/json",
      closed: true,
    });
    const responses = new HttpResponseFactory();
    const service = new CreateHttpService({
      protocol: stub.protocol,
      path: new StreamPathService("/api"),
      responses,
      bodyReader: new RequestBodyReader(1024, responses),
    });

    const response = await service.execute(
      routeCtx(
        new Request("http://x/api/stream", {
          method: "PUT",
          body: "[]",
          headers: {
            "content-type": "application/json",
            "stream-ttl": "60",
            "stream-closed": "true",
          },
        }),
      ),
    );

    expect(response.status).toBe(201);
    expect(response.headers.get("location")).toBe("http://x/api/stream");
    expect(response.headers.get("stream-closed")).toBe("true");
    expect(stub.createCalls).toEqual([
      {
        streamId: "stream",
        options: {
          contentType: "application/json",
          ttlSeconds: 60,
          expiresAt: undefined,
          initialData: undefined,
          closed: true,
          forkedFrom: undefined,
          forkOffset: undefined,
        },
      },
    ]);
  });

  it("rejects invalid create headers and maps protocol conflicts", async () => {
    const stub = makeProtocolStub();
    const responses = new HttpResponseFactory();
    const service = new CreateHttpService({
      protocol: stub.protocol,
      path: new StreamPathService("/"),
      responses,
      bodyReader: new RequestBodyReader(1024, responses),
    });
    expect(
      (
        await service.execute(
          routeCtx(new Request("http://x/s", { method: "PUT", headers: { "stream-ttl": "bad" } })),
        )
      ).status,
    ).toBe(400);
    expect(
      (
        await service.execute(
          routeCtx(
            new Request("http://x/s", { method: "PUT", headers: { "stream-fork-offset": "1_0" } }),
          ),
        )
      ).status,
    ).toBe(400);

    stub.setCreate({
      status: "conflict",
      nextOffset: "0_0",
      contentType: "text/plain",
      errorMessage: "mismatch",
    });
    const conflict = await service.execute(routeCtx(new Request("http://x/s", { method: "PUT" })));
    expect(conflict.status).toBe(409);
    expect(await conflict.text()).toBe("mismatch");
  });
});

describe("AppendHttpService", () => {
  it("validates body rules before calling protocol.append", async () => {
    const stub = makeProtocolStub();
    const responses = new HttpResponseFactory();
    const service = new AppendHttpService({
      protocol: stub.protocol,
      responses,
      bodyReader: new RequestBodyReader(1024, responses),
      producerHeaders: new ProducerHeaderParser(),
    });
    expect(
      (
        await service.execute(
          routeCtx(new Request("http://x/s", { method: "POST", body: new Uint8Array([1, 2]) })),
        )
      ).status,
    ).toBe(400);
    expect(
      (
        await service.execute(
          routeCtx(
            new Request("http://x/s", {
              method: "POST",
              body: "[]",
              headers: { "content-type": "application/json" },
            }),
          ),
        )
      ).status,
    ).toBe(400);
    expect(stub.appendCalls).toHaveLength(0);
  });

  it("passes producer/sequence options and maps appended/closed-conflict responses", async () => {
    const stub = makeProtocolStub();
    stub.setAppend({ status: "appended", nextOffset: "1_0", producerEpoch: 2, producerSeq: 3 });
    const responses = new HttpResponseFactory();
    const service = new AppendHttpService({
      protocol: stub.protocol,
      responses,
      bodyReader: new RequestBodyReader(1024, responses),
      producerHeaders: new ProducerHeaderParser(),
    });

    const appended = await service.execute(
      routeCtx(
        new Request("http://x/s", {
          method: "POST",
          body: "hi",
          headers: {
            "content-type": "text/plain",
            "stream-seq": "seq-1",
            "producer-id": "p",
            "producer-epoch": "2",
            "producer-seq": "3",
          },
        }),
      ),
    );
    expect(appended.status).toBe(200);
    expect(appended.headers.get("producer-epoch")).toBe("2");
    expect(stub.appendCalls[0]).toMatchObject({
      streamId: "stream",
      options: {
        contentType: "text/plain",
        seq: "seq-1",
        producer: { producerId: "p", producerEpoch: 2, producerSeq: 3 },
      },
    });
    expect(dec.decode(stub.appendCalls[0]!.options.data)).toBe("hi");

    stub.setAppend({ status: "conflict", conflictReason: "closed", nextOffset: "1_0" });
    const closed = await service.execute(
      routeCtx(
        new Request("http://x/s", {
          method: "POST",
          body: "bye",
          headers: { "content-type": "text/plain" },
        }),
      ),
    );
    expect(closed.status).toBe(409);
    expect(closed.headers.get("stream-closed")).toBe("true");
  });
});

describe("ReadHttpService", () => {
  it("returns an offset=now tail response without reading messages", async () => {
    const stub = makeProtocolStub();
    stub.setMetadata({
      status: "ok",
      contentType: "application/json",
      nextOffset: "2_0",
      closed: true,
    });
    const { responses, bodyCodec, longPoll, sse } = deps(stub);
    const service = new ReadHttpService({
      protocol: stub.protocol,
      responses,
      bodyCodec,
      readQuery: new ReadQueryParser(),
      etags: new EtagBuilder(),
      longPoll,
      sse,
    });

    const response = await service.execute(
      routeCtx(new Request("http://x/s?offset=now", { method: "GET" })),
    );
    expect(response.status).toBe(200);
    expect(response.headers.get("stream-next-offset")).toBe("2_0");
    expect(response.headers.get("stream-up-to-date")).toBe("true");
    expect(await response.text()).toBe("[]");
    expect(stub.readCalls).toHaveLength(0);
  });

  it("builds catch-up bodies and honors If-None-Match", async () => {
    const stub = makeProtocolStub();
    stub.setRead({
      status: "ok",
      messages: [message("1_0", "a"), message("2_0", "b")],
      nextOffset: "2_0",
      upToDate: true,
    });
    stub.setMetadata({ status: "ok", contentType: "text/plain", nextOffset: "2_0" });
    const { responses, bodyCodec, longPoll, sse } = deps(stub);
    const service = new ReadHttpService({
      protocol: stub.protocol,
      responses,
      bodyCodec,
      readQuery: new ReadQueryParser(),
      etags: new EtagBuilder(),
      longPoll,
      sse,
    });

    const response = await service.execute(
      routeCtx(new Request("http://x/s?offset=-1", { method: "GET" })),
    );
    expect(response.status).toBe(200);
    expect(response.headers.get("etag")).toBe(`"${btoa("/s")}:-1:2_0"`);
    expect(await response.text()).toBe("ab");

    const notModified = await service.execute(
      routeCtx(
        new Request("http://x/s?offset=-1", {
          method: "GET",
          headers: { "if-none-match": response.headers.get("etag")! },
        }),
      ),
    );
    expect(notModified.status).toBe(304);
  });
});

describe("SseHttpService", () => {
  it("emits initial data and a closed control event", async () => {
    const stub = makeProtocolStub();
    stub.setMetadata({ status: "ok", contentType: "text/plain", nextOffset: "1_0" });
    stub.setRead({
      status: "ok",
      messages: [message("1_0", "hello")],
      nextOffset: "1_0",
      upToDate: true,
      closed: true,
    });
    const service = deps(stub).sse;

    const response = await service.execute("s", "-1");
    expect(response.status).toBe(200);
    expect(response.headers.get("content-type")).toBe("text/event-stream");
    const body = await readStream(response);
    expect(body).toContain("event: data\ndata:hello\n\n");
    expect(body).toContain(
      'event: control\ndata:{"streamNextOffset":"1_0","streamClosed":true}\n\n',
    );
  });

  it("marks binary SSE responses with the base64 data encoding header", async () => {
    const stub = makeProtocolStub();
    stub.setMetadata({
      status: "ok",
      contentType: "application/octet-stream",
      nextOffset: "0_0",
      closed: true,
    });
    const response = await deps(stub).sse.execute("s", "now");
    expect(response.headers.get("stream-sse-data-encoding")).toBe("base64");
    expect(await readStream(response)).toContain("event: control");
  });
});

describe("MetadataHttpService and DeleteHttpService", () => {
  it("maps metadata headers and delete statuses", async () => {
    const stub = makeProtocolStub();
    stub.setMetadata({
      status: "ok",
      contentType: "text/plain",
      nextOffset: "1_0",
      ttlSeconds: 30,
      expiresAt: "2026-01-01T00:00:00Z",
      closed: true,
    });
    const responses = new HttpResponseFactory();
    const metadata = await new MetadataHttpService({ protocol: stub.protocol, responses }).execute(
      "s",
    );
    expect(metadata.status).toBe(200);
    expect(metadata.headers.get("stream-ttl")).toBe("30");
    expect(metadata.headers.get("stream-expires-at")).toBe("2026-01-01T00:00:00Z");
    expect(metadata.headers.get("stream-closed")).toBe("true");

    const deleted = await new DeleteHttpService({ protocol: stub.protocol, responses }).execute(
      "s",
    );
    expect(deleted.status).toBe(204);
    stub.setDelete({ status: "gone" });
    expect(
      (await new DeleteHttpService({ protocol: stub.protocol, responses }).execute("s")).status,
    ).toBe(410);
  });
});

describe("HttpDispatchService", () => {
  it("routes by method and applies security headers", async () => {
    const stub = makeProtocolStub();
    const { responses, bodyCodec, longPoll, sse } = deps(stub);
    const dispatch = new HttpDispatchService({
      path: new StreamPathService("/api"),
      responses,
      create: new CreateHttpService({
        protocol: stub.protocol,
        path: new StreamPathService("/api"),
        responses,
        bodyReader: new RequestBodyReader(1024, responses),
      }),
      append: new AppendHttpService({
        protocol: stub.protocol,
        responses,
        bodyReader: new RequestBodyReader(1024, responses),
        producerHeaders: new ProducerHeaderParser(),
      }),
      read: new ReadHttpService({
        protocol: stub.protocol,
        responses,
        bodyCodec,
        readQuery: new ReadQueryParser(),
        etags: new EtagBuilder(),
        longPoll,
        sse,
      }),
      metadata: new MetadataHttpService({ protocol: stub.protocol, responses }),
      delete: new DeleteHttpService({ protocol: stub.protocol, responses }),
    });

    const created = await dispatch.fetch(new Request("http://x/api/s", { method: "PUT" }));
    expect(created.status).toBe(201);
    expect(created.headers.get("x-content-type-options")).toBe("nosniff");
    expect(stub.createCalls[0]!.streamId).toBe("s");
    expect((await dispatch.fetch(new Request("http://x/api/s", { method: "PATCH" }))).status).toBe(
      405,
    );
    expect((await dispatch.fetch(new Request("http://x/nope", { method: "GET" }))).status).toBe(
      400,
    );
  });

  it("maps service exceptions to 500 responses", async () => {
    class ThrowingCreateService extends CreateHttpService {
      override execute(): Promise<Response> {
        throw new Error("boom");
      }
    }
    const stub = makeProtocolStub();
    const responses = new HttpResponseFactory();
    const { bodyCodec, longPoll, sse } = deps(stub);
    const dispatch = new HttpDispatchService({
      path: new StreamPathService("/"),
      responses,
      create: new ThrowingCreateService({
        protocol: stub.protocol,
        path: new StreamPathService("/"),
        responses,
        bodyReader: new RequestBodyReader(1024, responses),
      }),
      append: new AppendHttpService({
        protocol: stub.protocol,
        responses,
        bodyReader: new RequestBodyReader(1024, responses),
        producerHeaders: new ProducerHeaderParser(),
      }),
      read: new ReadHttpService({
        protocol: stub.protocol,
        responses,
        bodyCodec,
        readQuery: new ReadQueryParser(),
        etags: new EtagBuilder(),
        longPoll,
        sse,
      }),
      metadata: new MetadataHttpService({ protocol: stub.protocol, responses }),
      delete: new DeleteHttpService({ protocol: stub.protocol, responses }),
    });
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      expect((await dispatch.fetch(new Request("http://x/s", { method: "PUT" }))).status).toBe(500);
    } finally {
      errorSpy.mockRestore();
    }
  });
});
