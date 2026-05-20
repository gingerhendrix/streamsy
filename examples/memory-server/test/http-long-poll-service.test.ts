/**
 * Focused coverage for LongPollHttpService.
 *
 * In particular, the "messages-over-timeout" regression: LiveReadService can
 * return `status: "timeout"` together with non-empty messages when a write
 * lands during the wait. The HTTP layer must surface a 200 with the body in
 * that case rather than a 204, so producers don't lose data through a long
 * poll round trip.
 */

import { describe, expect, it } from "vitest";
import type {
  AppendResult,
  CreateResult,
  DeleteResult,
  MetadataResult,
  ReadLiveOptions,
  ReadLiveResult,
  ReadResult,
  StreamProtocolInterface,
} from "../../../packages/core/src/types/protocol.ts";
import type { StoredMessage } from "../../../packages/core/src/types/storage.ts";
import { MessageBodyCodec } from "../../../packages/core/src/http/message-body-codec.ts";
import { HttpResponseFactory } from "../../../packages/core/src/http/responses.ts";
import { LongPollHttpService } from "../../../packages/core/src/http/services/long-poll-http-service.ts";

const enc = new TextEncoder();

function notImplemented(method: string): never {
  throw new Error(`protocol.${method} not implemented in this stub`);
}

interface Stub {
  protocol: StreamProtocolInterface;
  liveCalls: ReadLiveOptions[];
  setLive(result: ReadLiveResult): void;
  setMetadata(result: MetadataResult): void;
}

function makeStub(): Stub {
  let liveResult: ReadLiveResult = {
    status: "ok",
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
  const liveCalls: ReadLiveOptions[] = [];
  const protocol: StreamProtocolInterface = {
    create(): Promise<CreateResult> {
      return notImplemented("create");
    },
    append(): Promise<AppendResult> {
      return notImplemented("append");
    },
    read(): Promise<ReadResult> {
      return notImplemented("read");
    },
    async readLive(_streamId, options) {
      liveCalls.push(options);
      return liveResult;
    },
    async metadata() {
      return metadataResult;
    },
    delete(): Promise<DeleteResult> {
      return notImplemented("delete");
    },
  };
  return {
    protocol,
    liveCalls,
    setLive(result) {
      liveResult = result;
    },
    setMetadata(result) {
      metadataResult = result;
    },
  };
}

function makeMessage(offset: string, body: string): StoredMessage {
  return {
    offset,
    data: enc.encode(body),
    timestamp: Date.now(),
  };
}

function makeService(stub: Stub): LongPollHttpService {
  return new LongPollHttpService({
    protocol: stub.protocol,
    responses: new HttpResponseFactory(),
    bodyCodec: new MessageBodyCodec(),
  });
}

describe("LongPollHttpService", () => {
  it("returns 204 when the live read returns no messages", async () => {
    const stub = makeStub();
    stub.setLive({
      status: "timeout",
      messages: [],
      nextOffset: "0_0",
      upToDate: true,
      cursor: "c1",
    });
    const response = await makeService(stub).execute("s", "-1");
    expect(response.status).toBe(204);
    expect(response.headers.get("stream-up-to-date")).toBe("true");
    expect(response.headers.get("stream-cursor")).toBe("c1");
  });

  it("omits the stream-cursor header and sets stream-closed when the stream is closed", async () => {
    const stub = makeStub();
    stub.setLive({
      status: "ok",
      messages: [],
      nextOffset: "0_0",
      upToDate: true,
      cursor: "c1",
      closed: true,
    });
    const response = await makeService(stub).execute("s", "-1");
    expect(response.status).toBe(204);
    expect(response.headers.get("stream-cursor")).toBeNull();
    expect(response.headers.get("stream-closed")).toBe("true");
  });

  it("returns 200 with the body when timeout races with messages", async () => {
    // Regression for the LiveReadService race where wait.status==="timeout"
    // can coincide with messages picked up by the post-wait readOwn. The HTTP
    // layer must prefer the data over the timeout status.
    const stub = makeStub();
    stub.setMetadata({ status: "ok", contentType: "text/plain", nextOffset: "1_0" });
    stub.setLive({
      status: "timeout",
      messages: [makeMessage("1_0", "late-write")],
      nextOffset: "1_0",
      upToDate: true,
      cursor: "c2",
    });
    const response = await makeService(stub).execute("s", "0_0");
    expect(response.status).toBe(200);
    expect(response.headers.get("content-type")).toBe("text/plain");
    expect(response.headers.get("stream-cursor")).toBe("c2");
    expect(await response.text()).toBe("late-write");
  });

  it("maps not-found and gone protocol results to the documented HTTP statuses", async () => {
    const stub = makeStub();
    stub.setLive({ status: "not-found", messages: [], nextOffset: "", upToDate: false, cursor: "" });
    expect((await makeService(stub).execute("s", "-1")).status).toBe(404);

    stub.setLive({ status: "gone", messages: [], nextOffset: "", upToDate: false, cursor: "" });
    expect((await makeService(stub).execute("s", "-1")).status).toBe(410);
  });

  it("forwards offset, mode, and cursor to readLive", async () => {
    const stub = makeStub();
    await makeService(stub).execute("s", "0_0", "cursor-in");
    expect(stub.liveCalls).toEqual([{ offset: "0_0", mode: "long-poll", cursor: "cursor-in" }]);
  });
});
