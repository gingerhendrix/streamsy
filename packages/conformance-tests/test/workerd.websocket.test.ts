/* oxlint-disable effecttsgo/async-function, effecttsgo/new-promise, effecttsgo/global-timers, effecttsgo/global-date -- This suite drives real sockets against local workerd, whose idle timer runs on wall-clock time. */
import { dirname, resolve as resolvePath } from "node:path";
import { fileURLToPath } from "node:url";
import { Schema } from "effect";
import { Miniflare } from "miniflare";
import { afterAll, beforeAll, describe, expect, test } from "vitest";

const workerPath = resolvePath(
  dirname(fileURLToPath(import.meta.url)),
  "../dist/live-worker/worker.js",
);

const PROTOCOL = "streamsy-live.v1";

let miniflare: Miniflare;

let origin = "";

/** The JSON text frames of `streamsy-live.v1`: `open` and `control`. */
const ServerFrame = Schema.Struct({
  type: Schema.String,
  protocol: Schema.optional(Schema.String),
  contentType: Schema.optional(Schema.String),
  streamNextOffset: Schema.optional(Schema.String),
  streamCursor: Schema.optional(Schema.String),
  upToDate: Schema.optional(Schema.Boolean),
  streamClosed: Schema.optional(Schema.Boolean),
});

type ServerFrame = typeof ServerFrame.Type;

const decodeFrame = Schema.decodeUnknownSync(Schema.fromJsonString(ServerFrame));

type Frame =
  | { readonly kind: "text"; readonly text: string }
  | { readonly kind: "binary"; readonly bytes: Uint8Array };

interface Closed {
  readonly code: number;
  readonly reason: string;
}

/** A client socket that queues frames, so a test can read them in order. */
class Client {
  readonly frames: Array<Frame> = [];
  readonly #waiters: Array<() => void> = [];
  readonly closed: Promise<Closed>;

  constructor(readonly ws: WebSocket) {
    ws.binaryType = "arraybuffer";
    ws.addEventListener("message", (event) => {
      const data: unknown = event.data;

      this.frames.push(
        data instanceof ArrayBuffer
          ? { kind: "binary", bytes: new Uint8Array(data) }
          : { kind: "text", text: String(data) },
      );
      this.#wake();
    });
    this.closed = new Promise((done) =>
      ws.addEventListener("close", (event) => {
        done({ code: event.code, reason: event.reason });
        this.#wake();
      }),
    );
  }

  #wake() {
    for (const waiter of this.#waiters.splice(0)) waiter();
  }

  /** The next frame, or a failure after `timeoutMs`. */
  async next(timeoutMs = 5_000): Promise<Frame> {
    const deadline = Date.now() + timeoutMs;

    while (this.frames.length === 0) {
      if (this.ws.readyState === WebSocket.CLOSED) throw new Error("socket closed");
      const left = deadline - Date.now();

      if (left <= 0) throw new Error("no frame before the timeout");
      await new Promise<void>((done) => {
        const timer = setTimeout(done, left);
        this.#waiters.push(() => {
          clearTimeout(timer);
          done();
        });
      });
    }

    const frame = this.frames.shift();

    if (frame === undefined) throw new Error("no frame");

    return frame;
  }

  async text(): Promise<ServerFrame> {
    const frame = await this.next();

    if (frame.kind !== "text") throw new Error("expected a text frame");

    return decodeFrame(frame.text);
  }

  async data(): Promise<Uint8Array> {
    const frame = await this.next();

    if (frame.kind !== "binary") throw new Error(`expected a data frame, got ${frame.text}`);

    return frame.bytes;
  }

  /** Reads `open` and the controls of the catch-up until `upToDate`. */
  async ready(): Promise<ServerFrame> {
    expect((await this.text()).type).toBe("open");

    for (;;) {
      const frame = await this.next();

      if (frame.kind === "text") {
        const control = decodeFrame(frame.text);

        if (control.upToDate === true) return control;
      }
    }
  }

  close() {
    this.ws.close(1000);
  }
}

const socketUrl = (path: string) => `${origin.replace(/^http/, "ws")}${path}`;

const open = (path: string, protocols: ReadonlyArray<string> = [PROTOCOL]) =>
  new Promise<Client>((done, reject) => {
    const ws = new WebSocket(socketUrl(path), [...protocols]);
    const client = new Client(ws);
    ws.addEventListener("open", () => done(client), { once: true });
    ws.addEventListener("error", () => reject(new Error(`upgrade failed: ${path}`)), {
      once: true,
    });
  });

const http = (path: string, init?: RequestInit) => fetch(`${origin}${path}`, init);

/** An upgrade request whose answer is not a socket. */
const upgrade = async (path: string) => {
  const response = await miniflare.dispatchFetch(`${origin}${path}`, {
    headers: { upgrade: "websocket" },
  });

  await response.arrayBuffer();

  return response.status;
};

const create = async (
  path: string,
  contentType = "application/json",
  body?: string | Uint8Array,
) => {
  const response = await http(path, {
    method: "PUT",
    headers: { "content-type": contentType },
    body,
  });

  expect([200, 201]).toContain(response.status);
};

const append = async (
  path: string,
  body: string | Uint8Array,
  contentType = "application/json",
) => {
  const response = await http(path, {
    method: "POST",
    headers: { "content-type": contentType },
    body,
  });

  expect(response.status).toBe(204);

  return response.headers.get("stream-next-offset");
};

const ObjectState = Schema.Struct({
  instance: Schema.String,
  constructions: Schema.Finite,
  messages: Schema.Finite,
  reads: Schema.Finite,
});

const decodeObjectState = Schema.decodeUnknownPromise(ObjectState);

const objectState = async (stream: string) => {
  const response = await http(`/__test/state?stream=${stream}`);

  return decodeObjectState(await response.json());
};

const decoder = new TextDecoder();

let counter = 0;

/** A fresh group per call, so each test uses its own object. */
const unique = (name: string) => `${name}-${Date.now()}-${(counter += 1)}`;

describe("live=websocket on the workerd Durable Object host", () => {
  beforeAll(async () => {
    if (!(await Bun.file(workerPath).exists()))
      throw new Error("Missing live worker; run bun run build:worker first");

    miniflare = new Miniflare({
      scriptPath: workerPath,
      modules: true,
      compatibilityDate: "2026-07-30",
      compatibilityFlags: ["nodejs_compat"],
      host: "127.0.0.1",
      port: 0,
      cf: false,
      durableObjects: {
        LIVE: { className: "LiveObject", useSQLite: true },
        SMALL: { className: "SmallObject", useSQLite: true },
        PLAIN: { className: "PlainObject", useSQLite: true },
      },
    });

    origin = (await miniflare.ready).origin;
  });

  afterAll(() => miniflare?.dispose());

  test("1. the handshake echoes the subprotocol and sends open first", async () => {
    const path = `/${unique("t1")}/s`;
    await create(path);
    const client = await open(`${path}?offset=now&live=websocket`);
    expect(client.ws.protocol).toBe(PROTOCOL);

    const first = await client.text();
    expect(first).toEqual({
      type: "open",
      protocol: PROTOCOL,
      contentType: "application/json",
      streamNextOffset: expect.any(String),
    });

    const control = await client.text();
    expect(control).toMatchObject({ type: "control", upToDate: true });
    expect(control.streamCursor).toEqual(expect.any(String));
    client.close();
  });

  test.each([
    ["application/json", '[{"a":1},{"b":2}]', '"c"'],
    ["text/plain", "hello ", "world"],
    ["application/octet-stream", new Uint8Array([0, 1, 2, 255]), new Uint8Array([7, 8])],
  ] as const)("2. catch-up for %s equals the HTTP catch-up", async (type, initial, more) => {
    const path = `/${unique("t2")}/s`;
    await create(path, type, initial);
    await append(path, more, type);

    const expected = await http(`${path}?offset=-1`);
    const body = new Uint8Array(await expected.arrayBuffer());
    const client = await open(`${path}?offset=-1&live=websocket`);
    expect(await client.text()).toMatchObject({ type: "open", contentType: type });
    expect(await client.data()).toEqual(body);

    const control = await client.text();
    expect(control).toMatchObject({
      type: "control",
      streamNextOffset: expected.headers.get("stream-next-offset"),
      upToDate: true,
    });
    client.close();
  });

  test("3. errors before the upgrade are HTTP answers", async () => {
    const group = unique("t3");
    const path = `/${group}/s`;
    await create(path);

    expect(await upgrade(`${path}?offset=bad&live=websocket`)).toBe(400);
    expect(await upgrade(`${path}?live=websocket`)).toBe(400);
    expect(await upgrade(`/${group}/missing?offset=-1&live=websocket`)).toBe(404);

    const source = `/${group}/source`;
    await create(source);

    const fork = await http(`/${group}/fork`, {
      method: "PUT",
      headers: { "content-type": "application/json", "stream-forked-from": source },
    });

    expect(fork.status).toBe(201);
    expect((await http(source, { method: "DELETE" })).status).toBe(204);
    expect(await upgrade(`${source}?offset=-1&live=websocket`)).toBe(410);

    const plain = await http(`${path}?offset=now&live=websocket`);
    expect(plain.status).toBe(426);

    const disabled = `/plain/${group}/s`;
    await create(disabled);
    expect(await upgrade(`${disabled}?offset=now&live=websocket`)).toBe(426);
  });

  test("4. an HTTP append pushes data and then control", async () => {
    const path = `/${unique("t4")}/s`;
    await create(path);
    const client = await open(`${path}?offset=now&live=websocket`);
    await client.ready();

    const offset = await append(path, '{"n":1}');
    expect(decoder.decode(await client.data())).toBe('[{"n":1}]');
    expect(await client.text()).toMatchObject({
      type: "control",
      streamNextOffset: offset,
      upToDate: true,
    });
    client.close();
  });

  test("5. a commit from an alarm pushes to the stream's sockets", async () => {
    await create("/alarm/target", "text/plain");
    const client = await open("/alarm/target?offset=now&live=websocket");
    await client.ready();

    // The trigger arms the alarm. The wakeup appends to the target and removes the trigger.
    await create("/alarm/trigger", "text/plain");
    expect(decoder.decode(await client.data())).toBe("from-alarm");
    expect(await client.text()).toMatchObject({ type: "control", upToDate: true });
    client.close();
  });

  test("6. fan-out reads once for sockets at one offset", async () => {
    const path = `/${unique("t6")}/s`;
    await create(path);

    const clients = await Promise.all(
      Array.from({ length: 50 }, () => open(`${path}?offset=now&live=websocket`)),
    );

    for (const client of clients) await client.ready();
    await http(`/__test/reset-reads?stream=${path.slice(1)}`);
    await append(path, "1");

    for (const client of clients) {
      expect(decoder.decode(await client.data())).toBe("[1]");
      expect(await client.text()).toMatchObject({ type: "control" });
    }

    expect((await objectState(path.slice(1))).reads).toBe(1);

    for (const client of clients) client.close();
  });

  test("7. a reconnect at the last control offset has no gap and no duplicate", async () => {
    const path = `/${unique("t7")}/s`;
    await create(path, "application/json", "[0]");
    const first = await open(`${path}?offset=-1&live=websocket`);
    const control = await first.ready();
    first.close();
    await first.closed;

    for (const n of [1, 2, 3]) await append(path, String(n));

    const offset = String(control.streamNextOffset);
    const cursor = String(control.streamCursor);
    const second = await open(`${path}?offset=${offset}&cursor=${cursor}&live=websocket`);
    expect(await second.text()).toMatchObject({ type: "open" });
    expect(decoder.decode(await second.data())).toBe("[1,2,3]");
    expect(await second.text()).toMatchObject({ type: "control", upToDate: true });
    second.close();
  });

  test("8. a closed stream ends with 1000 and a deleted stream with 4404 or 4410", async () => {
    const group = unique("t8");
    const closing = `/${group}/closing`;
    await create(closing);
    const client = await open(`${closing}?offset=now&live=websocket`);
    await client.ready();

    const closed = await http(closing, { method: "POST", headers: { "stream-closed": "true" } });
    expect(closed.status).toBe(204);

    const control = await client.text();
    expect(control).toMatchObject({ type: "control", streamClosed: true });
    expect(control.streamCursor).toBeUndefined();
    expect((await client.closed).code).toBe(1000);

    const deleted = `/${group}/deleted`;
    await create(deleted);
    const reader = await open(`${deleted}?offset=now&live=websocket`);
    await reader.ready();
    expect((await http(deleted, { method: "DELETE" })).status).toBe(204);
    expect([4404, 4410]).toContain((await reader.closed).code);
  });

  test("9. byte and socket caps", async () => {
    const group = unique("t9");
    const big = `/small/${group}/big`;
    await create(big, "application/json", JSON.stringify(["x".repeat(100)]));

    const behind = await open(`${big}?offset=-1&live=websocket`);
    expect(await behind.text()).toMatchObject({ type: "open" });
    expect(await behind.text()).toMatchObject({ type: "control", streamNextOffset: "-1" });
    expect((await behind.closed).code).toBe(4100);

    const pushed = `/small/${group}/pushed`;
    await create(pushed);
    const slow = await open(`${pushed}?offset=now&live=websocket`);
    await slow.ready();
    await append(pushed, JSON.stringify("y".repeat(100)));
    expect((await slow.closed).code).toBe(4100);

    const full = `/small/${group}/full`;
    await create(full);

    const sockets = [
      await open(`${full}?offset=now&live=websocket`),
      await open(`${full}?offset=now&live=websocket`),
    ];

    expect(await upgrade(`${full}?offset=now&live=websocket`)).toBe(503);

    for (const socket of sockets) socket.close();
  });

  test("10. the runtime answers ping without waking the object", async () => {
    const path = `/${unique("t10")}/s`;
    await create(path);
    const client = await open(`${path}?offset=now&live=websocket`);
    await client.ready();

    const before = await objectState(path.slice(1));
    client.ws.send("ping");
    expect(await client.next()).toMatchObject({ kind: "text", text: "pong" });

    const after = await objectState(path.slice(1));
    expect(after.messages).toBe(before.messages);
    expect(after.constructions).toBe(before.constructions);
    client.close();
  });

  test("11. any other client message closes with 4400", async () => {
    const path = `/${unique("t11")}/s`;
    await create(path);
    const client = await open(`${path}?offset=now&live=websocket`);
    await client.ready();
    client.ws.send("hello");
    expect((await client.closed).code).toBe(4400);
  });

  test("12. an unknown attachment version closes with 1012 on the next push", async () => {
    const path = `/${unique("t12")}/s`;
    await create(path);
    const client = await open(`${path}?offset=now&live=websocket`);
    await client.ready();
    await http(`/__test/attachments?stream=${path.slice(1)}&v=0`);
    await append(path, "1");
    expect((await client.closed).code).toBe(1012);
  });

  // Slow: workerd's idle timer is outside the test clock, so this waits in real time.
  test("13. an object with only idle sockets hibernates, and one with a parked reader does not", async () => {
    const idle = unique("t13");
    const busy = unique("t13busy");
    await create(`/${idle}/s`);
    await create(`/${busy}/s`);

    const client = await open(`/${idle}/s?offset=now&live=websocket`);
    await client.ready();
    const watcher = await open(`/${busy}/s?offset=now&live=websocket`);
    await watcher.ready();

    // The control: an SSE reader keeps a request and a timer open on the busy object.
    const sse = await http(`/${busy}/s?offset=now&live=sse`);
    const body = sse.body?.getReader();

    const before = await objectState(idle);
    const busyBefore = await objectState(busy);
    await new Promise((done) => setTimeout(done, 15_000));

    const offset = await append(`/${idle}/s`, '"after"');
    expect(decoder.decode(await client.data())).toBe('["after"]');
    expect(await client.text()).toMatchObject({ streamNextOffset: offset });

    const after = await objectState(idle);
    expect(after.instance).not.toBe(before.instance);
    expect((await objectState(busy)).instance).toBe(busyBefore.instance);

    await body?.cancel();
    client.close();
    watcher.close();
  }, 60_000);
});
