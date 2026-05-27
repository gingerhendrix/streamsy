import { randomUUID } from "node:crypto";
import { resolve } from "node:path";

const packageDir = resolve(import.meta.dir, "..");
const port = 20_000 + Math.floor(Math.random() * 20_000);
const baseUrl = `http://127.0.0.1:${port}`;
const streamId = `smoke/${Date.now()}-${randomUUID()}`;
const streamUrl = `${baseUrl}/${streamId}`;

class SmokeError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SmokeError";
  }
}

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) {
    throw new SmokeError(message);
  }
}

async function text(response: Response): Promise<string> {
  return await response.text();
}

function requiredHeader(response: Response, name: string): string {
  const value = response.headers.get(name);
  assert(value, `Expected response header ${name}`);
  return value;
}

async function request(input: string, init?: RequestInit): Promise<Response> {
  const response = await fetch(input, init);
  return response;
}

async function waitForServer(): Promise<void> {
  const deadline = Date.now() + 10_000;
  let lastError: unknown;

  while (Date.now() < deadline) {
    try {
      const response = await fetch(baseUrl);
      if (response.status === 400 || response.status === 404) {
        return;
      }
    } catch (error) {
      lastError = error;
    }
    await Bun.sleep(100);
  }

  throw new SmokeError(`Memory server did not become ready: ${lastError}`);
}

async function readSseUntilControl(response: Response): Promise<string> {
  assert(response.body, "Expected SSE response body");

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let output = "";
  const deadline = Date.now() + 5_000;

  try {
    while (Date.now() < deadline) {
      const remaining = Math.max(deadline - Date.now(), 1);
      const result = await Promise.race([
        reader.read(),
        Bun.sleep(remaining).then(() => ({ done: true, value: undefined })),
      ]);

      if (result.value) {
        output += decoder.decode(result.value, { stream: true });
      }

      if (output.includes("event: data") && output.includes("event: control")) {
        return output;
      }

      if (result.done) {
        break;
      }
    }
  } finally {
    await reader.cancel().catch(() => undefined);
  }

  throw new SmokeError(`Timed out waiting for SSE data/control events. Received:\n${output}`);
}

const server = Bun.spawn(["bun", "src/index.ts"], {
  cwd: packageDir,
  env: { ...process.env, PORT: String(port) },
  stdout: "pipe",
  stderr: "pipe",
});

try {
  await waitForServer();

  const createResponse = await request(streamUrl, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
  });
  assert(createResponse.status === 201, `create status ${createResponse.status}`);
  requiredHeader(createResponse, "stream-next-offset");

  const appendResponse = await request(streamUrl, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ type: "hello", n: 1 }),
  });
  assert(appendResponse.status === 204, `append status ${appendResponse.status}`);
  const firstOffset = requiredHeader(appendResponse, "stream-next-offset");

  const readResponse = await request(`${streamUrl}?offset=-1`);
  assert(readResponse.status === 200, `read status ${readResponse.status}`);
  assert(readResponse.headers.get("stream-up-to-date") === "true", "read should be up to date");
  const readOffset = requiredHeader(readResponse, "stream-next-offset");
  assert(readOffset === firstOffset, "read next offset should match append offset");
  const etag = requiredHeader(readResponse, "etag");
  const readBody = await text(readResponse);
  assert(readBody === '[{"type":"hello","n":1}]', `unexpected read body ${readBody}`);

  const notModifiedResponse = await request(`${streamUrl}?offset=-1`, {
    headers: { "If-None-Match": etag },
  });
  assert(notModifiedResponse.status === 304, `If-None-Match status ${notModifiedResponse.status}`);

  const longPollPromise = request(`${streamUrl}?offset=${firstOffset}&live=long-poll`);
  await Bun.sleep(100);
  const secondAppendResponse = await request(streamUrl, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ type: "hello", n: 2 }),
  });
  assert(
    secondAppendResponse.status === 204,
    `second append status ${secondAppendResponse.status}`,
  );

  const longPollResponse = await longPollPromise;
  assert(longPollResponse.status === 200, `long-poll success status ${longPollResponse.status}`);
  requiredHeader(longPollResponse, "stream-cursor");
  const secondOffset = requiredHeader(longPollResponse, "stream-next-offset");
  const longPollBody = await text(longPollResponse);
  assert(longPollBody === '[{"type":"hello","n":2}]', `unexpected long-poll body ${longPollBody}`);

  const sseResponse = await request(`${streamUrl}?offset=-1&live=sse`);
  assert(sseResponse.status === 200, `SSE status ${sseResponse.status}`);
  assert(
    sseResponse.headers.get("content-type")?.startsWith("text/event-stream"),
    `unexpected SSE content-type ${sseResponse.headers.get("content-type")}`,
  );
  const sseText = await readSseUntilControl(sseResponse);
  assert(sseText.includes("event: data"), "SSE should include a data event");
  assert(sseText.includes("event: control"), "SSE should include a control event");
  assert(
    sseText.includes(`"streamNextOffset":"${secondOffset}"`),
    "SSE control should include the current next offset",
  );
  assert(sseText.includes('"upToDate":true'), "SSE control should mark upToDate");

  console.log(`memory-server HTTP smoke passed for ${streamId}`);
} finally {
  server.kill();
  await server.exited.catch(() => undefined);

  const stdout = await new Response(server.stdout).text();
  const stderr = await new Response(server.stderr).text();
  if (stdout.trim()) {
    console.log(stdout.trim());
  }
  if (stderr.trim()) {
    console.error(stderr.trim());
  }
}
