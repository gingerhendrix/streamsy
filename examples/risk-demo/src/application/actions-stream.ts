/* oxlint-disable effecttsgo/global-timers, effecttsgo/new-promise -- The public action-notification Promise facade owns a cancellable subscription wait and preserves its existing caller contract. */
import { Schema } from "effect";
/**
 * The wire contract of a player's actions stream, in one place: the framing the
 * server writes and the parser every first-party client reads it with.
 *
 * The resource is `text/event-stream`. It is deliberately *finite*: the server
 * holds a connection for `ACTIONS_STREAM_TIMEOUT_MS` and then closes it, and a
 * client reconnects from the `nextOffset` it last saw. Resume is therefore exact
 * — the durable action-stream offset is the only position state — and a stalled
 * connection cannot silently outlive the game it belongs to.
 *
 * Framing follows Streamsy core's SSE conventions (`packages/core/src/http/
 * sse-event-encoder.ts`): a `data` event carries a JSON array split one element
 * per `data:` line, and a `control` event carries this resource's cursor. The
 * control payload is the demo's own — `nextOffset`/`upToDate`/`closed` name what
 * the agent API already called them — rather than core's `streamNextOffset`,
 * because an agent resumes this resource with `?offset=`, not core's raw reads.
 *
 * `EventSource` cannot send `Authorization`, and this resource is capability-
 * gated with the token never in the URL, so clients use `fetch` plus the parser
 * below rather than the browser primitive.
 */

/**
 * How long the server holds one actions connection before closing it, and the
 * bound every first-party client sizes its own abort timer against. One
 * constant, imported by the route, the bot, the tests and the docs.
 */
export const ACTIONS_STREAM_TIMEOUT_MS = 30_000;

/**
 * What a *client* arms its own abort timer with.
 *
 * A client timer is necessarily armed before the request is issued, so it is
 * already running through connect, TLS, auth and the server's catch-up work. Set
 * to the server's bound exactly, it would fire a hair *before* the server closes
 * — turning every idle connection into a client-side abort, which loses the
 * closing control frame and the cursor it re-states. The slack makes the server
 * the party that ends an idle connection, and leaves the client timer as what it
 * is meant to be: the guard for a server that never closes at all.
 */
export const ACTIONS_STREAM_CLIENT_TIMEOUT_MS = ACTIONS_STREAM_TIMEOUT_MS + 5_000;

/** The `control` event's payload: where to resume, and whether this is the end. */
export interface ActionsControl {
  /** Opaque cursor to send as `?offset=` on the next connection. */
  nextOffset: string;
  upToDate: boolean;
  /** Present only on the terminal `GameOver` control: nothing more will arrive. */
  closed?: boolean;
}

export const ActionsControlSchema = Schema.Struct({
  nextOffset: Schema.String,
  upToDate: Schema.Boolean,
  closed: Schema.optionalKey(Schema.Boolean),
});

/** One `data` event and the `control` event that closes it. */
export interface ActionsBatch<T = unknown> {
  messages: T[];
  nextOffset: string;
  upToDate: boolean;
  closed: boolean;
}

/** `event: data` carrying a JSON array, one element per `data:` line. */
export function actionsDataFrame(messages: readonly unknown[]): string {
  const lines = messages.map(
    (message, index) => `data:${JSON.stringify(message)}${index < messages.length - 1 ? "," : ""}`,
  );
  return `event: data\ndata:[\n${lines.map((line) => `${line}\n`).join("")}data:]\n\n`;
}

/** `event: control` carrying the resume cursor for everything written so far. */
export function actionsControlFrame(control: ActionsControl): string {
  return `event: control\ndata:${JSON.stringify(control)}\n\n`;
}

export interface SseFrame {
  event: string;
  data: string;
}

export interface SseParser {
  push(chunk: string): SseFrame[];
}

/**
 * Incremental SSE parser.
 *
 * Network chunk boundaries fall wherever they like — mid-line, mid-frame, or
 * exactly between two frames — so the parser buffers the trailing partial line
 * and dispatches only on a blank line. Multi-line `data:` fields are joined with
 * newlines, per the EventSource specification, which is what makes the array
 * framing above legal rather than merely conventional.
 */
export function createSseParser(): SseParser {
  let buffer = "";
  let event = "";
  let data: string[] = [];
  return {
    push(chunk: string): SseFrame[] {
      buffer += chunk;
      const frames: SseFrame[] = [];
      // Keep the trailing fragment: it is a complete line only once a newline
      // arrives, which may be in a later chunk.
      const lines = buffer.split("\n");
      buffer = lines.pop() ?? "";
      for (const raw of lines) {
        const line = raw.endsWith("\r") ? raw.slice(0, -1) : raw;
        if (line === "") {
          if (data.length > 0 || event !== "") {
            frames.push({ event: event || "message", data: data.join("\n") });
          }
          event = "";
          data = [];
          continue;
        }
        if (line.startsWith(":")) continue; // comment / heartbeat
        const colon = line.indexOf(":");
        const field = colon === -1 ? line : line.slice(0, colon);
        let value = colon === -1 ? "" : line.slice(colon + 1);
        if (value.startsWith(" ")) value = value.slice(1);
        if (field === "event") event = value;
        else if (field === "data") data.push(value);
      }
      return frames;
    },
  };
}

/**
 * Turn parsed frames into batches. A `data` event's messages are held until the
 * `control` event that names the offset they read through: a client must never
 * advance its cursor past messages it has not also received.
 */
export interface ActionsDecoder<T> {
  push(chunk: string): ActionsBatch<T>[];
}

export function createActionsDecoder(): ActionsDecoder<unknown>;
export function createActionsDecoder<T>(schema: Schema.Decoder<T>): ActionsDecoder<T>;
export function createActionsDecoder(
  schema: Schema.Decoder<unknown> = Schema.Unknown,
): ActionsDecoder<unknown> {
  const parser = createSseParser();
  let pending: unknown[] = [];
  const decodeMessages = Schema.decodeUnknownSync(Schema.Array(schema));
  const decodeControl = Schema.decodeUnknownSync(ActionsControlSchema);
  return {
    push(chunk: string): ActionsBatch<unknown>[] {
      const batches: ActionsBatch<unknown>[] = [];
      for (const frame of parser.push(chunk)) {
        if (frame.event === "data") {
          pending = pending.concat(decodeMessages(JSON.parse(frame.data)));
          continue;
        }
        if (frame.event !== "control") continue;
        const control = decodeControl(JSON.parse(frame.data));
        batches.push({
          messages: pending,
          nextOffset: control.nextOffset,
          upToDate: control.upToDate,
          closed: control.closed === true,
        });
        pending = [];
      }
      return batches;
    },
  };
}

export interface ActionsReader<T = unknown> {
  /** The next batch, or `null` if none arrived within `timeoutMs` or the stream ended. */
  next(timeoutMs: number): Promise<ActionsBatch<T> | null>;
  /** Drop the connection and wait for the in-flight read to actually settle. */
  close(): Promise<void>;
}

const TIMED_OUT = Symbol("timed-out");

/**
 * A reader for callers that need to observe *holding* — "nothing arrived in this
 * long" — rather than simply consuming a stream to its end.
 *
 * Two things this gets right that a bare `Promise.race` does not. A read that
 * loses the race is retained rather than abandoned, so the batch it eventually
 * carries is delivered to the next call instead of being dropped on the floor.
 * And `close` aborts the underlying connection and then *awaits* that read's
 * settlement, so a caller that closes has proof the reader finished — cancelling
 * a body while a reader still holds the lock silently does nothing.
 */
export function createActionsReader(
  response: Response,
  connection: AbortController,
): ActionsReader<unknown>;
export function createActionsReader<T>(
  response: Response,
  connection: AbortController,
  schema: Schema.Decoder<T>,
): ActionsReader<T>;
export function createActionsReader(
  response: Response,
  connection: AbortController,
  schema: Schema.Decoder<unknown> = Schema.Unknown,
): ActionsReader<unknown> {
  const batches = readActionsBatches(response, schema);
  let pending: Promise<ActionsBatch<unknown> | null> | null = null;
  const advance = (): Promise<ActionsBatch<unknown> | null> => {
    pending ??= batches
      .next()
      .then((result) => (result.done ? null : result.value))
      // An aborted or ended body is how this reader stops; it is not a failure.
      .catch(() => null)
      .finally(() => {
        pending = null;
      });
    return pending;
  };
  return {
    // oxlint-disable-next-line effecttsgo/async-function -- AsyncIterator compatibility requires a Promise-returning next method at this public SSE facade.
    async next(timeoutMs: number): Promise<ActionsBatch<unknown> | null> {
      const arrival = advance();
      let timer: ReturnType<typeof setTimeout> | undefined;
      const expiry = new Promise<typeof TIMED_OUT>((resolve) => {
        timer = setTimeout(() => resolve(TIMED_OUT), timeoutMs);
      });
      try {
        const outcome = await Promise.race([arrival, expiry]);
        return outcome === TIMED_OUT ? null : outcome;
      } finally {
        clearTimeout(timer);
      }
    },
    // oxlint-disable-next-line effecttsgo/async-function -- The public reader close contract waits for the Promise-native Web stream teardown.
    async close(): Promise<void> {
      connection.abort();
      await pending?.catch(() => {});
      // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- The generic SSE facade preserves the caller-selected message type; callers that consume domain messages own the matching decoder contract.
      await batches.return(undefined).catch(() => {});
    },
  };
}

/** Read one actions response to its end, yielding each batch as it lands. */
// oxlint-disable-next-line effecttsgo/async-function -- AsyncGenerator is the public Web-stream compatibility contract consumed by browser and agent clients.
export function readActionsBatches(response: Response): AsyncGenerator<ActionsBatch<unknown>>;
export function readActionsBatches<T>(
  response: Response,
  schema: Schema.Decoder<T>,
): AsyncGenerator<ActionsBatch<T>>;
export async function* readActionsBatches(
  response: Response,
  schema: Schema.Decoder<unknown> = Schema.Unknown,
): AsyncGenerator<ActionsBatch<unknown>, void, unknown> {
  const body = response.body;
  if (!body) return;
  const reader = body.getReader();
  const decoder = new TextDecoder();
  const actions = createActionsDecoder(schema);
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      for (const batch of actions.push(decoder.decode(value, { stream: true }))) yield batch;
    }
  } finally {
    reader.cancel().catch(() => {});
  }
}
