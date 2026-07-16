import type { JsonValue, StreamBatch, StreamBatchMeta } from "./types.ts";

const decoder = new TextDecoder();

/**
 * Real impedance matching: turn a substrate's `StoredMessage[]` into a single
 * content-aware delivery batch, matching `MessageBodyCodec` semantics so direct
 * and remote consumers observe the same payload shape.
 *
 * The media type is derived from the stream content type: `application/json`
 * (parameters ignored) yields a `json` batch, `text/*` a `text` batch, and
 * everything else (including an absent content type) a `bytes` batch.
 *
 * Throws `SyntaxError` on invalid stored JSON; callers map that to a
 * `parse-error` failure.
 */
export function encodeBatch<T extends JsonValue>(
  contentType: string,
  messages: readonly { data: Uint8Array }[],
  meta: StreamBatchMeta,
): StreamBatch<T> {
  const mediaType = contentType.split(";", 1)[0]!.trim().toLowerCase();
  if (mediaType === "application/json") {
    const body = `[${messages.map((message) => decoder.decode(message.data)).join(",")}]`;
    return { kind: "json", items: JSON.parse(body) as T[], ...meta };
  }
  if (mediaType.startsWith("text/")) {
    return {
      kind: "text",
      text: messages.map((message) => decoder.decode(message.data)).join(""),
      ...meta,
    };
  }
  const length = messages.reduce((total, message) => total + message.data.byteLength, 0);
  const data = new Uint8Array(length);
  let position = 0;
  for (const message of messages) {
    data.set(message.data, position);
    position += message.data.byteLength;
  }
  return { kind: "bytes", data, ...meta };
}
