import type { JsonValue, StreamBatch, StreamBatchMeta } from "./types.ts";

const decoder = new TextDecoder();

/**
 * Decodes the JSON batch body into its items.
 *
 * `JSON.parse` is typed `any`, so the decoded value is validated as a JSON
 * array before it is used. The element type `T` is the caller's declared
 * contract for the stream's payloads and cannot be checked at runtime, so the
 * narrowing to `T[]` is asserted once, here, behind that array check.
 */
function decodeJsonItems<T extends JsonValue>(body: string): T[] {
  const parsed: JsonValue = JSON.parse(body);
  if (!Array.isArray(parsed)) throw new SyntaxError("Stored JSON batch is not an array");
  // SAFETY: JSON parsing establishes JSON values, the array guard establishes
  // the batch container, and T is the caller-declared contract for those values.
  // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- `T` is a caller-declared payload contract; the runtime check above proves only that the batch is a JSON array.
  return parsed as T[];
}

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
    return { kind: "json", items: decodeJsonItems<T>(body), ...meta };
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
