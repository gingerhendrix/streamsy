import { Offset, type StoredMessage } from "@streamsy/core";

const OFFSET_BYTES = 33;
const TIMESTAMP_BYTES = 8;
const LENGTH_BYTES = 4;
const FRAME_OVERHEAD = OFFSET_BYTES + TIMESTAMP_BYTES + LENGTH_BYTES;
const OFFSET_PATTERN = /^\d{16}_\d{16}$/;

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/** Encode the version-one frames representation used between Durable Objects. */
export function encodeFrames(messages: ReadonlyArray<StoredMessage>): Uint8Array {
  const offsets = messages.map((message) => encoder.encode(message.offset));
  const total = messages.reduce(
    (sum, message) => sum + FRAME_OVERHEAD + message.data.byteLength,
    0,
  );
  const output = new Uint8Array(total);
  const view = new DataView(output.buffer);
  let position = 0;
  for (const [index, message] of messages.entries()) {
    const offset = offsets[index];
    if (offset === undefined || offset.byteLength !== OFFSET_BYTES)
      throw new Error("Invalid frame offset");
    output.set(offset, position);
    position += OFFSET_BYTES;
    view.setFloat64(position, message.timestamp, false);
    position += TIMESTAMP_BYTES;
    view.setUint32(position, message.data.byteLength, false);
    position += LENGTH_BYTES;
    output.set(message.data, position);
    position += message.data.byteLength;
  }
  return output;
}

/** Decode version-one frames, rejecting every malformed boundary. */
export function decodeFrames(bytes: Uint8Array): ReadonlyArray<StoredMessage> {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const messages: Array<StoredMessage> = [];
  let position = 0;
  while (position < bytes.byteLength) {
    if (bytes.byteLength - position < FRAME_OVERHEAD) throw new Error("Truncated frame");
    const offsetText = decoder.decode(bytes.subarray(position, position + OFFSET_BYTES));
    if (!OFFSET_PATTERN.test(offsetText)) throw new Error("Invalid frame offset");
    position += OFFSET_BYTES;
    const timestamp = view.getFloat64(position, false);
    if (!Number.isFinite(timestamp)) throw new Error("Invalid frame timestamp");
    position += TIMESTAMP_BYTES;
    const length = view.getUint32(position, false);
    position += LENGTH_BYTES;
    if (length > bytes.byteLength - position) throw new Error("Truncated frame data");
    messages.push({
      offset: Offset.make(offsetText),
      timestamp,
      data: bytes.slice(position, position + length),
    });
    position += length;
  }
  return messages;
}
