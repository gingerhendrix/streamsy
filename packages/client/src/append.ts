import type { AppendStreamOptions, ClientAppendResult, ClientFailure } from "@streamsy/core";
import type { OfficialProtocolClient } from "./client.ts";
import { failure } from "./errors.ts";
import { copyToArrayBuffer } from "./bytes.ts";

const STREAM_OFFSET_HEADER = "stream-next-offset";
const PRODUCER_EPOCH_HEADER = "producer-epoch";
const PRODUCER_SEQ_HEADER = "producer-seq";

export async function officialAppend(
  client: OfficialProtocolClient,
  url: string | URL,
  data: Uint8Array | string,
  options: AppendStreamOptions,
  fallbackContentType?: string,
  jsonBatch = false,
): Promise<ClientAppendResult> {
  const headers = await client.appendHeaders();
  const contentType = options.contentType ?? fallbackContentType;
  if (contentType) headers.set("content-type", contentType);
  if (options.seq !== undefined) headers.set("stream-seq", options.seq);
  if (options.expectedOffset !== undefined) {
    headers.set("stream-expected-offset", options.expectedOffset);
  }
  if (options.producer) {
    headers.set("producer-id", options.producer.producerId);
    headers.set(PRODUCER_EPOCH_HEADER, String(options.producer.producerEpoch));
    headers.set(PRODUCER_SEQ_HEADER, String(options.producer.producerSeq));
  }

  const response = await client.fetchAppend(url, {
    method: "POST",
    headers,
    body: encodeBody(data, contentType, jsonBatch),
    signal: options.signal,
  });
  const offset = response.headers.get(STREAM_OFFSET_HEADER);
  if (offset === null) return parseFailure("Missing Stream-Next-Offset on append success");

  if (options.producer) {
    const producer = parseProducerState(response.headers);
    if ("status" in producer) return producer;
    if (response.status === 204) {
      return { status: "duplicate", offset, ...producer };
    }
    return { status: "appended", offset, ...producer };
  }
  return { status: "appended", offset };
}

function encodeBody(data: Uint8Array | string, contentType?: string, jsonBatch = false): BodyInit {
  if (normalizedContentType(contentType) === "application/json") {
    const json = typeof data === "string" ? data : new TextDecoder().decode(data);
    return jsonBatch ? json : `[${json}]`;
  }
  if (typeof data === "string") return data;
  return copyToArrayBuffer(data);
}

function normalizedContentType(contentType?: string): string | undefined {
  return contentType?.split(";", 1)[0]?.trim().toLowerCase();
}

function parseProducerState(
  headers: Headers,
): { producerEpoch: number; producerSeq: number } | ClientFailure {
  const producerEpoch = safeInteger(headers.get(PRODUCER_EPOCH_HEADER));
  const producerSeq = safeInteger(headers.get(PRODUCER_SEQ_HEADER));
  if (producerEpoch === undefined || producerSeq === undefined) {
    return parseFailure("Missing or invalid producer state on append success");
  }
  return { producerEpoch, producerSeq };
}

function safeInteger(value: string | null): number | undefined {
  if (value === null || !/^(0|[1-9]\d*)$/.test(value)) return undefined;
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) ? parsed : undefined;
}

function parseFailure(message: string): ClientFailure {
  return failure("parse-error", message);
}
