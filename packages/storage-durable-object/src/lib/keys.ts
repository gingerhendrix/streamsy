export const STREAM_ID_KEY = "stream-id";
export const RECORD_KEY = "record";
export const MESSAGE_PREFIX = "message:";
export const PRODUCER_PREFIX = "producer:";

export function messageKey(offset: string): string {
  return `${MESSAGE_PREFIX}${offset}`;
}

export function producerKey(producerId: string): string {
  return `${PRODUCER_PREFIX}${producerId}`;
}
