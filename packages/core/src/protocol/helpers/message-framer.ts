/**
 * Body framing for the durable streams protocol.
 *
 * Responsibilities:
 *
 * - Non-JSON content types pass the original bytes through as a single
 *   message.
 * - JSON content types are decoded and re-encoded:
 *   - JSON object input becomes one canonical `JSON.stringify(parsed)` message.
 *   - JSON array input produces one message per item.
 *   - Empty JSON array produces zero messages.
 * - The JSON branch is selected by `contentType.toLowerCase().startsWith("application/json")`,
 *   without parameter stripping.
 * - Invalid JSON propagates the underlying `JSON.parse` error.
 *
 * Used by `AppendService`, `CreateStreamService`, and `ForkService` before
 * incoming request bodies are persisted as stored messages.
 */

export function frameMessages(data: Uint8Array, contentType: string): Uint8Array[] {
  if (!contentType.toLowerCase().startsWith("application/json")) return [data];
  const parsed = JSON.parse(new TextDecoder().decode(data));
  if (Array.isArray(parsed))
    return parsed.length === 0
      ? []
      : parsed.map((item) => new TextEncoder().encode(JSON.stringify(item)));
  return [new TextEncoder().encode(JSON.stringify(parsed))];
}
