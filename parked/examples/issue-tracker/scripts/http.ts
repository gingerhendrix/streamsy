import { Schema } from "effect";

/** The smoke scripts deliberately exercise the host through Bun's native HTTP stack. */
export function request(input: string | URL, init?: RequestInit): Promise<Response> {
  // oxlint-disable-next-line effecttsgo/global-fetch -- Executable acceptance scripts must cross the real Bun HTTP boundary rather than provide an in-memory Effect client.
  return globalThis.fetch(input, init);
}

/** Decode a successful or error response through the contract selected by its caller. */
export function decodeResponse<S extends Schema.ConstraintDecoder<unknown>>(
  schema: S,
  response: Response,
): Promise<S["Type"]> {
  return response.text().then((text) => Schema.decodePromise(Schema.fromJsonString(schema))(text));
}

/** Request and decode one JSON response while preserving the native response lifecycle. */
export function requestJson<S extends Schema.ConstraintDecoder<unknown>>(
  schema: S,
  input: string | URL,
  init?: RequestInit,
): Promise<S["Type"]> {
  return request(input, init).then((response) => decodeResponse(schema, response));
}
