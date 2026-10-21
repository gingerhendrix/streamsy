import { Config, Context, Effect, Layer, Schema } from "effect";
import { HttpServerRequest, HttpServerResponse } from "effect/http";
import { bearerMatches } from "./auth.ts";
import { collectBytes } from "./bytes.ts";

export const json = HttpServerResponse.jsonUnsafe;

export const JSON_BODY_LIMIT = 512 * 1024;

export class JsonBodyTooLarge extends Schema.TaggedError<
  JsonBodyTooLarge,
  { readonly brand: unique symbol }
>()("JsonBodyTooLarge", {}) {}

export class UnsupportedMediaType extends Schema.TaggedError<
  UnsupportedMediaType,
  { readonly brand: unique symbol }
>()("UnsupportedMediaType", {}) {}

export class Unauthorized extends Schema.TaggedError<
  Unauthorized,
  { readonly brand: unique symbol }
>()("Unauthorized", {}) {}

/** The writer token. With no token, the host accepts no writes. */
export class WriterToken extends Context.Service<WriterToken, { readonly token: string }>()(
  "FolderSync/WriterToken",
) {}

/** Read the token from config once, when the layer is built. */
export const WriterTokenLive = Layer.effect(
  WriterToken,
  Effect.map(
    Config.String("FOLDER_SYNC_WRITER_TOKEN").pipe(Config.withDefault(""), Effect.orDie),
    (token) => WriterToken.of({ token }),
  ),
);

/** Fail with `Unauthorized` unless the request carries the writer token. */
export const authorize = Effect.fn("Http.authorize")(function* () {
  const request = yield* HttpServerRequest.HttpServerRequest;
  const { token } = yield* WriterToken;

  if (!bearerMatches(request.headers.authorization, token)) return yield* new Unauthorized();

  return yield* Effect.void;
});

/**
 * The JSON body of a write. Every JSON write needs `Content-Type: application/json`.
 * The body may be at most 512 KiB, with or without `Content-Length`.
 */
export const jsonBody = Effect.fn("Http.jsonBody")(function* <S extends Schema.Top>(schema: S) {
  const request = yield* HttpServerRequest.HttpServerRequest;
  const type = request.headers["content-type"]?.split(";")[0]?.trim().toLowerCase();

  if (type !== "application/json") return yield* new UnsupportedMediaType();
  const length = request.headers["content-length"];

  if (length !== undefined && Number(length) > JSON_BODY_LIMIT)
    return yield* new JsonBodyTooLarge();

  const bytes = yield* collectBytes(request.stream, JSON_BODY_LIMIT, () => new JsonBodyTooLarge());

  return yield* Schema.decodeEffect(Schema.fromJsonString(schema))(new TextDecoder().decode(bytes));
});

/** Answers for the failures that every JSON write route can meet. */
export const requestFailures = {
  Unauthorized: () => Effect.succeed(json({ error: "Unauthorized" }, { status: 401 })),
  UnsupportedMediaType: () =>
    Effect.succeed(json({ error: "Expected application/json" }, { status: 415 })),
  JsonBodyTooLarge: () =>
    Effect.succeed(json({ error: "JSON write bodies must be at most 512 KiB" }, { status: 413 })),
  SchemaError: () => Effect.succeed(json({ error: "Invalid request" }, { status: 400 })),
  HttpServerError: () => Effect.succeed(json({ error: "Invalid body" }, { status: 400 })),
} as const;

/** Log any other failure and answer 500. */
// oxlint-disable-next-line typescript/no-unnecessary-type-parameters -- catchTags passes each route's remaining error type here.
export const storageFailure = <E>(cause: E) =>
  Effect.logError(cause).pipe(Effect.as(json({ error: "Storage failure" }, { status: 500 })));
