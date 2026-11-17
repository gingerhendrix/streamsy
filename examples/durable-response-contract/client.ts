import { Effect } from "effect";
import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/http";
import {
  ChatCreated,
  ModelCalls,
  responsePath,
  StopReply,
  type ChatRequest,
  type StopRequest,
} from "./api.ts";

export const startChat = Effect.fn("Api.startChat")(function* (input: ChatRequest) {
  const response = yield* HttpClient.execute(
    HttpClientRequest.post(new URL("/chat", window.location.origin).href).pipe(
      HttpClientRequest.bodyJsonUnsafe(input),
    ),
  );

  yield* HttpClientResponse.filterStatusOk(response);

  return yield* HttpClientResponse.schemaBodyJson(ChatCreated)(response);
});

export const stopResponse = Effect.fn("Api.stopResponse")(function* (
  id: string,
  input: StopRequest,
) {
  const response = yield* HttpClient.execute(
    HttpClientRequest.post(new URL(`${responsePath(id)}/stop`, window.location.origin).href).pipe(
      HttpClientRequest.bodyJsonUnsafe(input),
    ),
  );

  yield* HttpClientResponse.filterStatusOk(response);

  return yield* HttpClientResponse.schemaBodyJson(StopReply)(response);
});

export const modelCalls = Effect.suspend(() =>
  HttpClient.get(new URL("/debug/model-calls", window.location.origin).href),
).pipe(
  Effect.flatMap(HttpClientResponse.filterStatusOk),
  Effect.flatMap(HttpClientResponse.schemaBodyJson(ModelCalls)),
);
