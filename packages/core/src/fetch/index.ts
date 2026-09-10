import { Context, Effect, Fiber, Layer, Scope } from "effect";
import { HttpClient, type HttpClientRequest } from "effect/unstable/http";
import { TransportFault } from "../fault.ts";
import { StreamsReader, StreamsWriter, type Reader, type Writer } from "../protocol/tags.ts";
import { requests, type Options } from "./request.ts";
import * as Response from "./response.ts";
import type { HttpResponse } from "./wire.ts";
export type { Options } from "./request.ts";
export { TransportFault } from "../fault.ts";

type Decoder<A> = (
  operation: Response.Operation,
  response: HttpResponse,
) => Effect.Effect<A, TransportFault>;

/** Supplies the existing services. HttpClient and the calling fiber own request lifetimes. */
export const layer = (options: Options) =>
  Layer.effectContext(
    Effect.gen(function* () {
      const client = HttpClient.withScope(yield* HttpClient.HttpClient);
      const owner = yield* Scope.Scope;
      const request = yield* Effect.try({
        try: () => requests(options),
        catch: (cause) =>
          new TransportFault({
            operation: "layer",
            reason: "configuration",
            message: "Invalid fetch configuration",
            cause,
          }),
      });
      const execute = <A>(
        operation: Response.Operation,
        make: () => HttpClientRequest.HttpClientRequest,
        decode: Decoder<A>,
      ) =>
        Effect.gen(function* () {
          const input = yield* Effect.try({
            try: make,
            catch: (cause) =>
              new TransportFault({
                operation,
                reason: "configuration",
                message: "Invalid request",
                cause,
              }),
          });
          const response = yield* client.execute(input).pipe(
            Effect.mapError(
              (cause) =>
                new TransportFault({
                  operation,
                  reason: "request",
                  message: "HTTP request failed",
                  cause,
                }),
            ),
          );
          return yield* decode(operation, response);
        }).pipe(
          Effect.scoped,
          Effect.forkIn(owner),
          Effect.flatMap((fiber) =>
            Fiber.join(fiber).pipe(Effect.ensuring(Fiber.interrupt(fiber))),
          ),
        );
      const reader: Reader<TransportFault> = {
        head: (id) => execute("head", () => request.head(id), Response.head),
        read: (id, input) => execute("read", () => request.read(id, input), Response.read),
        readNext: (id, input) =>
          execute("readNext", () => request.readNext(id, input), Response.readNext),
      };
      const writer: Writer<TransportFault> = {
        create: (id, input) => execute("create", () => request.create(id, input), Response.create),
        fork: (id, source, input) =>
          execute(
            "create",
            () => request.create(id, { ...input, forkedFrom: source }),
            Response.create,
          ),
        append: (id, input) => {
          if (input.expectedOffset !== undefined && options.capabilities?.expectedOffset !== true)
            return Effect.succeed({ status: "not-supported", feature: "expected-offset" });
          if (input.producer !== undefined && options.capabilities?.producer !== true)
            return Effect.succeed({ status: "not-supported", feature: "producer" });
          return execute(
            "append",
            () => request.append(id, input),
            (operation, response) =>
              Response.append(operation, response, { producer: input.producer !== undefined }),
          );
        },
        remove: (id) => execute("remove", () => request.remove(id), Response.remove),
      };
      return Context.make(StreamsReader, reader).pipe(Context.add(StreamsWriter, writer));
    }),
  );
