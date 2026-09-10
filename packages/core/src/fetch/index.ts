import { Context, Effect, Fiber, Layer, Scope } from "effect";
import { HttpClient, type HttpClientRequest } from "effect/unstable/http";
import { StreamId } from "../schema/index.ts";
import { NotSupported } from "../protocol/errors.ts";
import { TransportFault } from "../fault.ts";
import { StreamsReader, StreamsWriter, type Reader, type Writer } from "../protocol/tags.ts";
import { requests, type Options } from "./request.ts";
import * as Response from "./response.ts";
import type { HttpResponse } from "./wire.ts";
export type { Options } from "./request.ts";
export { TransportFault } from "../fault.ts";

type Decoder<A, E> = (operation: Response.Operation, response: HttpResponse) => Effect.Effect<A, E>;

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
      const execute = <A, E>(
        operation: Response.Operation,
        make: () => HttpClientRequest.HttpClientRequest,
        decode: Decoder<A, E>,
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
        head: (id) =>
          execute(
            "head",
            () => request.head(id),
            (operation, response) => Response.head(operation, response, { id }),
          ),
        read: (id, input) =>
          execute(
            "read",
            () => request.read(id, input),
            (operation, response) => Response.read(operation, response, { id }),
          ),
        readNext: (id, input) =>
          execute(
            "readNext",
            () => request.readNext(id, input),
            (operation, response) => Response.readNext(operation, response, { id }),
          ),
      };
      const writer: Writer<TransportFault> = {
        create: (id, input) =>
          execute(
            "create",
            () => request.create(id, input),
            (operation, response) =>
              Response.create(operation, response, {
                id,
                source:
                  input?.forkedFrom === undefined ? undefined : StreamId.make(input.forkedFrom),
              }),
          ),
        fork: (id, source, input) =>
          execute(
            "create",
            () => request.create(id, { ...input, forkedFrom: source }),
            (operation, response) => Response.create(operation, response, { id, source }),
          ),
        append: (id, input) => {
          if (input.expectedOffset !== undefined && options.capabilities?.expectedOffset !== true)
            return Effect.fail(new NotSupported({ id, feature: "expected-offset" }));
          if (input.producer !== undefined && options.capabilities?.producer !== true)
            return Effect.fail(new NotSupported({ id, feature: "producer" }));
          return execute(
            "append",
            () => request.append(id, input),
            (operation, response) =>
              Response.append(operation, response, {
                id,
                expectedOffset: input.expectedOffset,
                producer: input.producer !== undefined,
              }),
          );
        },
        remove: (id) =>
          execute(
            "remove",
            () => request.remove(id),
            (operation, response) => Response.remove(operation, response, { id }),
          ),
      };
      return Context.make(StreamsReader, reader).pipe(Context.add(StreamsWriter, writer));
    }),
  );
