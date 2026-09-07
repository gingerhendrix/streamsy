import { Effect, Option } from "effect";
import { HttpServerRequest, HttpServerResponse } from "effect/unstable/http";
import { Protocol } from "@streamsy/core";
import type { HttpOptions } from "@streamsy/core/http";
import { program } from "@streamsy/core/http";
import { reconcileAlarm } from "./alarm.ts";
import { HostCommand } from "./host-command.ts";

const mutates = (method: string): boolean =>
  method === "PUT" || method === "POST" || method === "DELETE";

export const hostProgram = (options: HttpOptions) =>
  Effect.gen(function* () {
    const command = yield* Effect.serviceOption(HostCommand);
    if (Option.isSome(command)) {
      yield* Protocol.expireDue();
      yield* reconcileAlarm();
      return HttpServerResponse.empty({ status: 204 });
    }

    const request = yield* HttpServerRequest.HttpServerRequest;
    const response = yield* program(options);
    if (mutates(request.method)) yield* reconcileAlarm().pipe(Effect.uninterruptible);
    return response;
  }).pipe(
    Effect.catchTag("StorageFault", () =>
      Effect.succeed(HttpServerResponse.text("Internal server error", { status: 500 })),
    ),
  );
