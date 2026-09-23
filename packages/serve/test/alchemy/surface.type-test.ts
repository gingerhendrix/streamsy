import type { HttpEffect } from "alchemy/Http";
import type { Effect } from "effect";
import type { HttpServerError } from "effect/unstable/http/HttpServerError";
import type * as Host from "@streamsy/serve/alchemy";

type Equal<A, B> = [A] extends [B] ? ([B] extends [A] ? true : false) : false;
type Assert<T extends true> = T;
export type PublicNames = Assert<
  Equal<
    keyof typeof Host,
    "objectHandlers" | "alarm" | "alarmLayer" | "router" | "Placement" | "Alarm" | "rule"
  >
>;
export type RouterFits = Assert<ReturnType<typeof Host.router> extends HttpEffect ? true : false>;
export type RouterError = Assert<
  Equal<Effect.Error<ReturnType<typeof Host.router>>, HttpServerError>
>;
export type AlarmError = Assert<
  Equal<Effect.Error<typeof Host.alarm>, import("@streamsy/core").StorageFault>
>;
