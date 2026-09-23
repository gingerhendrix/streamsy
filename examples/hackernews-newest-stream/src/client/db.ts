import { createStreamDB } from "@durable-streams/state/db";
import { STATE_VERSION_HEADER } from "@streamsy/serve/contract";
import { hackerNewsState } from "../state-schema.ts";

// The installed binding writes the string event key into row.id, despite HnStory.id being typed number.
/** createStreamDB starts a fresh session at -1, then follows the live suffix. */
export function createHnDb(origin: string) {
  return createStreamDB({
    streamOptions: {
      url: new URL("/state/newest", origin).toString(),
      contentType: "application/json",
      headers: { [STATE_VERSION_HEADER]: "1" },
      warnOnHttp: false,
    },
    live: "long-poll",
    state: hackerNewsState,
  });
}
export type HnDb = ReturnType<typeof createHnDb>;
