/* oxlint-disable effecttsgo/async-function -- Vitest owns the real-time process-host check. */
import { Effect } from "effect";
import { expect, test } from "vitest";
import { smoke } from "../src/app.ts";

test("a repeated caller key makes one request, effect and receipt", async () => {
  expect(await Effect.runPromise(smoke)).toEqual({ requests: 1, effects: 1, receipts: 1 });
}, 10_000);
