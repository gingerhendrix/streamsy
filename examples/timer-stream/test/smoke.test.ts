/* oxlint-disable effecttsgo/async-function -- Vitest owns the real-time process-host check. */
import { Effect } from "effect";
import { expect, test } from "vitest";
import { smoke } from "../src/app.ts";

test("one binding fires a one-shot, cancels a row and advances cron", async () => {
  expect(await Effect.runPromise(smoke)).toEqual({
    oneShot: 1,
    cancelled: 0,
    cron: 1,
    sleepingBindings: 1,
    timerRows: 2,
  });
}, 10_000);
