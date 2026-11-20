import { BunRuntime } from "@effect/platform-bun";
import { Console, Effect } from "effect";
import { smoke } from "../src/app.ts";

BunRuntime.runMain(smoke.pipe(Effect.tap(() => Console.log("Views shop smoke passed"))));
