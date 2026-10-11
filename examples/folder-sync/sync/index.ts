import { BunRuntime } from "@effect/platform-bun";
import { Effect } from "effect";
import { ArgumentsError, parseArguments, usage } from "./arguments.ts";
import { sync } from "./engine.ts";

// argv is read only at the executable boundary.
const args = Bun.argv.slice(2);

if (args.includes("--help")) {
  BunRuntime.runMain(Effect.logInfo(usage));
} else {
  BunRuntime.runMain(
    Effect.try({
      try: () => parseArguments(args),
      catch: (cause) => new ArgumentsError({ message: String(cause) }),
    }).pipe(Effect.flatMap(sync)),
  );
}
