import { BunRuntime } from "@effect/platform-bun";
import { Config, Effect } from "effect";
import { ArgumentsError, parseArguments, usage } from "./arguments.ts";
import { listRejected, parseRejectedArguments } from "./rejected.ts";
import { sync } from "./engine.ts";

// argv is read only at the executable boundary.
const args = Bun.argv.slice(2);

if (args.includes("--help")) {
  BunRuntime.runMain(Effect.logInfo(usage));
} else if (args[0] === "rejected") {
  BunRuntime.runMain(
    Effect.try({
      try: () => parseRejectedArguments(args.slice(1)),
      catch: (cause) => new ArgumentsError({ message: String(cause) }),
    }).pipe(Effect.flatMap(listRejected)),
  );
} else {
  BunRuntime.runMain(
    Effect.try({
      try: () => parseArguments(args),
      catch: (cause) => new ArgumentsError({ message: String(cause) }),
    }).pipe(
      Effect.flatMap((options) =>
        Effect.gen(function* () {
          if (options.token !== undefined) return yield* sync(options);

          const token = yield* Config.String("FOLDER_SYNC_WRITER_TOKEN").pipe(
            Config.withDefault(""),
          );

          return yield* sync(token === "" ? options : { ...options, token });
        }),
      ),
    ),
  );
}
