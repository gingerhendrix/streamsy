import { BunRuntime } from "@effect/platform-bun";
import { Projection } from "@streamsy/projection";
import { Config, Effect, Layer, Schema } from "effect";
import { applicationLayer, sqliteStorage } from "../server/host.ts";
import { tracker } from "../server/outputs.ts";
import { issueRows } from "../server/projection.ts";

class Usage extends Schema.TaggedError<Usage>()("Usage", { message: Schema.String }) {}

const program = Effect.gen(function* () {
  const workspaceId = yield* Effect.fromNullishOr(Bun.argv[2]).pipe(
    Effect.mapError(
      () => new Usage({ message: "usage: ISSUE_TRACKER_DB=<path> run-once.ts <workspaceId>" }),
    ),
  );
  yield* Effect.log(yield* Projection.run(issueRows.at({ workspaceId })));
  yield* Effect.log(yield* Projection.run(tracker.at({ workspaceId })));
});

const host = Layer.unwrap(
  Effect.map(Config.String("ISSUE_TRACKER_DB"), (filename) =>
    applicationLayer(sqliteStorage(filename)),
  ),
);

// oxlint-disable-next-line effecttsgo/strict-effect-provide -- This script is the entry point.
BunRuntime.runMain(program.pipe(Effect.provide(host)));
