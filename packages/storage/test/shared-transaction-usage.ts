/* oxlint-disable effecttsgo/node-builtin-import, eslint/no-underscore-dangle -- Executable documentation owns a temporary SQLite path and checks the public tagged outcome. */
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Context, Data, Effect, Exit, Layer, Predicate, Scope } from "effect";
import {
  Storage,
  StreamId,
  ZERO_OFFSET,
  type Mutation,
  type MutationOutcome,
} from "@streamsy/core";
import { CommitBoundary } from "@streamsy/storage";
import * as BunStorage from "@streamsy/storage/bun";
import * as SqlClient from "effect/unstable/sql/SqlClient";

class Rejected extends Data.TaggedError("Rejected")<{
  readonly outcome: Extract<MutationOutcome, { readonly _tag: "Rejected" }>;
}> {}

const id = StreamId.make("compiled-shared-transaction");
const create: Mutation = {
  operations: [
    {
      _tag: "Create",
      record: {
        id,
        config: { contentType: "text/plain", createdAt: 0 },
        lifecycle: { closed: false, softDeleted: false },
        currentOffset: ZERO_OFFSET,
      },
      initialMessages: [],
    },
  ],
};

const directory = await mkdtemp(join(tmpdir(), "streamsy-sql-example-"));
const scope = await Effect.runPromise(Scope.make());
try {
  const context = await Effect.runPromise(
    Layer.buildWithScope(
      BunStorage.layer({ client: { filename: join(directory, "streamsy.sqlite") } }),
      scope,
    ),
  );
  const sql = Context.get(context, SqlClient.SqlClient);
  const result = await Effect.runPromise(
    Effect.gen(function* () {
      const storage = yield* Storage;
      const boundary = yield* CommitBoundary;
      yield* sql.unsafe("CREATE TABLE application_rows(id TEXT PRIMARY KEY)");
      yield* storage.mutate(create);
      const outcome = yield* boundary
        .withTransaction(
          Effect.gen(function* () {
            yield* sql.unsafe("INSERT INTO application_rows VALUES ('rolled-back')");
            const mutation = yield* storage.mutate(create);
            if (Predicate.isTagged(mutation, "Rejected"))
              return yield* new Rejected({ outcome: mutation });
            return mutation;
          }),
        )
        .pipe(Effect.catchTag("Rejected", ({ outcome: rejection }) => Effect.succeed(rejection)));
      const rows = yield* sql.unsafe<{ readonly id: string }>("SELECT id FROM application_rows");
      return { outcome, rows };
    }).pipe(Effect.provide(context)),
  );
  if (result.outcome._tag !== "Rejected" || result.rows.length !== 0)
    throw new Error("shared transaction did not roll back rejection");
} finally {
  await Effect.runPromise(Scope.close(scope, Exit.void));
  await rm(directory, { recursive: true, force: true });
}
