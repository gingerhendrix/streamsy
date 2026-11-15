/* oxlint-disable effecttsgo/strict-effect-provide -- This executable example composes its complete local host. */
import { Array, Effect, Layer, Schema } from "effect";
import { SqlClient } from "effect/sql";
import { Streamsy } from "@streamsy/core";
import { Protocol } from "@streamsy/core/protocol";
import { Commit, Projection } from "@streamsy/projection";
import { Accepted, CommandChanges, Commands, type Request } from "./streams.ts";
import { Storage } from "./host.ts";

/** The request ledger is small here; a production ledger needs a retention policy. */
const requested = Projection.fold({
  id: "requested-keys",
  params: {},
  inputs: { commands: Commands },
  state: { schema: Schema.Array(Schema.String), initial: [] },
  fold: (keys, { item }) => Array.append(keys, item.key),
});

export const commandChanges = Projection.wake({
  id: "command-changes",
  params: {},
  input: Commands,
  output: CommandChanges,
});

export const perform = Projection.process({
  id: "perform-commands",
  params: {},
  inputs: { commands: Commands },
  accepted: Accepted,
  commit: Commit.transaction,
  process: (batch) =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      for (const request of batch.commands.items) {
        yield* sql`INSERT INTO results (request_key, result) VALUES (${request.key}, ${request.text.toUpperCase()})
        ON CONFLICT (request_key) DO NOTHING`;
      }
    }),
});

/** A repeated caller key appends nothing and waits for the already accepted input. */
export const submit = (request: Request) =>
  Effect.gen(function* () {
    const receipt = yield* Projection.decide(requested.at({}), {
      input: "commands",
      decide: ({ state }) =>
        Effect.succeed({
          value: request.key,
          items: state.includes(request.key) ? [] : [request],
        }),
    });

    yield* Streamsy.awaitWake(
      Accepted,
      {},
      {
        stream: Commands.at({}).id,
        offset: receipt.offset,
      },
    ).pipe(Effect.timeout("5 seconds"));

    return receipt;
  });

const local = Protocol.bind(Storage, Streamsy.group(Commands, CommandChanges, Accepted)).pipe(
  Layer.provideMerge(Storage),
);

const host = Layer.mergeAll(
  Projection.attach(commandChanges, { durable: true }),
  Projection.attach(perform, { durable: true, triggers: [CommandChanges] }),
).pipe(Layer.provideMerge(Projection.Registry.layer), Layer.provideMerge(local));

const flow = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE results (request_key TEXT PRIMARY KEY, result TEXT NOT NULL)`;
  yield* (yield* Commands).create({});
  yield* (yield* CommandChanges).create({});
  yield* (yield* Accepted).create({});
  const first = yield* submit({ key: "caller-1/request-1", text: "hello" });
  const repeated = yield* submit({ key: "caller-1/request-1", text: "hello" });
  const results = yield* sql<{ request_key: string; result: string }>`SELECT * FROM results`;
  const requests = (yield* (yield* Commands).collect({})).items;
  const receipts = (yield* (yield* Accepted).collect({})).items;

  if (
    !first.appended ||
    repeated.appended ||
    first.offset !== repeated.offset ||
    requests.length !== 1 ||
    receipts.length !== 1 ||
    results.length !== 1 ||
    results[0]?.result !== "HELLO"
  ) {
    return yield* Effect.die(new Error("Command idempotency or receipt mismatch"));
  }

  return { requests: requests.length, effects: results.length, receipts: receipts.length };
});

export const smoke = flow.pipe(Effect.provide(host), Effect.scoped, Effect.timeout("8 seconds"));
