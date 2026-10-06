import { memoryHost } from "../support/memory-host.ts";
import { BunStorage } from "@streamsy/storage/bun";
import { Protocol } from "@streamsy/core/protocol";
import { Context, Effect, Layer, Option, Schema } from "effect";
import { SqlClient } from "effect/sql";
import { Streamsy } from "@streamsy/core";
import {
  CheckpointStore,
  type CheckpointRecord,
  Commit,
  Projection,
  Transaction,
  type Folded,
  type Emitted,
  type Accumulated,
} from "@streamsy/projection";

class Extra extends Context.Service<Extra, {}>()("commit-types/Extra") {}

class Audit extends Context.Service<Audit, {}>()("commit-types/Audit") {}

class Input extends Streamsy.json<Input>()("commit-types/Input", "commit-types/input", {
  params: {},
  schema: Schema.Finite,
}) {}

type Same<A, B> = [A] extends [B] ? ([B] extends [A] ? true : false) : false;

type Assert<T extends true> = T;

const process = Projection.process({
  commit: Commit.transaction,
  id: "types",
  params: {},
  inputs: { input: Input },
  process: Projection.each(() =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* Extra;
      yield* sql.unsafe("SELECT 1");
    }),
  ),
}).at({});

const run = Projection.run(process);

const compensation = Commit.transaction.pipe(
  Commit.withCompensation((_result, _cause, unit) => {
    const key: string = unit.key;
    void key;

    return Effect.asVoid(Audit);
  }),
);

const compensated = Projection.run({ ...process, commit: compensation });

const folded = Projection.fold({
  id: "fold",
  params: {},
  inputs: { input: Input },
  state: { schema: Schema.Finite, initial: 0 },
  fold: (state, { item }) => state + item,
}).at({});

const foldRun = Projection.run(folded);

const foldRead = Projection.read(folded);

const foldForget = Projection.forget(folded);

export type Checks = [
  Assert<Same<Effect.Services<typeof run>, Input | Extra | CheckpointStore | Transaction>>,
  Assert<
    Same<Effect.Services<typeof compensated>, Input | Extra | Audit | CheckpointStore | Transaction>
  >,
  Assert<Same<Effect.Services<typeof foldRun>, Input | CheckpointStore>>,
  Assert<Same<Effect.Services<typeof foldRead>, CheckpointStore>>,
  Assert<Same<Effect.Services<typeof foldForget>, CheckpointStore>>,
];

// oxlint-disable effecttsgo/missing-effect-context -- These negative assignments prove the host and result requirements remain visible.
// @ts-expect-error a host without Transaction cannot execute transactional work
const noTransaction: Effect.Effect<
  unknown,
  Effect.Error<typeof run>,
  Input | Extra | CheckpointStore
> = run;

// @ts-expect-error SQL subtraction must preserve Extra
const noExtra: Effect.Effect<
  unknown,
  Effect.Error<typeof run>,
  Input | CheckpointStore | Transaction
> = run;

// @ts-expect-error checkpoint cannot land a process result
const wrongProcess: Commit.Commit<void, never, CheckpointStore> = Commit.checkpoint;

// @ts-expect-error checkpoint cannot land emitted outputs
const wrongEmit: Commit.Commit<Emitted<{}>, never, CheckpointStore> = Commit.checkpoint;

// @ts-expect-error producer cannot land a folded value without outputs
const wrongProducer: Commit.Commit<Folded<number>, never, CheckpointStore> = Commit.producer;

const fits: Commit.Commit<Accumulated<number, {}>, never, CheckpointStore> = Commit.producer;

void noTransaction;

void noExtra;

void wrongProcess;

void wrongEmit;

void wrongProducer;

void fits;
// oxlint-enable effecttsgo/missing-effect-context

const directCompensation = Commit.withCompensation(Commit.transaction, () => Effect.asVoid(Audit));

const voidCompensation = Commit.atLeastOnce.pipe(
  Commit.withCompensation((result, _cause, unit) => {
    const value: void = result;
    const key: string = unit.key;
    void key;

    return Effect.as(Audit, value);
  }),
);

export type CompensationChecks = [
  Assert<Same<typeof compensation, typeof directCompensation>>,
  Assert<Same<typeof voidCompensation, Commit.Commit<void, never, CheckpointStore | Audit>>>,
];

const badCompensation = Commit.atLeastOnce.pipe(
  // @ts-expect-error compensation cannot demand a number from a void Commit
  Commit.withCompensation((_value: number) => Effect.void),
);

// @ts-expect-error compensation's Audit service remains a host requirement
const droppedAudit: Commit.Commit<void, never, CheckpointStore> = voidCompensation;

void badCompensation;

void droppedAudit;

class Bucket extends Context.Service<Bucket, {}>()("commit-types/Bucket") {}

const custom = Commit.make({
  name: "bucket",
  guarantee: "exactly-once",
  load: () => Effect.as(Bucket, { record: Option.none<CheckpointRecord>(), token: "0" }),
  apply: (pending: Commit.Pending<Folded<unknown>>) =>
    Effect.flatMap(pending.run, () => Effect.asVoid(Bucket)),
});

const customRun = Projection.run({ ...folded, commit: custom });

const customRead = Projection.read({ ...folded, commit: custom });

export type CustomChecks = [
  Assert<Same<typeof custom, Commit.Commit<Folded<unknown>, never, Bucket>>>,
  Assert<Same<Effect.Services<typeof customRun>, Input | Bucket>>,
  Assert<Same<Effect.Services<typeof customRead>, Bucket>>,
];

// oxlint-disable effecttsgo/missing-effect-context -- This negative assignment proves that a custom Commit's inferred service is required.
// @ts-expect-error a custom Commit's inferred Bucket requirement cannot be dropped
const missingBucket: Effect.Effect<unknown, Effect.Error<typeof customRun>, Input> = customRun;

void missingBucket;
// oxlint-enable effecttsgo/missing-effect-context

const hostProcess = Projection.process({
  id: "sql-host-required",
  params: {},
  inputs: { input: Input },
  commit: Commit.transaction,
  process: () => Effect.asVoid(SqlClient.SqlClient),
}).at({});

const memoryGraph = memoryHost();

const memoryRun = Effect.gen(function* () {
  return yield* Projection.run(hostProcess).pipe(
    Effect.provideContext(
      yield* Layer.build(
        Protocol.bind(memoryGraph, Streamsy.group(Input)).pipe(Layer.provideMerge(memoryGraph)),
      ),
    ),
  );
}).pipe(Effect.scoped);

const sqlHost = Layer.merge(
  Protocol.bind(memoryGraph, Streamsy.group(Input)),
  Layer.mergeAll(CheckpointStore.layer, Transaction.layer).pipe(
    Layer.provide(BunStorage.layer(Streamsy.group(), { client: { filename: ":memory:" } })),
  ),
);

const sqlRun = Effect.gen(function* () {
  return yield* Projection.run(hostProcess).pipe(
    Effect.provideContext(yield* Layer.build(sqlHost)),
  );
}).pipe(Effect.scoped);

// oxlint-disable effecttsgo/missing-effect-context -- Proves that memory cannot satisfy Transaction.
// @ts-expect-error A memory host leaves the SQL Transaction requirement unsatisfied
const runnableOnMemory: Effect.Effect<unknown, Effect.Error<typeof memoryRun>> = memoryRun;

// oxlint-enable effecttsgo/missing-effect-context
const runnableOnSql: Effect.Effect<unknown, Effect.Error<typeof sqlRun>> = sqlRun;

void runnableOnMemory;

void runnableOnSql;

export type HostChecks = [
  Assert<Same<Effect.Services<typeof memoryRun>, Transaction>>,
  Assert<Same<Effect.Services<typeof sqlRun>, never>>,
];
