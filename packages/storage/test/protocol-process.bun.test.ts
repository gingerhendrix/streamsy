/* oxlint-disable effecttsgo/async-function, effecttsgo/crypto-random-uuid, effecttsgo/node-builtin-import -- Bun owns true process fixtures. */
import { expect, test } from "bun:test";
import { Clock, Config, Effect, Schema } from "effect";
import { ZERO_OFFSET } from "@streamsy/core";

const scratch = Effect.runSync(
  Config.String("STREAMSY_STORAGE_SCRATCH").pipe(Config.withDefault("/tmp")),
);
const worker = new URL("./protocol-process-worker.ts", import.meta.url).pathname;

const WorkerResult = Schema.Struct({
  upToDate: Schema.optional(Schema.Boolean),
  nextOffset: Schema.optional(Schema.String),
  producerEpoch: Schema.optional(Schema.Finite),
  producerSeq: Schema.optional(Schema.Finite),
  expectedSeq: Schema.optional(Schema.Finite),
  receivedSeq: Schema.optional(Schema.Finite),
  currentEpoch: Schema.optional(Schema.Finite),
  ttlSeconds: Schema.optional(Schema.Finite),
  messages: Schema.optional(Schema.Array(Schema.Struct({ text: Schema.String }))),
  result: Schema.optional(
    Schema.Struct({
      _tag: Schema.String,
      index: Schema.optional(Schema.Finite),
      reason: Schema.optional(Schema.String),
    }),
  ),
  freshAbsent: Schema.optional(Schema.Boolean),
  _tag: Schema.optional(Schema.String),
  value: Schema.optional(Schema.Struct({ epoch: Schema.Finite, lastSeq: Schema.Finite })),
});

const run = async (args: ReadonlyArray<string>) => {
  const childProcess = Bun.spawn([process.execPath, worker, ...args], {
    stdout: "pipe",
    stderr: "pipe",
    env: { ...Bun.env, STREAMSY_STORAGE_SCRATCH: scratch },
  });
  const [stdout, stderr, exitCode] = await Promise.all([
    new Response(childProcess.stdout).text(),
    new Response(childProcess.stderr).text(),
    childProcess.exited,
  ]);
  if (exitCode !== 0) throw new Error(stderr || stdout);
  return Schema.decodeSync(Schema.fromJsonString(WorkerResult))(stdout);
};

const appendArgs = (options: {
  readonly filename: string;
  readonly id: string;
  readonly data: string;
  readonly expectedOffset?: string;
  readonly producerId?: string;
  readonly epoch?: number;
  readonly sequence?: number;
  readonly waitUntil?: number;
}) => [
  "append",
  options.filename,
  options.id,
  options.data,
  options.expectedOffset ?? "-",
  options.producerId ?? "-",
  String(options.epoch ?? 0),
  String(options.sequence ?? 0),
  "-",
  String(options.waitUntil ?? 0),
];

test("separate protocol processes preserve CAS, producer fencing, restart, lineage and TTL", async () => {
  const filename = `${scratch}/protocol-process-${process.pid}-${crypto.randomUUID()}.sqlite`;
  const concurrentCreates = await Promise.all([
    run(["create", filename, "create-race", "-", "-", "-"]),
    run(["create", filename, "create-race", "-", "-", "-"]),
  ]);
  expect(
    concurrentCreates
      .map(({ _tag }) => _tag)
      .toSorted((left, right) => String(left).localeCompare(String(right))),
  ).toEqual(["Created", "Exists"]);
  expect(await run(["multi-fail", filename, "must-not-exist", "create-race"])).toMatchObject({
    result: { _tag: "MutationRejected", index: 1, reason: "exists" },
    freshAbsent: true,
  });
  const created = await run(["create", filename, "race", "-", "-", "-"]);
  expect(created._tag).toBe("Created");
  const waitUntil = Effect.runSync(Clock.currentTimeMillis) + 250;
  const race = await Promise.all([
    run(
      appendArgs({
        filename,
        id: "race",
        data: "left",
        expectedOffset: ZERO_OFFSET,
        waitUntil,
      }),
    ),
    run(
      appendArgs({
        filename,
        id: "race",
        data: "right",
        expectedOffset: ZERO_OFFSET,
        waitUntil,
      }),
    ),
  ]);
  expect(
    race
      .map(({ _tag }) => _tag)
      .toSorted((left, right) => String(left).localeCompare(String(right))),
  ).toEqual(["Appended", "OffsetMismatch"]);
  const raceRead = await run(["read", filename, "race"]);
  expect(raceRead.messages).toHaveLength(1);

  expect(await run(["create", filename, "producer", "-", "-", "-"])).toMatchObject({
    _tag: "Created",
  });
  const tuple = { filename, id: "producer", data: "exact", producerId: "p", epoch: 0 };
  expect(await run(appendArgs({ ...tuple, sequence: 0 }))).toMatchObject({
    _tag: "Appended",
    producerEpoch: 0,
    producerSeq: 0,
  });
  expect(await run(appendArgs({ ...tuple, sequence: 0 }))).toMatchObject({
    _tag: "Duplicate",
    producerEpoch: 0,
    producerSeq: 0,
  });
  expect(await run(appendArgs({ ...tuple, sequence: 2 }))).toEqual({
    _tag: "ProducerGap",
    expectedSeq: 1,
    receivedSeq: 2,
  });
  expect(await run(appendArgs({ ...tuple, data: "next", sequence: 1 }))).toMatchObject({
    _tag: "Appended",
    producerSeq: 1,
  });
  expect(
    await run(appendArgs({ ...tuple, data: "takeover", epoch: 1, sequence: 0 })),
  ).toMatchObject({ _tag: "Appended", producerEpoch: 1, producerSeq: 0 });
  expect(await run(appendArgs({ ...tuple, sequence: 2 }))).toEqual({
    _tag: "StaleEpoch",
    currentEpoch: 1,
  });
  expect(await run(["producer", filename, "producer", "p"])).toMatchObject({
    _tag: "Some",
    value: { epoch: 1, lastSeq: 0 },
  });

  const source = await run(["create", filename, "source", "abc", "60", "-"]);
  expect(source._tag).toBe("Created");
  expect(await run(["fork", filename, "child", "source", ZERO_OFFSET, "2"])).toMatchObject({
    _tag: "Created",
  });
  expect(await run(appendArgs({ filename, id: "child", data: "d" }))).toMatchObject({
    _tag: "Appended",
  });
  const child = await run(["read", filename, "child"]);
  expect(child).toMatchObject({ upToDate: true });
  const childMessages = child.messages ?? [];
  expect(childMessages.map(({ text }) => text)).toEqual(["ab", "d"]);
  expect(await run(["head", filename, "child"])).toMatchObject({ ttlSeconds: 60 });

  expect(
    await run(["create", filename, "expired", "old", "-", "2000-01-01T00:00:00Z"]),
  ).toMatchObject({ _tag: "Created" });
  expect(await run(["head", filename, "expired"])).toMatchObject({ _tag: "StreamNotFound" });
});
