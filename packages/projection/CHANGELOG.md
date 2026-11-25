## @streamsy/projection@0.15.0

### Require Effect 4.1.5 and Alchemy beta.87

**Breaking change:** Streamsy now requires `effect` and any `@effect/*` peers at `^4.1.5`. Upgrade the Effect family together.

The optional Alchemy integration requires exactly `alchemy@2.0.0-beta.87` and `@alchemy.run/cloudflare@2.0.0-beta.87`. Install both when using `@streamsy/serve/alchemy`.

On Node, `Node.listener` leaves the answer before the routes serve and after they stop to Effect's `NodeHttpServer`. It stays 503 with the body `service unavailable` and `Retry-After: 1`. It now has `Cache-Control: no-store` and keeps the connection open. With Effect 4.1.5 and Streamsy 0.14.0, such a request got no answer. Bun and Deno answer 503 instead of 404 in the same gap. See [the Node listener](https://streamsy.dev/docs/reference/serve/node).

`expiresAt` and `Stream-Expires-At` follow Effect 4.1.5 `DateTime.make`, which also accepts the zone names `UTC`, `UT`, `EST`, `EDT`, `CST`, `CDT`, `MST`, `MDT`, `PST` and `PDT`, such as `Sat, 01 Jan 2028 00:00:00 UTC`.

## @streamsy/projection@0.14.0

### Give nested SQL clients their own transactions

In 0.13.0, nesting `CommitBoundary.withTransaction` on a second SQL client ran the inner body without a transaction, so a failure could leave partial writes behind.

- **Independent inner transactions:** A nested `CommitBoundary.withTransaction` on a second SQL client now opens its own transaction, which commits first and survives an outer rollback. Non-transactional projections running on the second client now save their checkpoints inside a transaction. Durable Object clients still share the driver's transaction.
- **Stricter boundary and lifetime checks:** Open `CommitBoundary.withTransaction` before nesting raw `sql.withTransaction` on that client. Mutating a second client inside its raw `sql.withTransaction` while only the first client's boundary is open now dies instead of running. Fibers or finalizers that outlive a transaction body and reuse its old context now die if they attempt to enter the boundary or mutate storage.

See [Commit with your own tables](https://streamsy.dev/docs/storage/sqlite#commit-with-your-own-tables).

### List projection bindings and routed outputs on MySQL (schema v4)

`Projection.bindings`, startup binding discovery for attached projections, and `Projection.retire` / `Projection.forget` for bindings with routed outputs now work on MySQL. Durable pending marks on MySQL also now support encoded params longer than 512 code points, and both attach hosts reject MySQL projection IDs longer than 255 code points when durability is enabled.

**Breaking change (MySQL schema v4):** Opening a MySQL database adds `full_key` to `streamsy_owner_records` and `full_params` to `streamsy_projection_pending`, upgrading MySQL storage to schema version 4. Streamsy 0.13.0 cannot open a MySQL database at version 4. SQLite and Postgres remain at schema version 3.

**Upgrade steps for MySQL:**

- Stop every 0.13.0 process before opening the database with this release. A running 0.13.0 process can read the fitted form of a long pending mark, fail to decode it, and delete the mark without running the binding (leaving it idle until its next commit).
- Any MySQL owner key longer than 512 code points written by 0.13.0 becomes listable again after its next write; until then, listing a range containing that key fails.

See [MySQL owner record listing](https://streamsy.dev/docs/reference/storage/sql-storage#mysql-owner-record-listing).

### MySQL storage transactions default to `READ COMMITTED`

**Breaking change:** On MySQL, `CommitBoundary.withTransaction` now sets `READ COMMITTED` for each transaction regardless of the session, pool, or global default (including `SERIALIZABLE`), so every plain read takes a fresh snapshot. At this isolation level, locking reads cannot lock absent rows or guard ranges against inserts—use a unique key or lock an existing parent row instead.

This isolation level ensures consistent reads across concurrent hosts:

- Views circuits in a host transaction read operator state consistent with checkpoints when another host advances the binding (with or without earlier application reads, and when running two circuits in one transaction).
- `Projection.forget` sees a routed binding's first checkpoint committed concurrently by another host.
- `Projection.bindings` sees newly committed bindings from other hosts during the same transaction.

**Configuration and requirements:**

- **Opting out:** Set Effect's `SqlClient.BeginTransaction` to `"START TRANSACTION"` to use the session default. Running under `REPEATABLE READ` reintroduces stale Views results in the sequences above and causes `Projection.forget` to fail with a retryable `token-conflict` if its snapshot predates a new checkpoint.
- **Multi-statement requests:** Storage sends `SET TRANSACTION` and `START TRANSACTION` in one request (per-transaction only; keep the session in autocommit). Effect's `mysql2` client enables multi-statement support by default, whereas passing `-MULTI_STATEMENTS` or using a custom client without it fails at the first transaction.
- **Binary logging:** With binary logging enabled, InnoDB writes in `READ COMMITTED` transactions require `binlog_format=ROW` or `MIXED` (logged by row in mixed mode).

See the [MySQL isolation-level reference](https://dev.mysql.com/doc/refman/8.4/en/innodb-transaction-isolation-levels.html) and [MySQL transactions](https://streamsy.dev/docs/reference/storage/sql-storage#mysql-transactions).

### Use `generation` instead of `version` on projections and Views circuits

**Breaking change:** Projection and Views circuit declarations now use `generation` alone to version stored state and rebuilds, and reject any defined `version` with `RangeError`.

- **Declarations:** Remove `version: 1` to keep the existing generation. For any other version, remove `version` and raise `generation` above every generation the `id` has previously used.
- **Types and custom stores:** `ProjectionKey`, `Unit`, and the derived declaration and binding types no longer have a `version` field. If custom stores or handlers included `key.version` or `unit.version` in storage keys, substitute the literal `1` to preserve keys for the former default version. Passing a key object with a defined `version` other than `1` to `recordKey`, `Projection.forget`, or a `fromStore` checkpoint store throws `RangeError`.
- **Unchanged:** `View.fn(name, version, implementation)` keeps its function version argument.

When upgrading a declaration that used a `version` other than `1`, stop the old runners, retain full input history, and prepare output addresses before starting the upgraded process. Let the new generation catch up and move readers before retiring the old binding with a one-off 0.13.0 script that only calls `retire`. See [Migrate a declaration that set `version`](https://streamsy.dev/docs/projections/checkpoints#migrate-a-declaration-that-set-version).

### Unified transaction commits on SQL and memory hosts

`Commit.transaction` and `Commit.transactionWithCleanup` now use one unified transaction model across SQL and `MemoryStorage` hosts: compose `Transaction.layer` and `CheckpointStore.layer` over the same storage owner so each unit, its outputs, and its checkpoint (or lifecycle cleanup on `forget`/`retire`) commit together.

**Breaking change:**

- `Transaction` no longer exposes a `sql` field. Read `SqlClient` from Effect context where SQL is needed, and use `Layer.provideMerge` when composing the host so the storage owner's `SqlClient` remains in the runner's context (including for `StateTable` and `FoldByKey` runners).
- The `Commit.transactionWithCleanup` callback now takes only the checkpoint key.
- `StateTable` and `FoldByKey` fail with `unsupported-composition` if `SqlClient` has no open transaction. Wrap direct `table.sync()` calls in `Transaction.withTransaction`.
- The transaction now verifies checkpoint ownership on every unit, including units without routed outputs. Use the `load` function provided by `CheckpointStore.layer` directly—wrapping `load` drops owner metadata and fails with `unsupported-composition`.

See [`Commit.transaction`](https://streamsy.dev/docs/reference/projection/commit#committransaction).

### Retire forgotten routed streams and undecodable checkpoints

- `Projection.forget(bound)` now marks routed output records as forgotten rather than deleting them, so `Projection.retire` can still locate and clean up those streams. Pass the binding from `declaration.at(params)` with its routed outputs. Replay checks that each forgotten stream starts empty and open.
- `Projection.retire` can now write a tombstone over an undecodable checkpoint and close or remove its streams in the same call.
- **Breaking change:** `Retired.streams` replaces `Retired.outputs` and `Retired.routed`, and `Projection.RetiredOutput` is renamed to `Projection.RetiredStream` with an `output` field. The array is sorted by output name and stream id, and includes shared wrapped outputs as well as the wrapped accepted stream under `"@streamsy/accepted"`.
- **Custom stores:** Custom `RouteRecords` implementations must now provide `state` and `forget` (`fromStore` from `@streamsy/projection/checkpoint` supplies both). See [`RouteRecords`](https://streamsy.dev/docs/reference/projection/checkpoint-store#routerecords).

**Upgrade notes:**

- Bindings forgotten under 0.13.0 have no route records left, so `retire` cannot discover their routed streams.
- Keep 0.13.0 runners stopped after upgrading: a 0.13.0 runner trusts forgotten route records and can append replayed data into old streams.
- Routed `forget` (for an existing checkpoint) and routed `retire` fail with `unsupported-composition` when the owner store cannot list the binding's records (such as a MySQL range containing a legacy hashed key).

See [Forget](https://streamsy.dev/docs/projections/checkpoints#forget) and [Retire a generation](https://streamsy.dev/docs/projections/checkpoints#retire-a-generation).

### Prevent `Projection.forget` cleanup from overlapping another host's first unit

In 0.13.0 on Postgres, a `Projection.forget` under `Commit.transactionWithCleanup` that saw no checkpoint could run its cleanup after another host committed the binding's first unit, leaving the checkpoint behind without its cleaned-up rows while reporting success.

- `Commit.transaction` and `Commit.transactionWithCleanup` now re-check the checkpoint after running cleanup. If a checkpoint exists, the forget rolls back and fails with a retryable `token-conflict`. This covers routed and unrouted bindings, including MySQL at `REPEATABLE READ` (requires `@streamsy/storage` at the same version).
- A cleanup callback that catches a SQL error now fails `forget` instead of reporting success on a rolled-back MySQL transaction.

Always stop and drain a binding's runners before calling `forget`. To clean up a binding left with a checkpoint and missing rows by an earlier release, stop its runners and forget it again, or raise `generation`. One limit remains: if two hosts race to commit a binding's first unit on Postgres or MySQL, one wins while the slower host can hit a non-retryable `invalid-output` (or `ViewDataFault` on a routed Views circuit), stopping the binding until `Registry.retry` or restart—run one host per binding. See [One writer](https://streamsy.dev/docs/projections/checkpoints#one-writer).

## @streamsy/projection@0.13.0

### Stop a binding on a permanent handler error

Mark a typed handler error as permanent with `readonly [NonRetryable] = true`, using `NonRetryable` from `@streamsy/projection`. When an attached binding fails with that error, the runner stops the binding at once. It does not step the retry schedule or call `onExhausted`. `registry.failed` reports the binding as stopped with `exhausted: false` and failure reason `non-retryable-handler`. Other handler errors and defects still retry under the declaration's `retry` policy. [Binding retry](https://streamsy.dev/docs/reference/projection/runners#binding-retry)

```ts
import { Schema } from "effect";
import { NonRetryable } from "@streamsy/projection";

class UnknownAccount extends Schema.TaggedError<UnknownAccount>()("UnknownAccount", {
  account: Schema.String,
}) {
  readonly [NonRetryable] = true;
}
```

### Rebuild a projection into a new generation

You can now change a projection's handler and replay its inputs into fresh output streams, while readers stay on the old streams until you move them. The steps are:

1. Wrap the projection's outputs with `Projection.perGeneration`, so each generation writes to its own streams.
2. Raise `generation` and attach. The runner replays the existing bindings in the background.
3. Wait with `Projection.caughtUp`, then move readers to the new streams.
4. Call `Projection.retire` to close or remove the old generation.

The next four sections describe each step. [Rebuild a projection](https://streamsy.dev/docs/projections/rebuild) is the full guide, with an in-place procedure and a side-by-side procedure.

### Put the generation in an output's address

Wrap a projection output, or the wake class in `accepted`, with `Projection.perGeneration(route, param)`. The named route parameter takes the declaration's `generation`. The binding's params fill the other parameters.

```ts
class Doubled extends Streamsy.json<Doubled>()("Derived/Doubled", "doubled/:user/g/:rev", {
  params: { user: Schema.String, rev: Schema.FiniteFromString },
  schema: Schema.Finite,
}) {}

const doubled = Projection.emit({
  id: "doubled",
  generation: 2,
  params: { user: Schema.String },
  inputs: { input: Input },
  outputs: { doubled: Projection.perGeneration(Doubled, "rev") },
  commit: Commit.transaction,
  emit: ({ item }) => ({ doubled: [item * 2] }),
});

// The projection writes to doubled/u1/g/2. A reader pins an address the same way.
const pinned = Doubled.at({ user: "u1", rev: 2 });
```

`Commit.transaction` and `Commit.producer` create a wrapped stream on its first write and check that it starts empty. A failed check fails the unit with the non-retryable reason `invalid-output`. Pass `{ shared: true }` as the third argument to skip the check for a stream that several writers share. Call `Projection.createOutputs(bound)` when readers need the streams before the first unit commits.

`Projection.isPerGeneration` recognizes a wrapped output, and `Projection.generationBinding` reads its route, generation parameter, `shared` flag and `routeBy` names. [Generation-addressed outputs](https://streamsy.dev/docs/projections/outputs#generation-addressed-outputs)

### Replay existing bindings when you attach a new generation

When you raise `generation` and attach the declaration, `Projection.attach` and `ProjectionWakeup.attach` list the live checkpoints for the projection id. Each binding with no checkpoint at the declared generation gets its generation-addressed outputs created and is queued for replay. A projection with no params queues its `{}` binding.

Pass `bindings` to either attach form to choose the replayed bindings yourself. It is an Effect that returns decoded params. Use it when you run a new projection id beside the old one, on MySQL, or when bindings that never committed a unit must replay.

```ts
Projection.attach(totalsV2, { durable: true, bindings: Projection.bindings(totalsV1) });
```

`Projection.bindings(declaration, { generation })` lists the decoded params of the bindings with live checkpoints for a projection id. Without `generation` it lists every generation. It fails with `InvalidBinding` when stored params no longer decode, and with `unsupported-composition` on MySQL and on checkpoint stores that cannot list.

When startup cannot list bindings, or the `bindings` Effect fails, attach logs the cause and skips startup replay. Live commits and triggers still run. [Choose the binding inventory](https://streamsy.dev/docs/projections/rebuild#choose-the-binding-inventory)

### Check replay progress

Capture `Projection.tails(binding)` once, then wait with `Projection.caughtUp(binding, tails)` until the binding's checkpoint reaches those input offsets and closes. Producers can keep writing while you wait. `caughtUp` has no timeout, so add your own.

`Projection.lag(binding, tails)` returns the committed and target offset of each input and a `caughtUp` flag. `Tails` is a Schema, so you can store the captured value and check it later. [Check replay progress](https://streamsy.dev/docs/projections/rebuild#check-replay-progress)

### Retire the old generation

Call `Projection.retire(declaration, params, { generation })` to fence the generation's checkpoint and remove its exclusively owned generation-addressed outputs. Pass `streams: "close"` to keep their data and let readers reach the end. The returned `Retired` value reports a `RetiredOutput` for each output. A later run of the retired generation fails with the non-retryable reason `retired-generation`. [Retire a generation](https://streamsy.dev/docs/projections/checkpoints#retire-a-generation)

Two more calls remove what a retired binding leaves in SQL:

- `Commit.transactionWithCleanup(cleanup)` runs your cleanup in the same transaction as `Projection.forget` or `Projection.retire`. Run the cleanup SQL with the supplied client, so a failure rolls back the lifecycle call and the cleanup together. [Commit reference](https://streamsy.dev/docs/reference/projection/commit#committransactionwithcleanup)
- `StateTable.clear(table)` deletes a retired stream's cached rows and applied offset. [Clear a retired stream's rows](https://streamsy.dev/docs/projections/state-table-operations#clear-a-retired-streams-rows)

### Route one binding's output to many streams

Pass `routeBy` to `Projection.perGeneration(route, generationParam, { routeBy: ["workspace", "date"] })` when one binding writes to several stream addresses. Each name in `routeBy` must be a distinct route parameter. The handler supplies those values, the generation fills the generation parameter, and the binding's params fill the rest. A routed output cannot be `shared`. [`Projection.perGeneration`](https://streamsy.dev/docs/reference/projection/constructors#projectionpergeneration)

Declare routed outputs in `Projection.process` with `Commit.transaction` or `Commit.transactionWithCleanup`. In `process`, `outputs[name]` is a `RoutedHandle`. Its `open(routeParams)` returns the stream address and creates the stream on first use, which must find the stream empty and open. The stream creation, the appends and the checkpoint commit in one transaction.

`Projection.retire` closes or removes the streams that a routed output opened and reports them in `Retired.routed`, an array of `RetiredOutput` for each output name. `Projection.forget` clears the binding's record of those streams. On MySQL, which cannot list owner records, routed retirement fails with `unsupported-composition`.

### Check these changes when you upgrade

- **Triggers rescan once.** Trigger cursors are now kept for each generation, so a new generation reads its trigger streams from the start. The stored cursors have no generation, so the first pass of each attached projection with `triggers` after the upgrade also reads them from the start. Expect that pass to schedule every member named in the retained trigger history once.
- **New fault reason.** `ProjectionFault.reason` gains `retired-generation`. Add it to exhaustive switches over `reason`.
- **Custom checkpoint stores.** `CheckpointStoreApi` gains two optional members, `list` and `routes`. `CheckpointStore.layer` and `CheckpointStore.layerMemory` implement both. A custom store without them keeps working, but attach skips startup replay, `Projection.bindings` fails, and opening a routed output fails with `unsupported-composition`. For a store built with `CheckpointStore.fromStore`, implement `EncodedStore.list(prefix)` to get both. [`CheckpointStoreApi`](https://streamsy.dev/docs/reference/projection/checkpoint-store#checkpointstoreapi)

## @streamsy/projection@0.12.0

### Set a retry policy for each projection declaration

All five projection constructors accept `retry` with a timing `schedule`, a failed-run
budget `maxAttempts`, and an `onExhausted(unit, failure)` hook. Both attach hosts stop
exhausted bindings and list them in `registry.failed` with `exhausted: true`.
`registry.retry` permits another run. New input during backoff preserves the budget
and stored deadline; success clears the failure count. The default remains unlimited
retry with delays doubling from 1 s to 1 hour. Use schedules based on step count;
schedules depending on input or elapsed time are unsupported.

Hooks run at least once before stopping; use `unit.key` to dedupe. When the exhausting
failure occurs before a unit forms, the hook is skipped and logged. Hook failures are
logged and the binding stops anyway. Non-retryable faults stop without the hook.
`ProcessContext.attempt` starts at 1 for each binding and increases after failures;
direct `Projection.run` and `Projection.follow` always supply 1.

**Breaking change:** `AttachOptions.retry` is now `passRetry`. Rename the option on
`Projection.attach` and `ProjectionWakeup.attach`. Its meaning is unchanged: it paces a
failed drain pass. `retry` now names the binding retry policy on a declaration, and an
attach call that still passes `retry` fails with a `RangeError`.

See [binding retry](https://streamsy.dev/docs/reference/projection/runners#binding-retry).

### Run projection bindings in parallel

Pass `concurrency` to `Projection.attach` or `ProjectionWakeup.attach` to run several
bindings at once. The default is `1`; each binding still runs its units in order,
and a failed binding backs off alone. The option must be a positive safe integer.

SQLite serializes `Commit.transaction` handlers; Postgres and MySQL run them up to
the pool size. `Commit.atLeastOnce` handlers run in parallel on every host. Keep
concurrency below the pool size, use the provided `SqlClient` in the handler's fiber,
and append to shared outputs last. See [binding concurrency](https://streamsy.dev/docs/reference/projection/runners#binding-concurrency).

### Read the offset of each local projection entry

`Slice.offsets` now gives the storage offset of every item on local inputs, and
`Entry.offset` gives the corresponding offset in entry handlers. Reading from
`entry.offset` resumes after that entry. Pinned retries retain the same offsets.
Non-empty HTTP inputs have `offsets` undefined and entries omit `offset`, because
the protocol supplies page boundaries only. Every empty slice has `offsets: []`,
including skipped inputs and timer-only units.
See [slices and entries](https://streamsy.dev/docs/reference/projection/units#slice).

### Schedule a projection family and rescan its trigger history

`registry.schedule` now accepts an array of bindings of one projection, validates
them all before writing marks, coalesces duplicates and wakes the runner once.
Empty arrays do nothing; mixed projection ids fail with `RangeError`. A storage
failure in a later chunk can leave earlier marks written; repeating the request is safe.

After a fold fix, call `registry.rescan(projectionOrId, WakeClassOrStreamId)` to reset
one trigger cursor to the start, clear its stop and wake the runner. The reset is
stored in both durability modes. A first attach already reads from the start, so a
new family needs no rescan. Both requests work on the process and alarm hosts.
See [runner requests](https://streamsy.dev/docs/projections/run#observe-attached-runners).

### Follow wake streams on another owner

`Projection.attach` can now use a wake stream on another owner in `triggers`.
Provide its stream class through the remote or other owner's protocol layer.
A scoped live-read follower wakes the runner on new rows. Each remote page's
cursor commits locally with its durable pending marks, so restart resumes from
that page boundary.

Remote read failures use `passRetry` after local bindings run; local commits
during pass backoff wait until it ends (up to 30 seconds by default). Invalid
trigger data stops only that stream. `ProjectionWakeup.attach` rejects remote
triggers with a `RangeError` naming the stream; it accepts only triggers on its
own storage owner. See [Trigger streams](https://streamsy.dev/docs/projections/run#trigger-streams).

### Observe, retry and trigger attached projections

`Projection.Registry` is a new in-process view of the runners that `Projection.attach` and `ProjectionWakeup.attach` start. Use it to check runner health, inspect failed bindings, request runs and wait until a projection has read your write. Trigger streams request projection runs from a wake stream; accepted streams publish how far a projection has read each input.

Provide `Projection.Registry.layer` below your attach layers so every attachment sees the same registry. On a Durable Object, put it in the object's `layer` next to `CheckpointStore.layer`. Each runner registers under its projection id and unregisters when its attach scope closes. Two attachments with the same id in one registry fail the layer build. See [Observe attached runners](https://streamsy.dev/docs/projections/run#observe-attached-runners).

The registry has these members:

- `status` reports each runner's `state`, consecutive pass `failures`, `retryAt`, `active`, `queued` and `lastPassAt`. Both modes include a `marks` summary with disjoint `fresh`, `sleeping`, `retryDue`, `backingOff` and `stopped` counts, plus `nextDueAt`. The runner's `failure`, `stopped` count and `triggers` expose pass, binding and trigger-stream stops. `status` fails with `StorageFault` if it cannot read the mark counts.
- `changes` emits the status list on subscription and after runner changes. A slow subscriber keeps at most one pending notification. The stream logs and skips failed snapshots and stays open.
- `schedule(binding)` makes that binding due for the next pass, overriding its backoff or stop. It returns before processing finishes and fails with `InvalidBinding` when encoded params do not round-trip through the attached declaration.
- `retry(target)` makes failed and stopped bindings due now and returns the number of changed marks. A repeat before execution returns zero. Pass an id or declaration to select its failed bindings and resume stopped trigger streams, or a binding to select one mark. Resuming a trigger stream or failed pass adds nothing to the count.
- `failed(target, { limit })` lists failed bindings, stopped first, then by deadline. Entries include encoded `params`, `attempts`, `dueAt` (`None` while stopped), `stopped` and the stored failure's time, optional reason and message. The limit defaults to 100 and accepts integers from 1 to 1,000. Durable failures remain readable after eviction.
- `events` emits future binding observations. `Ran` carries status, unit count and input offsets. `Failed` carries attempts, failure, stopped state and a retry deadline in either mode (`None` for a stop). `Dropped` reports a source removed before the binding's first run. Events have no replay and buffer without a limit, so close subscriptions when finished.
- `awaitOffset(binding, input, offset)` subscribes before reading the checkpoint, then waits for the requested input offset. A matching failed run prompts a checkpoint reread for units already committed. A different attached version or generation fails with `ProjectionFault` reason `identity-mismatch`. Add `Effect.timeout` to bound the wait.

Requests resolve by projection id and fail with `NotAttached` when it is missing. `schedule` and `retry` end the runner's current wait and permit one pass, including after a finite pass policy is exhausted. They preserve that policy: another failed pass after exhaustion returns straight to `failed`, with no automatic delay. On an object, both methods reconcile the host alarm before returning. `Projection.Registry.isFor(binding)` matches events by id and encoded params; `RunnerEvent` exports constructors, `$is` and `$match`.

Pass `triggers: [FolderWake]` to request source bindings from a fixed-address wake stream; the process host also supports remote triggers, while the alarm host accepts only its own storage owner's triggers. See [Trigger streams](https://streamsy.dev/docs/projections/run#trigger-streams) for ownership, cursor recovery and validation rules.

Set `accepted` to a wake class on a declaration to publish the offset of each input that moved in a unit. State tables use `StateTable.define(State, { accepted: TotalsAccepted })`. Accepted offsets require `Commit.transaction` or `Commit.producer`, including on folds. Transactional rows commit with the checkpoint; producer rows can be visible before checkpoint settlement and deduplicate on retry. `Streamsy.awaitWake(Wake, params, { stream, offset })` waits over local or remote storage. It reads from the start unless a fourth argument supplies `{ from: savedWakeCursor }`; only rows after that cursor can satisfy the wait. See [Accepted stream](https://streamsy.dev/docs/projections/accepted).

In `@streamsy/serve`, `Wakeup.bind(host)` receives `reconcile`, `retry` and retry `state` effects that work outside requests. An optional `retryable(cause)` returning `false` stops automatic run retries without stepping the schedule. A non-retryable `StorageFault` from `next` does the same. `changed` runs after a run or failed deadline read updates retry state. `ProjectionWakeup` supplies these hooks for the registry. A cold start makes stopped durable bindings due once; a still-broken binding stops again. Stopped marks alone arm no alarm. See [Projections on alarms](https://streamsy.dev/docs/servers/cloudflare/alarms#projections).

### Changed behaviour

**Breaking change:** Attached runners call the `passRetry` schedule for timing only, once per retryable failed pass with no failure input. A source write or trigger row records work without shortening backoff or ending a stop. In both durability modes, a retryable failed binding waits alone, by default from 1 s doubling to 1 hour; a finite pass schedule no longer bounds non-durable binding retries. See [binding retry](https://streamsy.dev/docs/reference/projection/runners#binding-retry) for the declaration's policy. The runner stops the binding, trigger stream or pass that hits a known non-retryable fault and reports the cause. `registry.retry` or the next runner start resumes it; `registry.schedule` also resumes a selected binding. Handler errors and defects retry on attached runners. `Projection.follow` now retries `token-conflict` and `output-busy` on its schedule, while other handler errors and defects still fail its stream. Structurally typed wake outputs must supply `paramCodecs`, as `Streamsy.wake` classes already do; structural `Attachable` values must include `version` and `generation`.

**Breaking change:** Opening SQL storage upgrades the schema to version 3, including each existing Durable Object on its next acquisition. Projection marks gain `stopped`, `failed_at`, `failure_reason`, `failure_message` and nullable `wake_at`; existing sequence numbers, attempts and due times remain intact. There is no downgrade from schema version 3: a release that supports only version 2 refuses the database, so returning to it requires restoring a pre-upgrade backup. Custom durable-mark adapters must implement the updated internal `ProjectionMarksApi`: `summary`, stopped-aware reads and `retry`, `fail` in place of `defer`, plus `resume`, bounded `failed` and `sleep`; the marks-level `deferred` method is removed. `clear` resets failure state when a newer sequence keeps the mark. The storage contract checks these rules. See the [marks contract](https://streamsy.dev/docs/reference/core/memory-storage#types-with-no-exported-name) and [SQL migration rules](https://streamsy.dev/docs/reference/storage/sql-storage#sqlstoragelayer).

### Request a process wake with its checkpoint

Return `Projection.wakeAt(time)` from a `Projection.process` handler to record its
next wake, using epoch milliseconds or an Effect `DateTime`. Returning nothing
clears it. Use `Commit.atLeastOnce` or `Commit.transaction`, including compensation
wrappers, so the request commits with the checkpoint.

A due unit carries the recorded time as `unit.due`, and its key includes that time
for idempotent retries. Without new input it has empty slices and ranges.
`Progress.wakeAt` reports the next recorded deadline. `run`, `pass` and `follow`
handle due timers when they run; each cycle makes at most one due unit without
input. Attached process and Durable Object hosts wake at the deadline. A process
wait rechecks wall time every 30 seconds while a deadline exists, including after
host suspend. Backoff delays a wake; stops keep it until retry or restart. With
`durable: false`, restart or eviction loses the wake until the next input. Return
a time after `unit.due` to avoid repeatedly running an immediately due timer.

`RunnerStatus.marks.sleeping` counts sleeping bindings separately from pending and
failed work. `RunnerEvent.Ran.wakeAt` reports the next recorded wake. Schema
version 3 adds nullable `wake_at` to projection marks on every SQL host.

See [process timers](https://streamsy.dev/docs/projections/process#timers).

## @streamsy/projection@0.10.0

### State tables and keyed folds

`StateTable` keeps the rows of a Durable State stream in SQL tables on the stream's storage. `StateTable.define(State)` returns an ordinary projection declaration, so `Projection.attach`, `ProjectionWakeup.attach`, `follow` and `run` keep the table current. Attach it with `durable: true`. The table stores the canonical JSON of each change's value and never decodes it with the collection schema, so `StateTable.rebuild` makes the same rows from the stream alone. A bound table reads rows with `get`, `getMany` and `list`, each with the offset that the rows reflect. `sync` applies the stream up to its tail in the caller's transaction, and fails only with a `ProjectionFault`. The declaration and the bound table have the types `StateTable.Declaration` and `StateTable.Bound`.

Run `StateTable.migrate(sql)` once at host start. It creates `streamsy_state_heads` and `streamsy_state_rows` outside the storage migrations, so the storage version does not change. The tables support the SQLite family only.

`FoldByKey.process(table, spec)` builds a `Projection.process` handler that folds input records into one collection of a State stream. Each unit calls `sync`, loads the previous rows of its keys, and appends one change for each changed key. The handler has the type `FoldByKey.Handler`. A row whose key field differs from its key fails the unit with a typed `FoldByKeyError`. A wire, SQL or schema failure fails it with a `ProjectionFault`. A callback that throws is a defect, as in every other handler kind. Attach folds with `durable: true`.

`Projection.attach` now logs and retries a pass that fails with a defect. Before, a handler that threw under memory-mode `attach` ended the runner of that projection with no log.

In `@streamsy/core`, a `Streamsy.state` class and a `StreamRoute.state` route keep their `params` codec record as `paramCodecs`. `StateTable.define` reads it.

**Breaking change:** a `Projection.process` handler takes a context object as its third argument, in place of `prepared`. Write `process: (batch, unit, { prepared }) => ...` where you wrote `process: (batch, unit, prepared) => ...`. A handler that takes only `(batch, unit)` is unchanged, and so is `prepare`. The context types are `ProcessContext` and `OutputsContext`.

`Projection.process` takes an optional `outputs` map under `Commit.transaction`. The Commit checks each output before the handler runs, and `prepare` and `process` get the bound output addresses as `context.outputs`.

A fold's `id`, `version` and `generation` are its identity. A renamed fold folds its sources again over the old rows and doubles counters with no error, so treat a rename as a fold fix. See [Operate State tables](https://streamsy.dev/docs/projections/state-table-operations).

## @streamsy/projection@0.9.0

### Require Effect 4.1.2 and Alchemy beta.85

**Breaking change:** Streamsy now requires `effect` and any `@effect/*` peers at `^4.1.2`. Upgrade the Effect family together. Effect 4.0.x is no longer supported.

The optional Alchemy integration requires exactly `alchemy@2.0.0-beta.85` and `@alchemy.run/cloudflare@2.0.0-beta.85`. Install both when using `@streamsy/serve/alchemy`, and replace imports from `alchemy/Cloudflare` with `@alchemy.run/cloudflare`.

Examples and tests use Effect's `SecureRandom` and `Digest` services with explicit crypto layers. Content hashes remain SHA-256 with the same bytes and lowercase hex encoding.

`expiresAt` and `Stream-Expires-At` follow Effect 4.1 `DateTime.make`, which also accepts a trailing lowercase `z` or `gmt`, such as `2028-01-01T00:00:00z`.

## @streamsy/projection@0.8.0

### The Cloudflare object host logs requests

`StreamsyObject.make` and `objectHandlers` built their web handler with `disableLogger: true`. A handler defect answered 500 and logged nothing, so the only sign of the failure was the status code.

- The object host now logs each request at INFO level when it ends, with method, URL and status.
- A failed request logs its full cause, stack included.
- Each request adds one line to Workers logs.

### New @streamsy/outbox package replaces action delivery in serve

**Breaking change:** `@streamsy/serve/action` and its sub-paths `/action/errors`, `/action/outbox`, `/action/runtime` and `/action/sql` are removed. Use `@streamsy/outbox`.

`@streamsy/outbox` is a transactional outbox for Effect. `Outbox.enqueue` writes entries in the caller's transaction, and `Outbox.drain` delivers them at least once in bounded passes. The package has no host dependencies.

- `@streamsy/outbox/memory` exports `layerMemory`. `@streamsy/outbox/sql` exports `layerSql` and `migrateOutbox` for SQLite, Postgres and MySQL. `layerSql` builds the store from the `SqlClient` in context, so `enqueue` joins the application's active transaction.
- A drain pass delivers at most `limit` entries (default 100) and returns `{ name, attempted, delivered, retried, deadLettered, more }`. `more` is true when the pass stopped at its limit.
- `OutboxBacking` is the adapter protocol behind `OutboxStore`. Its `settle(id, Settlement)` takes a `Settlement` with `Delivered`, `Retry` and `Dead` cases.
- `@streamsy/serve/cloudflare` and `@streamsy/serve/alchemy` export `OutboxWakeup.make(definition, deliver, options)`, which drains an outbox scope from a host wakeup.

SQL stores no longer create their table. Run `migrateOutbox(sql)` once at startup on every dialect, outside any transaction. Without it, store operations fail with `OutboxUnavailable`.

The new table is `streamsy_outbox`. This release does not migrate the old `streamsy_effect_outbox` table or change it. Entries still pending in that table are not delivered after the upgrade. Drain them with the previous release before you upgrade, or move them yourself.

### Create rejects invalid options and JSON with typed errors

`CreateError` gains `InvalidCreateRequest`, a minor change to the public error union. Code that exhaustively matches `CreateError` must handle the new member. The error carries `id` and `message`.

- A remote plain create HTTP 400 response now fails with `InvalidCreateRequest` and the response message instead of `InvalidForkRequest`. Responses with `stream-not-supported` still fail with `NotSupported`, and fork HTTP 400 responses still fail with `InvalidForkRequest`.
- Local and remote creates reject `ttlSeconds` combined with `expiresAt`, and reject an `expiresAt` value that `DateTime.make` cannot parse. Both fail with `InvalidCreateRequest` before storage access or an HTTP request. Local creates previously used the TTL when both were set and could create a stream that never expired for an invalid date. HTTP continues to return 400 with the same messages.
- Invalid JSON `initialData` on a plain create, including typed toolkit create, now fails with `InvalidCreateRequest` and `Invalid JSON` instead of dying with `SyntaxError`. Fork initial data fails with `InvalidForkRequest` and the same message. Append framing and its invalid JSON behavior are unchanged.

## @streamsy/projection@0.7.0

### Durable pending marks for attached projections

`Projection.attach` and `ProjectionWakeup.attach` take `durable` and `batch` options. In durable mode, the commit that writes an input also writes a pending mark for each binding it touches, in the same transaction. A binding that catches up deletes its mark by a `seq` compare, so a write during the run keeps it. After a restart, an eviction or a deploy, the runner finds the marks and runs those bindings. A failed binding defers its own mark (1 s doubling to 1 hour) and no longer puts its neighbours on backoff.

`ProjectionWakeup.attach` is durable by default. `Projection.attach` stays in memory unless you pass `durable: true`. Durable mode needs a local memory or SQL storage layer and fails at acquisition over remote storage.

Storage migration 2 adds the `streamsy_projection_pending` table. A database opened by this release cannot be opened by an older one. `Wakeups.status` reports a `deferred` count for each wakeup.

### Long checkpoint keys save on MySQL

`CheckpointStore.layer` keeps each checkpoint under a key that holds the projection id, version, generation and params JSON. MySQL stores that key in a `VARCHAR(512)` column. Before this release, a projection whose params made the key longer than 512 characters failed every checkpoint save on MySQL with `ER_DATA_TOO_LONG`, so it never advanced.

- On MySQL, SQL storage now hashes an owner record key over 512 characters to the first 447 characters, `#` and the SHA-256 hex of the full key. Two long keys that share their first 512 characters stay apart.
- A key of at most 512 characters does not change, so stored checkpoints stay readable.
- SQLite, Durable Object storage and Postgres store the key as `TEXT` and keep every key as it is, so checkpoints they already hold under long keys stay readable.
- `recordKey` from `@streamsy/projection/checkpoint` is unchanged. A custom Commit that stores records elsewhere still sees the full key and must fit it into its own store's key limit.

### Long projection producer ids are hashed

A `Commit.producer` projection, such as `Projection.wake`, appends under the producer id `<id>/v<version>/<params JSON>`. Storage refuses a producer id over 255 code points. Before this release, a binding with long params, such as a hex folder id over 113 UTF-8 bytes, failed every append with a `mutate.producer-id` storage fault and never wrote its output.

- A producer id of at most 255 code points does not change. Deployed outputs keep their producer state under it.
- A longer producer id becomes `<id>/v<version>/#<sha256>`, where the SHA-256 is the hex digest of the full id. When the projection id itself is long, the part before `#` is cut so the whole id is 255 code points. The digest uses WebCrypto (`crypto.subtle`), which Bun, Node, workerd and browsers in a secure context provide.
- A retry of a pinned unit sends the same hashed id, so the output still answers `Duplicate`.

## @streamsy/projection@0.6.0

### Projection.follow returns a Stream

**Breaking change:** `Projection.follow` now returns a `Stream` of committed cycles. Before this release, it returned a scoped `Fiber` and needed `Scope`. `Progress` now has a state type parameter, and `Projection.run` returns the same `Progress`.

A follower in the same process, such as a page, a TUI or a test, had no signal for each commit. It could only join the fiber for the end, or poll the checkpoint store. Now each element of the stream tells it what the follower committed.

- `Projection.follow(bound, options)` gives `Stream<Progress<StateValue<S>, Inputs>, E | ProjectionFault, R>`. The stream has no `Scope` requirement and forks no fiber. To run the stream is to be the writer, so run one `follow` stream per key.
- `Progress<S, Inputs>` extends `Snapshot<S, Inputs>`. It has the `state` and `offsets` that the last pass of the cycle left, and `status`, `units` and `items`. Neither `run` nor `follow` loads the checkpoint again after the cycle. `status` is `caught-up`, `limit-reached` or `source-closed`.
- The stream emits after the first cycle, also with 0 units. After that, it emits after each cycle that commits a unit or reaches `limit`, and after the last cycle when every input is closed. Then it ends. A wake that finds no input emits nothing.
- The stream is pull based. The next cycle runs only when the consumer pulls, so a slow consumer gets one element for all the input that arrived meanwhile. Use `Stream.buffer({ capacity: 1, strategy: "sliding" })` to decouple a slow consumer.
- An invalid `limit` or `repairIntervalMs` fails the stream at its first pull. Before, the `yield*` of `follow` failed.
- `Projection.run` now gives the `state` that the last pass of its cycle left. `Projection.pass` keeps its own result type, `PassResult`, with the `progress` status and no state.
- One `Projection.follow` span covers the whole stream. Each `Projection.pass` and `Projection.hint` span of the follower is a child of it.
- The wake hints, the retry of `storage-failure` and the repair interval do not change.

To migrate a caller:

```ts
Effect.gen(function* () {
  // Run to the end, as Fiber.join did.
  yield* Stream.runDrain(Projection.follow(bound));

  // Run in the background of a scope, as follow did.
  yield* Projection.follow(bound).pipe(Stream.runDrain, Effect.forkScoped);

  // Wait for an offset without a second writer.
  yield* Projection.follow(bound).pipe(
    Stream.takeUntil(({ offsets }) => offsets.input === offset),
    Stream.runLast,
  );
});
```

### Wake streams, attached projections and object links

- `Streamsy.wake<Self>()(name, template, { params })` is a State preset with `wakeCollections` and the normal State service. `WakeRow` holds `{ stream, offset, closed? }`, keyed by source id. `DurableState.materialized` and `materialize` fold State events into collection maps.
- `Projection.wake({ id, params, input, output })` uses `mapAccumBatch` and `Commit.producer` to publish source tails, closes and removals. The producer creates a missing output on its own owner. An output on another owner must exist first: its owner creates it at start with the State service's `create`. HTTP access remains mount policy.
- **Behaviour change from 0.5.0:** a close without new items gives one unit with `slice.closed`. A gone input with a checkpoint gives one final unit with `slice.gone`, then forgets the binding by default. The last `Projection.run` result or `Projection.follow` element holds the state and offsets that this final unit committed, and `Projection.read` then gives the initial state. Custom Commits need `remove` for this outcome. Live history gaps remain faults. A removal between a pin and its settle still fails with `range-unreproducible`, and the pin stays; retain history until pending units settle. `Projection.follow` already ended on close in 0.5.0; use attachment or another run for a later removal.
- **Upgrade from 0.5.0:** the checkpoint now records which inputs are closed. A 0.5.0 checkpoint has no such record, so the first run after the upgrade gives one extra unit with no items and `slice.closed` for each input that was already closed. A handler that expects items in every unit sees this unit once.
- **Breaking change:** `Storage.committed` is a scoped `PubSub` subscription to the ids of each later local commit. The subscription exists when the effect returns. Custom storage adapters must add it, or they do not compile.
- `Projection.attach(projection, { limit, retry })` runs affected bindings on Bun or Node. `ProjectionWakeup.attach(projection, options)` is acquired in a Cloudflare object scope. Both share one runner and accept a retry `Schedule`. The default schedule has no end. A custom schedule with an end stops the attachment for all bindings of that projection when the schedule ends, until the process or object restarts. Pending bindings live in memory; a restart, such as a crash, a Cloudflare eviction or a deploy, can delay work until the next source commit. Remote process commits do not appear.
- `Streamsy.followWake` and `wakeActions` compare rows with a saved cursor map and return `WakeItem` actions: `Dirty`, `Removed`, `Reset` and `Snapshot`.
- `ObjectStreams.layer(group, { objects, placement, prefix })` routes stream services through object stubs. `ObjectFetch` returns an Effect with typed HTTP client errors, preserving caller context and cancellation. Alchemy's `objectFetch` adapts its binding.
- `ObjectLink.make({ group, prefix, placement, access })` declares the internal owner routes and caller client together. Register `link.routes` and host `links`. `link.client(objects)` takes a namespace or an `ObjectFetch` and returns one `LinkClient` shape for both hosts. `StreamsyObject` takes `links: (env) => [...]`; Alchemy `objectHandlers` takes `links: [link.client(objectFetch(binding))]`. Local storage cannot also provide a linked declaration.
- **Behaviour change from 0.5.0:** public Cloudflare and Alchemy routers return 404 for paths whose first segment starts with `__`. Internal links require a `/__` prefix. Apps using root stream ids starting with `__` must rename them or use another prefix.

## @streamsy/projection@0.5.0

### Durable State control messages

**Breaking change:** the item type of a State address now includes control messages. Code that reads `type`, `key` or `headers.operation` from a State item must narrow the item first.

A State stream can now hold the control messages of the Durable State protocol: `snapshot-start`, `snapshot-end` and `reset`. Before this release, `StreamAddress.state` failed to decode a stored control. `Streams.decode` then failed the whole batch with a `DecodeFault`, and a follow or projection on that address stopped.

- `StreamAddress.state` decodes and encodes controls. The items of a `StateAddress`, a `Streamsy.state` route and `Serve.state` have the new type `StreamAddress.CollectionsEvent<C>`. That type is `CollectionsChange<C> | StateControl`. It was `CollectionsChange<C>`.
- `StreamAddress.isControl` and `StreamAddress.isChange` narrow an item. `StreamAddress.reset()`, `StreamAddress.snapshotStart()` and `StreamAddress.snapshotEnd()` make a control. Each one takes an optional `{ offset }`.
- `StreamAddress.stateControl` is the codec for one control. `StreamAddress.stateChange` stays the codec for the changes of one collection.
- `DurableState.changes` and projection outputs accept `DurableState.Entry<C>`, which is a row change or a control. A control is written unchanged. With an `offset`, a control that has no offset gets that offset.
- `Serve.state` serves stored controls as they are. It still refuses the `x-streamsy-state-reset: snapshot` request header.

To migrate a reader that folds State items, skip controls or handle them:

```ts
if (StreamAddress.isControl(item)) {
  if (item.headers.control === "reset") rows.clear();
} else if ("value" in item) rows.set(item.key, item.value);
else rows.delete(item.key);
```

## @streamsy/projection@0.4.0

### Streamsy 0.4: a new library, written in Effect

Streamsy 0.4 is a complete rewrite in [Effect](https://effect.website) 4. It shares only the name and the Durable Streams protocol with the 0.2 releases. No code or API carries over, so treat it as a new library and not as an upgrade.

Four packages ship together and share one version:

- `@streamsy/core` has the protocol, typed stream refs, stream services, a memory Layer, the HTTP app and a remote client.
- `@streamsy/storage` is new. It stores streams in SQLite on Bun and in Durable Objects.
- `@streamsy/projection` is new. It runs checkpointed projections with named stream, rows and value outputs.
- `@streamsy/serve` is new. It has HttpRouter routes, the Bun and Cloudflare hosts and action delivery.

`effect` is now a peer dependency (`^4.0.5`), so install it beside Streamsy:

```sh
bun add @streamsy/core effect
```

The guides and API reference are at [streamsy.dev](https://streamsy.dev).
