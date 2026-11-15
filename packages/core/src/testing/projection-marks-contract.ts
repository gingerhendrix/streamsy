import { Array, Effect, Option } from "effect";
import type { ProjectionMarksApi } from "../internal/projection-marks.ts";

// No test-runner imports: the same assertions execute inside real workerd.
const check = (condition: boolean, message: string) =>
  condition ? Effect.void : Effect.die(new Error(`Projection marks contract: ${message}`));

/** The pending work, sleep and stop/failure contract, shared by storage hosts and the standalone memory store. */
export const projectionMarksContract = (marks: ProjectionMarksApi) =>
  Effect.gen(function* () {
    const projection = "marks-contract";
    const failure = { at: 123, reason: Option.some("invalid-record"), message: "x".repeat(1_100) };
    const savedFailure = { ...failure, message: "x".repeat(1_000) };

    const read = (params: string) =>
      marks.read(projection, [params]).pipe(Effect.map((rows) => rows.get(params)));

    yield* check((yield* marks.resume(projection)) === 0, "empty resume");
    yield* marks.fail(projection, "a-stop", { attempts: 2, failure, dueAt: Option.none() });
    yield* marks.fail(projection, "b-stop", {
      attempts: 1,
      failure: { at: 124, reason: Option.none(), message: "handler" },
      dueAt: Option.none(),
    });
    yield* check(Option.isNone(yield* marks.nextDue(projection)), "stops have no deadline");
    yield* check(
      (yield* marks.due(projection, Number.MAX_SAFE_INTEGER, 10)).length === 0,
      "stops never due",
    );
    const stoppedOnly = yield* marks.summary(projection, 4_000);
    yield* check(
      stoppedOnly.stopped === 2 &&
        stoppedOnly.fresh === 0 &&
        stoppedOnly.sleeping === 0 &&
        stoppedOnly.retryDue === 0 &&
        stoppedOnly.backingOff === 0 &&
        Option.isNone(stoppedOnly.nextDueAt),
      "stopped-only summary",
    );
    yield* Effect.orDie(marks.mark(projection, ["a-stop"]));
    const raised = yield* read("a-stop");
    yield* check(
      raised?.seq === 2 &&
        raised.stopped &&
        raised.attempts === 2 &&
        Option.isSome(raised.failure) &&
        raised.failure.value.at === 123 &&
        Option.contains(raised.failure.value.reason, "invalid-record") &&
        raised.failure.value.message === savedFailure.message,
      "mark preserves stop and truncated failure",
    );

    yield* marks.fail(projection, "future", { attempts: 3, failure, dueAt: Option.some(6_000) });
    yield* marks.fail(projection, "past", { attempts: 4, failure, dueAt: Option.some(3_000) });
    yield* Effect.orDie(marks.mark(projection, ["fresh"]));
    yield* marks.fail("marks-other", "other", { attempts: 1, failure, dueAt: Option.none() });
    yield* marks.sleep(projection, "sleeping", 0, 5_000);
    const summary = yield* marks.summary(projection, 4_000);
    yield* check(
      summary.fresh === 1 &&
        summary.sleeping === 1 &&
        summary.retryDue === 1 &&
        summary.backingOff === 1 &&
        summary.stopped === 2 &&
        Option.contains(summary.nextDueAt, 0),
      "five disjoint summary counts",
    );
    const listed = yield* marks.failed(projection, 3);
    yield* check(
      Array.join(
        Array.map(listed, (row) => row.params),
        ",",
      ) === "a-stop,b-stop,past",
      "failed is bounded, stopped first, then dueAt",
    );
    yield* check(
      Option.isSome(listed[1]!.failure) &&
        Option.isNone(Option.getOrThrow(listed[1]!.failure).reason),
      "handler failure has no reason",
    );
    yield* check((yield* marks.failed(projection, 0)).length === 0, "zero failed limit");
    yield* check((yield* marks.failed("absent", 10)).length === 0, "empty failed listing");
    yield* check(
      Array.join(
        Array.map(yield* marks.due(projection, 4_000, 10), (row) => row.params),
        ",",
      ) === "fresh,past",
      "due excludes stops",
    );
    yield* check((yield* marks.retry(projection, "fresh")) === 0, "retry ignores fresh marks");
    yield* check(
      (yield* marks.retry(projection, "a-stop")) === 1,
      "retry selects stopped mark at dueAt zero",
    );
    yield* check((yield* marks.retry(projection, "a-stop")) === 0, "repeat retry changes nothing");
    const retried = yield* read("a-stop");
    yield* check(
      retried?.seq === 2 &&
        retried.attempts === 2 &&
        retried.dueAt === 0 &&
        !retried.stopped &&
        Option.isSome(retried.failure) &&
        retried.failure.value.message === savedFailure.message,
      "retry preserves seq, attempts and failure",
    );
    yield* check((yield* marks.resume(projection)) === 1, "resume selects remaining stop");
    yield* check((yield* marks.resume(projection)) === 0, "repeat resume changes nothing");
    const resumed = yield* read("b-stop");
    yield* check(
      resumed?.seq === 1 &&
        resumed.attempts === 1 &&
        resumed.dueAt === 0 &&
        !resumed.stopped &&
        Option.isSome(resumed.failure) &&
        resumed.failure.value.at === 124,
      "resume preserves failure and attempts",
    );
    yield* check((yield* read("future"))?.dueAt === 6_000, "resume keeps backoff");
    yield* check((yield* marks.summary("marks-other", 0)).stopped === 1, "projection isolation");

    // A success must reset a raised mark, whether it used to back off or stop.
    for (const dueAt of [Option.some(9_000), Option.none<number>()]) {
      yield* marks.fail(projection, "moved", { attempts: 7, failure, dueAt });
      const before = (yield* read("moved"))!;
      yield* Effect.orDie(marks.mark(projection, ["moved"]));
      yield* marks.clear(projection, "moved", before.seq);
      const kept = yield* read("moved");
      yield* check(
        kept?.seq === before.seq + 1 &&
          kept.attempts === 0 &&
          kept.dueAt === 0 &&
          !kept.stopped &&
          Option.isNone(kept.failure),
        "clear moved seq resets all failure state",
      );
      yield* marks.clear(projection, "moved", kept!.seq);
      yield* check((yield* read("moved")) === undefined, "clear matching seq deletes");
    }

    yield* marks.clear(projection, "absent", 1);
    yield* marks.remove(projection, "b-stop");
    yield* check((yield* read("b-stop")) === undefined, "remove a failed mark");
    // Updating a stopped mark with a backoff replaces all failure fields, including a nullable reason.
    yield* marks.fail(projection, "replace", { attempts: 1, failure, dueAt: Option.none() });
    yield* marks.fail(projection, "replace", {
      attempts: 2,
      failure: { at: 456, reason: Option.none(), message: "new" },
      dueAt: Option.some(7_000),
    });
    const replaced = yield* read("replace");
    yield* check(
      replaced?.seq === 1 &&
        replaced.attempts === 2 &&
        !replaced.stopped &&
        replaced.dueAt === 7_000 &&
        Option.isSome(replaced.failure) &&
        replaced.failure.value.at === 456 &&
        replaced.failure.value.message === "new" &&
        Option.isNone(replaced.failure.value.reason),
      "fail replaces failure fields and clears stop",
    );

    // Isolate deadlines from the fresh/retry rows above so nextDue is observable.
    const timers = "timer-marks-contract";

    const timer = (key: string) =>
      marks.read(timers, [key]).pipe(Effect.map((rows) => rows.get(key)));

    yield* marks.sleep(timers, "absent", 0, 5_000);
    const inserted = (yield* timer("absent"))!;
    yield* check(
      inserted.seq === 1 &&
        inserted.dueAt === 5_000 &&
        Option.contains(inserted.wakeAt, 5_000) &&
        inserted.attempts === 0 &&
        !inserted.stopped &&
        Option.isNone(inserted.failure),
      "sleep inserts absent row",
    );
    yield* check((yield* marks.due(timers, 4_999, 10)).length === 0, "not due before wake");
    yield* check(Option.contains(yield* marks.nextDue(timers), 5_000), "nextDue includes sleep");
    const dueSleep = yield* marks.due(timers, 5_000, 10);
    yield* check(
      dueSleep.length === 1 && Option.contains(dueSleep[0]!.wakeAt, 5_000),
      "due at wake",
    );
    yield* check(
      (yield* marks.retry(timers)) === 0 &&
        (yield* marks.resume(timers)) === 0 &&
        (yield* marks.failed(timers, 10)).length === 0 &&
        (yield* timer("absent"))?.dueAt === 5_000,
      "retry resume failed ignore sleepers",
    );
    yield* marks.sleep(timers, "absent", 1, 6_000);
    yield* check(
      Option.contains((yield* timer("absent"))!.wakeAt, 6_000),
      "matching sleep moves wake",
    );
    yield* Effect.orDie(marks.mark(timers, ["absent"]));
    const woken = (yield* timer("absent"))!;
    yield* check(
      woken.seq === 2 && woken.dueAt === 0 && Option.isNone(woken.wakeAt),
      "mark wakes sleep now",
    );
    yield* marks.sleep(timers, "absent", 1, 7_000);
    const mismatch = (yield* timer("absent"))!;
    yield* check(
      mismatch.seq === 2 &&
        mismatch.dueAt === 0 &&
        mismatch.attempts === 0 &&
        !mismatch.stopped &&
        Option.isNone(mismatch.wakeAt) &&
        Option.isNone(mismatch.failure),
      "sleep mismatch stays fresh",
    );
    yield* marks.fail(timers, "absent", { attempts: 3, failure, dueAt: Option.some(8_000) });
    yield* Effect.orDie(marks.mark(timers, ["absent"]));
    yield* check((yield* timer("absent"))?.dueAt === 8_000, "mark preserves backoff");
    yield* marks.sleep(timers, "absent", 3, 9_000);
    const reset = (yield* timer("absent"))!;
    yield* check(
      reset.attempts === 0 &&
        !reset.stopped &&
        Option.isNone(reset.failure) &&
        Option.contains(reset.wakeAt, 9_000),
      "sleep resets failure state",
    );
    yield* marks.clear(timers, "absent", 2);
    yield* check(
      Option.isNone((yield* timer("absent"))!.wakeAt) && (yield* timer("absent"))?.dueAt === 0,
      "clear mismatch clears sleep",
    );
    yield* marks.sleep(timers, "absent", 3, 9_000);
    yield* marks.clear(timers, "absent", 3);
    yield* check((yield* timer("absent")) === undefined, "clear matching sleep deletes");

    for (const dueAt of [Option.some(10_000), Option.none<number>()]) {
      yield* marks.sleep(timers, "failed-sleep", 0, 9_000);
      yield* marks.fail(timers, "failed-sleep", { attempts: 1, failure, dueAt });
      yield* check(Option.isNone((yield* timer("failed-sleep"))!.wakeAt), "fail clears sleep");
      yield* marks.remove(timers, "failed-sleep");
    }

    yield* marks.sleep(timers, "past-sleep", 0, 1);
    yield* check(
      Option.contains((yield* timer("past-sleep"))!.wakeAt, 1) &&
        (yield* marks.due(timers, 10_000, 10)).length === 1,
      "past wake stored unchanged",
    );
  });

/** Failure writes and state resets join their owner's transaction. */
export const projectionMarksTransactionContract = (
  marks: ProjectionMarksApi,
  boundary: import("../storage/commit-boundary.ts").CommitBoundaryApi,
) =>
  Effect.gen(function* () {
    const failure = { at: 1, reason: Option.some("invalid-record"), message: "bad" };
    yield* marks.fail("rollback-stop", "a", { attempts: 2, failure, dueAt: Option.none() });
    yield* Effect.orDie(marks.mark("rollback-stop", ["a"]));
    const before = (yield* marks.read("rollback-stop", ["a"])).get("a");

    for (const change of [
      marks.resume("rollback-stop"),
      marks.sleep("rollback-stop", "a", 2, 20),
      marks.clear("rollback-stop", "a", 1),
      marks.fail("rollback-stop", "a", { attempts: 3, failure, dueAt: Option.some(10) }),
    ]) {
      const exit = yield* Effect.exit(
        boundary.withTransaction(change.pipe(Effect.andThen(Effect.fail("rollback")))),
      );

      yield* check(exit._tag === "Failure", "transaction failed");
      const after = (yield* marks.read("rollback-stop", ["a"])).get("a");
      yield* check(
        after?.seq === before?.seq &&
          after?.attempts === 2 &&
          after.stopped &&
          Option.isSome(after.failure) &&
          after.failure.value.at === 1,
        "transaction rollback restores stop and failure",
      );
    }
  });
