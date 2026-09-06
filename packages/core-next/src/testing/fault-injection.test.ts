import { expect, it } from "bun:test";
import { Effect, Exit, Option } from "effect";
import { faultyStorage } from "./fault-injection.ts";
import { layer } from "../storage/memory/layer.ts";
import { Storage } from "../storage/storage.ts";
import { ProducerId, StreamId } from "../schema/index.ts";
import { validateProducer } from "../policy/producer-idempotency-service.ts";
import { ZERO_OFFSET, next } from "../offset/index.ts";
for (const when of ["before", "after"] as const) {
  it(`fault injection fails once ${when} the Nth mutation and retries the same producer tuple exactly once`, () =>
    Effect.gen(function* () {
      const storage = yield* Storage;
      const id = StreamId.make("s");
      yield* storage.mutate({
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
      });
      const producerId = ProducerId.make("p");
      const tuple = { epoch: 4, lastSeq: 0 };
      const offset = next(ZERO_OFFSET);
      const operation = {
        _tag: "Append" as const,
        streamId: id,
        messages: [{ offset, timestamp: 7, data: new TextEncoder().encode("once") }],
        patch: { currentOffset: offset },
        producer: { producerId, expected: Option.none(), next: tuple },
      };
      const failure = yield* Effect.result(storage.mutate({ operations: [operation] }));
      expect(failure).toMatchObject({
        _tag: "Failure",
        failure: { _tag: "StorageFault", retryable: true },
      });
      expect(yield* storage.producer(id, producerId)).toEqual(
        when === "after" ? Option.some(tuple) : Option.none(),
      );
      expect((yield* storage.messages(id, {})).length).toBe(when === "after" ? 1 : 0);
      // Replay the identical mutation: storage CAS owns at-most-once persistence.
      // The later protocol batch will turn the persisted tuple into a duplicate acknowledgement.
      const retry = yield* storage.mutate({ operations: [operation] });
      expect(retry).toMatchObject(
        when === "after" ? { _tag: "Rejected", reason: "producer" } : { _tag: "Applied" },
      );
      const persisted = Option.getOrThrow(yield* storage.producer(id, producerId));
      expect(persisted).toEqual(tuple);
      expect(validateProducer(persisted, tuple.epoch, tuple.lastSeq)).toEqual({
        _tag: "Duplicate",
        epoch: 4,
        lastSeq: 0,
      });
      expect(Option.getOrThrow(yield* storage.record(id)).currentOffset).toBe(offset);
      expect(yield* storage.messages(id, {})).toEqual(operation.messages);
    })
      .pipe(
        Effect.provide(faultyStorage(layer(), { failOn: 2, when })),
        Effect.scoped,
        Effect.runPromiseExit,
      )
      .then((exit) => {
        expect(Exit.isSuccess(exit)).toBe(true);
      }));
}
