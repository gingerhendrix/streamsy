import { expect, it } from "bun:test";
import { Effect, Exit, Option } from "effect";
import { faultyStorage } from "./fault-injection.ts";
import { layer } from "../storage/memory/layer.ts";
import { Storage } from "../storage/storage.ts";
import { StreamId } from "../schema/index.ts";
import { ZERO_OFFSET } from "../offset/index.ts";
for (const when of ["before", "after"] as const) {
  it(`fault injection fails once ${when} the Nth mutation and preserves truthful state`, () =>
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
      const operation = {
        _tag: "Append" as const,
        streamId: id,
        messages: [],
        patch: { lifecycle: { closed: true } },
      };
      const failure = yield* Effect.result(storage.mutate({ operations: [operation] }));
      expect(failure).toMatchObject({
        _tag: "Failure",
        failure: { _tag: "StorageFault", retryable: true },
      });
      expect(Option.getOrThrow(yield* storage.record(id)).lifecycle.closed).toBe(when === "after");
      expect((yield* storage.mutate({ operations: [operation] }))._tag).toBe("Applied");
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
