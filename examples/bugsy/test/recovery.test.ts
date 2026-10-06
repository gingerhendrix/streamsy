import { BunServices } from "@effect/platform-bun";
import { layer as testLayer } from "@streamsy/effect-test";
import { FetchHttpClient } from "effect/http";
import { Layer } from "effect";

import * as Addresses from "./support/addresses.ts";
// Probe D: all three stream outputs nonempty; the unit stops after the second
// append (labelCounts landed, transitions not). Recovery after new input must
// give one copy of each item and match a clean run with the same unit boundaries.
import { expect } from "bun:test";
import { DurableState, TransportFault, ZERO_OFFSET } from "@streamsy/core";
import { Projection } from "@streamsy/projection";
import { Effect } from "effect";
import { applicationLayer, createInputs, sqliteStorage } from "../server/host.ts";
import { tracker, Board, LabelCounts, Transitions } from "../server/outputs.ts";
import { Events, LabelEvents, Labels } from "../server/streams.ts";

testLayer(Layer.mergeAll(BunServices.layer, FetchHttpClient.layer), { excludeTestServices: true })(
  (it) => {
    const at = "2026-09-23T10:00:00Z";

    const seed = Effect.gen(function* () {
      yield* createInputs("live");
      yield* (yield* Labels).append(
        { workspaceId: "live" },
        DurableState.changes(Labels.at({ workspaceId: "live" }), { offset: ZERO_OFFSET }, [
          DurableState.upsert("label", {
            workspaceId: "live",
            labelId: "bug",
            name: "Bug",
            color: "#ff0000",
            updatedAt: at,
          }),
        ]),
      );
      yield* (yield* Events).append({ workspaceId: "live" }, [
        {
          type: "IssueCreated",
          eventId: "e1",
          workspaceId: "live",
          issueId: "i1",
          projectId: "p1",
          title: "One",
          status: "todo",
          sequence: 0,
          occurredAt: at,
        },
      ]);
      yield* (yield* LabelEvents).append({ workspaceId: "live" }, [
        {
          type: "LabelAttached",
          eventId: "a1",
          workspaceId: "live",
          issueId: "i1",
          labelId: "bug",
          membershipId: "i1.bug",
          sequence: 0,
          occurredAt: at,
        },
      ]);
    });

    const more = Effect.flatMap(Events, (events) =>
      events.append({ workspaceId: "live" }, [
        {
          type: "IssueStatusChanged",
          eventId: "e2",
          workspaceId: "live",
          issueId: "i1",
          status: "done",
          sequence: 1,
          occurredAt: at,
        },
      ]),
    );

    const collect = Effect.gen(function* () {
      return {
        board: yield* Addresses.readOutput(Board, "live"),
        counts: yield* Addresses.readOutput(LabelCounts, "live"),
        transitions: yield* Addresses.readOutput(Transitions, "live"),
        summary: yield* Projection.read(tracker.at({ workspaceId: "live" })).pipe(
          Effect.map(({ state }) => state),
        ),
      };
    });

    const run = (failOn: number | undefined) =>
      Effect.gen(function* () {
        yield* seed;
        const bound = tracker.at({ workspaceId: "live" });

        if (failOn === undefined) yield* Projection.run(bound);
        else {
          let calls = 0;

          const r = yield* Projection.run(bound).pipe(
            Addresses.patchOutputAppends(
              (append) => (id, o) =>
                ++calls === failOn
                  ? Effect.fail(
                      new TransportFault({
                        reason: "response",
                        operation: "append",
                        message: "stop",
                      }),
                    )
                  : append(id, o),
            ),
            Effect.result,
          );

          expect(r._tag).toBe("Failure");
        }

        yield* more;
        yield* Projection.run(bound);

        return yield* collect;
      });

    it.effect("stop between outputs recovers to the same outputs as a clean run", () =>
      Effect.gen(function* () {
        const results = [];

        for (const failOn of [undefined, 2, 3]) {
          const rt = yield* Layer.build(applicationLayer(sqliteStorage(":memory:")));

          results.push(yield* run(failOn).pipe(Effect.provideContext(rt)));
        }

        expect(results[1]).toEqual(results[0]);
        expect(results[2]).toEqual(results[0]);
      }),
    );
  },
);
