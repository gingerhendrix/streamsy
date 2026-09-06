/**
 * The failure surfaces this batch touched.
 *
 * Each application site below now represents its typed error by yielding the
 * error value itself instead of wrapping it in `Effect.fail`, so these tests
 * pin the observable contract that change must not move: the failure reaches
 * the caller through the typed error channel, carries its `_tag`, and carries
 * its payload. They also pin the append boundary the batch re-expressed through
 * Schema, and the browser client's failure contract, which this batch
 * deliberately leaves as Promise-native code with no Effect import.
 *
 * The bodies return promises rather than declaring `async` functions, which
 * keeps this file from adding to the example's outstanding async-function
 * inventory.
 */
import {
  createMemoryStorageAdapter,
  directProtocolClient,
  StreamProtocol,
  type StreamProtocolClient,
} from "@streamsy/core";
import { Effect, Layer, ManagedRuntime, Schema } from "effect";
import { afterEach, describe, expect, test } from "vitest";
import {
  issueCommand,
  probeCoverage,
  requireDetail,
  type ApplicationServices,
} from "../server/application.ts";
import { CommandProducers } from "../server/commands.ts";
import * as AppConfigModule from "../server/config.ts";
import { applicationLayer } from "../server/runtime.ts";
import { Wake } from "../server/wake.ts";
import { IssueEvent, ProjectMembershipFact } from "../shared/domain.ts";
import { ApiFailure } from "../src/lib/api.ts";

const disposals: Array<() => Promise<void>> = [];

afterEach(() => Promise.all(disposals.splice(0).map((dispose) => dispose())));

/** Derive one command lane, which is where an unusable command id is refused. */
const lane = (commandId: string) =>
  Effect.gen(function* () {
    const producers = yield* CommandProducers;
    return yield* producers.forCommand(commandId);
  });

/** A runtime assembled from explicit layers, exactly as a host assembles one. */
function testRuntime(): ManagedRuntime.ManagedRuntime<ApplicationServices, never> {
  const client: StreamProtocolClient = directProtocolClient(
    new StreamProtocol({ storage: { adapter: createMemoryStorageAdapter() } }),
  );
  const runtime: ManagedRuntime.ManagedRuntime<ApplicationServices, never> = ManagedRuntime.make(
    applicationLayer({
      client,
      config: AppConfigModule.layer({ host: "local", deployment: "test" }),
      wake: Layer.succeed(Wake, Wake.of({ wake: () => Effect.void })),
    }),
  );
  disposals.push(() => runtime.dispose().then(() => client.close()));
  return runtime;
}

describe("application failures stay in the typed error channel", () => {
  test("requireDetail fails with UnknownIssue for an issue that was never created", () =>
    testRuntime()
      .runPromise(Effect.flip(requireDetail("w1", "absent")))
      .then((failure) => {
        const { _tag: tag } = failure;
        expect(tag).toBe("UnknownIssue");
        if (tag !== "UnknownIssue") throw new Error("the failure must be UnknownIssue");
        expect(failure.issueId).toBe("absent");
      }));

  test("issueCommand fails with UnknownIssue rather than throwing a defect", () =>
    testRuntime()
      .runPromise(
        Effect.flip(
          issueCommand("w1", "absent", { commandId: "cmd-1", type: "rename", title: "Nope" }),
        ),
      )
      .then((failure) => {
        const { _tag: tag } = failure;
        expect(tag).toBe("UnknownIssue");
      }));

  test("probeCoverage rejects an empty position with InvalidRequest", () =>
    testRuntime()
      .runPromise(Effect.flip(probeCoverage("w1", "issue-1", "")))
      .then((failure) => {
        const { _tag: tag } = failure;
        expect(tag).toBe("InvalidRequest");
        if (tag !== "InvalidRequest") throw new Error("the failure must be InvalidRequest");
        expect(failure.field).toBe("position");
        expect(failure.detail).toBe("is required");
      }));

  test("a command lane is refused with InvalidRequest outside the accepted id length", () => {
    const runtime = testRuntime();

    return runtime
      .runPromise(Effect.all(["", "c".repeat(129)].map((id) => Effect.flip(lane(id)))))
      .then((failures) => {
        for (const failure of failures) {
          const { _tag: tag } = failure;
          expect(tag).toBe("InvalidRequest");
          expect(failure.field).toBe("commandId");
          expect(failure.detail).toBe("must be 1 to 128 characters long");
        }
        return runtime.runPromise(lane("c".repeat(128)));
      })
      .then((accepted) => {
        expect(accepted.producerId.startsWith("issue-tracker-cmd-")).toBe(true);
      });
  });
});

describe("append payloads are produced by the Schema JSON codec", () => {
  const IssueEventJson = Schema.fromJsonString(IssueEvent);
  const encodeEvent = Schema.encodeUnknownSync(IssueEventJson);
  const decodeEvent = Schema.decodeSync(IssueEventJson);
  const encodeFact = Schema.encodeUnknownSync(Schema.fromJsonString(ProjectMembershipFact));

  test("an event encodes to its wire JSON and decodes back unchanged", () => {
    const event: IssueEvent = {
      commandId: "cmd-1",
      at: "2026-08-22T00:00:00.000Z",
      type: "IssueCreated",
      issueId: "issue-1",
      issueKey: "SHIP-1",
      projectId: "launch",
      title: "Ship it",
      status: "backlog",
      priority: "medium",
      creatorId: "user-1",
    };
    const payload = encodeEvent(event);
    expect(payload).toBe(JSON.stringify(event));
    expect(decodeEvent(payload)).toEqual(event);
  });

  test("a membership fact encodes to its wire JSON", () => {
    const fact: ProjectMembershipFact = { type: "IssueJoined", issueId: "issue-1", from: null };
    expect(encodeFact(fact)).toBe(JSON.stringify(fact));
  });

  test("a value outside the domain is refused instead of silently serialised", () => {
    expect(() => encodeEvent({ type: "NotAnEvent" })).toThrow();
  });
});

describe("the browser client reports failures as a native error", () => {
  test("ApiFailure carries its name, message, status, and class identity", () => {
    const failure = new ApiFailure("not found: issue-1", 404);
    expect(failure.name).toBe("ApiFailure");
    expect(failure.message).toBe("not found: issue-1");
    expect(failure.status).toBe(404);
    expect(failure instanceof ApiFailure).toBe(true);
    expect(failure instanceof Error).toBe(true);
    expect(String(failure)).toBe("ApiFailure: not found: issue-1");
  });
});
