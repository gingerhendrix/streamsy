import { it } from "@effect/vitest";
import { Array, Effect, Option } from "effect";
import { afterEach, describe, expect, vi } from "vitest";
import { Intent } from "../src/domain/intent.ts";
import {
  pendingWritesAtom,
  projectsAtom,
  selectedProjectAtom,
  visibleIssuesAtom,
} from "../src/queries.ts";
import { displayNameAtom } from "../src/utils/display-name.ts";
import { noticesAtom } from "../src/utils/notices.ts";
import { writeAtom } from "../src/writes.ts";
import {
  byId,
  ids,
  issue,
  makeHarness,
  project,
  settle,
  upsertIssue,
  upsertProject,
} from "./support.ts";

const issueWritten = (id: string, txid: string) => ({
  awaitOffset: "0_0",
  txid,
  issue: issue(id),
});

const wipRefusal = { _tag: "WipLimitReached", error: "This project already has 3 in progress" };

/** A harness with one project and two issues on the stream. */
const liveHarness = Effect.gen(function* () {
  const harness = makeHarness();

  harness.registry.mount(visibleIssuesAtom);
  harness.registry.mount(pendingWritesAtom);
  harness.registry.mount(writeAtom);
  yield* harness.receive(upsertProject(project), upsertIssue(issue("a")), upsertIssue(issue("b")));

  return harness;
});

const advanceTimers = (millis: number) => Effect.promise(() => vi.advanceTimersByTimeAsync(millis));

const newIssue = <A extends { readonly title: string }>(rows: ReadonlyArray<A>) =>
  Option.getOrUndefined(Array.findFirst(rows, ({ title }) => title === "New"));

const moveToInProgress = (issueId: string) =>
  Intent.UpdateIssue({ issueId, patch: { status: "in_progress" } });

describe("an optimistic write", () => {
  afterEach(() => {
    vi.useRealTimers();
    localStorage.clear();
  });

  it.live("shows faded at once and stays faded after the HTTP answer", () =>
    Effect.gen(function* () {
      const { registry, requests, receive } = yield* liveHarness;

      registry.set(writeAtom, Intent.CreateIssue({ projectId: project.id, title: "New" }));
      yield* settle;

      const shown = newIssue(registry.get(visibleIssuesAtom));

      expect(shown?.$synced).toBe(false);

      const [request] = requests;
      const txid = request?.body.txid ?? "";

      request?.answer(201, issueWritten(request.body.id ?? "", txid));
      yield* settle;

      expect(registry.get(pendingWritesAtom)).toHaveLength(1);
      expect(newIssue(registry.get(visibleIssuesAtom))?.$synced).toBe(false);

      yield* receive(upsertIssue(issue(request?.body.id ?? "", { title: "New" }), txid));

      expect(registry.get(pendingWritesAtom)).toHaveLength(0);
      expect(newIssue(registry.get(visibleIssuesAtom))?.$synced).toBe(true);
    }),
  );

  it.live("confirms when the txid arrives before the HTTP answer", () =>
    Effect.gen(function* () {
      const { registry, requests, receive } = yield* liveHarness;

      registry.set(writeAtom, moveToInProgress("a"));
      yield* settle;

      const [request] = requests;
      const txid = request?.body.txid ?? "";

      yield* receive(upsertIssue(issue("a", { status: "in_progress" }), txid));

      expect(registry.get(pendingWritesAtom)).toHaveLength(0);

      request?.answer(200, issueWritten("a", txid));
      yield* settle;

      expect(registry.get(pendingWritesAtom)).toHaveLength(0);
      expect(registry.get(noticesAtom)).toHaveLength(0);
    }),
  );

  it.live("rolls a refusal back and shows the server's message for 4 seconds", () =>
    Effect.gen(function* () {
      const { registry, requests } = yield* liveHarness;

      registry.set(writeAtom, moveToInProgress("a"));
      yield* settle;

      expect(byId(registry.get(visibleIssuesAtom), "a")?.status).toBe("in_progress");

      vi.useFakeTimers();
      requests[0]?.answer(422, wipRefusal);
      yield* advanceTimers(10);

      expect(byId(registry.get(visibleIssuesAtom), "a")).toMatchObject({
        status: "open",
        $synced: true,
      });
      expect(registry.get(noticesAtom)).toMatchObject([{ message: wipRefusal.error }]);

      yield* advanceTimers(4000);

      expect(registry.get(noticesAtom)).toHaveLength(0);
    }),
  );

  it.live("rolls back with a notice when the connection drops", () =>
    Effect.gen(function* () {
      const { registry, requests } = yield* liveHarness;

      registry.set(writeAtom, moveToInProgress("a"));
      yield* settle;
      requests[0]?.dropConnection();
      yield* settle;

      expect(registry.get(pendingWritesAtom)).toHaveLength(0);
      expect(registry.get(noticesAtom).at(-1)?.message).toBe("The server did not answer");
    }),
  );

  it.live("keeps concurrent writes apart", () =>
    Effect.gen(function* () {
      const { registry, requests, receive } = yield* liveHarness;

      registry.set(writeAtom, moveToInProgress("a"));
      registry.set(writeAtom, moveToInProgress("b"));
      yield* settle;

      expect(requests).toHaveLength(2);
      expect(requests[0]?.wasAborted()).toBe(false);

      const [refused, accepted] = requests;

      refused?.answer(422, wipRefusal);
      yield* settle;

      const shown = registry.get(visibleIssuesAtom);

      // The refused change is gone while the other write is still open.
      expect(byId(shown, "a")).toMatchObject({ status: "open", $synced: true });
      expect(byId(shown, "b")).toMatchObject({
        status: "in_progress",
        $synced: false,
      });

      const txid = accepted?.body.txid ?? "";

      accepted?.answer(200, issueWritten("b", txid));
      yield* receive(upsertIssue(issue("b", { status: "in_progress" }), txid));

      expect(byId(registry.get(visibleIssuesAtom), "b")?.$synced).toBe(true);
    }),
  );

  it.live("shows rows from another tab while a write is open", () =>
    Effect.gen(function* () {
      const { registry, receive } = yield* liveHarness;

      registry.set(writeAtom, moveToInProgress("a"));
      yield* settle;
      yield* receive(upsertIssue(issue("from-another-tab")));

      expect(ids(registry.get(visibleIssuesAtom))).toContain("from-another-tab");
      expect(registry.get(pendingWritesAtom)).toHaveLength(1);
    }),
  );

  it.live("selects a new project at once and falls back when the server refuses it", () =>
    Effect.gen(function* () {
      const { registry, requests } = yield* liveHarness;

      registry.mount(selectedProjectAtom);
      registry.set(writeAtom, Intent.CreateProject({ name: "Zed", description: "" }));
      yield* settle;

      expect(Option.map(registry.get(selectedProjectAtom), ({ name }) => name)).toEqual(
        Option.some("Zed"),
      );

      requests[0]?.answer(409, { _tag: "Conflict", error: "Project already exists" });
      yield* settle;

      expect(registry.get(projectsAtom)).toHaveLength(1);
      expect(Option.map(registry.get(selectedProjectAtom), ({ id }) => id)).toEqual(
        Option.some(project.id),
      );
    }),
  );

  it.live("sends the writer's name, no times, and the txid", () =>
    Effect.gen(function* () {
      const { registry, requests } = yield* liveHarness;

      registry.set(displayNameAtom, "  Ada ");
      registry.set(writeAtom, Intent.CreateIssue({ projectId: project.id, title: "New" }));
      registry.set(writeAtom, moveToInProgress("a"));
      yield* settle;

      expect(requests[0]).toMatchObject({ method: "POST", path: "/api/w/main/issues" });
      expect(requests[0]?.body).toMatchObject({ title: "New", status: "open", updatedBy: "Ada" });
      expect(requests[0]?.body).not.toHaveProperty("createdAt");
      expect(requests[1]).toMatchObject({ method: "PATCH", path: "/api/w/main/issues/a" });
      expect(requests[1]?.body.txid).toBeTypeOf("string");
    }),
  );

  it.live("leaves updatedBy out for a writer with no name", () =>
    Effect.gen(function* () {
      const { registry, requests } = yield* liveHarness;

      registry.set(writeAtom, moveToInProgress("a"));
      yield* settle;

      expect(requests[0]?.body).not.toHaveProperty("updatedBy");
    }),
  );
});
