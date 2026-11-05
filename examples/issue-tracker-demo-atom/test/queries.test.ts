import { it } from "@effect/vitest";
import { Effect, Option } from "effect";
import { afterEach, describe, expect } from "vitest";
import {
  commentsByIssueAtom,
  issueCountByProjectAtom,
  projectsAtom,
  replicaAtom,
  selectedProjectAtom,
  statusCountsAtom,
  visibleIssuesAtom,
} from "../src/queries.ts";
import { filterAtom, selectedProjectIdAtom } from "../src/state.ts";
import {
  comment,
  ids,
  issue,
  makeHarness,
  project,
  upsertComment,
  upsertIssue,
  upsertProject,
} from "./support.ts";

const second = { ...project, id: "proj_second", createdAt: "2026-11-06T00:00:00.000Z" };

const seed = [
  upsertProject(project),
  upsertProject(second),
  upsertIssue(issue("a")),
  upsertIssue(issue("b", { status: "in_progress" })),
  upsertIssue(issue("other", { projectId: second.id })),
  upsertComment(comment("c", "a", "2026-11-05T10:00:00.000Z")),
];

describe("derived atoms", () => {
  let harness = makeHarness();

  afterEach(() => {
    harness.registry.dispose();
    harness = makeHarness();
  });

  it.live("starts empty and not live, then follows the stream", () =>
    Effect.gen(function* () {
      const { registry, receive } = harness;

      expect(registry.get(replicaAtom).isLive).toBe(false);
      registry.mount(replicaAtom);
      yield* receive(...seed);

      expect(registry.get(replicaAtom).isLive).toBe(true);
      expect(ids(registry.get(projectsAtom))).toEqual([project.id, second.id]);
    }),
  );

  it.live("shows the first project until the user selects another", () =>
    Effect.gen(function* () {
      const { registry, receive } = harness;

      registry.mount(visibleIssuesAtom);
      yield* receive(...seed);

      expect(Option.map(registry.get(selectedProjectAtom), ({ id }) => id)).toEqual(
        Option.some(project.id),
      );
      expect(registry.get(statusCountsAtom)).toEqual({ all: 2, open: 1, in_progress: 1, done: 0 });

      registry.set(selectedProjectIdAtom, Option.some(second.id));

      expect(ids(registry.get(visibleIssuesAtom))).toEqual(["other"]);
      expect(registry.get(statusCountsAtom).all).toBe(1);
    }),
  );

  it.live("applies the filter to the list and leaves the counts alone", () =>
    Effect.gen(function* () {
      const { registry, receive } = harness;

      registry.mount(visibleIssuesAtom);
      registry.mount(statusCountsAtom);
      yield* receive(...seed);

      registry.set(filterAtom, "in_progress");

      expect(ids(registry.get(visibleIssuesAtom))).toEqual(["b"]);
      expect(registry.get(statusCountsAtom).all).toBe(2);
    }),
  );

  it.live("recomputes only the atoms that read the changed value", () =>
    Effect.gen(function* () {
      const { registry, receive } = harness;

      registry.mount(projectsAtom);
      registry.mount(visibleIssuesAtom);
      yield* receive(...seed);

      const projects = registry.get(projectsAtom);
      const list = registry.get(visibleIssuesAtom);

      registry.set(filterAtom, "open");

      expect(registry.get(projectsAtom)).toBe(projects);
      expect(registry.get(visibleIssuesAtom)).not.toBe(list);
    }),
  );

  it.live("derives the project counts and comment groups", () =>
    Effect.gen(function* () {
      const { registry, receive } = harness;

      registry.mount(issueCountByProjectAtom);
      registry.mount(commentsByIssueAtom);
      yield* receive(...seed);

      expect(registry.get(issueCountByProjectAtom).get(project.id)).toBe(2);
      expect(registry.get(commentsByIssueAtom).get("a")).toHaveLength(1);
    }),
  );

  it.live("keeps the failure of the read", () =>
    Effect.gen(function* () {
      const { registry, fail } = harness;

      registry.mount(replicaAtom);
      yield* fail("Unable to open the workspace stream");

      expect(registry.get(replicaAtom).maybeError).toEqual(
        Option.some("Unable to open the workspace stream"),
      );
    }),
  );
});
