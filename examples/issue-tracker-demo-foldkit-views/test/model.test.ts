import { Schema } from "effect";
import { describe, expect, test } from "vitest";
import { Model } from "../src/model.ts";
import { init, update } from "../src/update.ts";
import { issue, project, receivedViews, upsertIssue, upsertProject } from "./support.ts";

// Foldkit's Vite plugin keeps the Model across a reload as JSON. A Model that
// does not survive this codec starts again from `init`.
const ModelJson = Schema.toCodecJson(Model);

describe("Model", () => {
  test("a live Model survives the JSON codec that keeps it across a reload", () => {
    const live = update(
      { ...init({ workspaceId: "main", displayName: "Ada" }).model, issueTitle: "draft" },
      receivedViews([upsertProject(project), upsertIssue(issue("issue_a"))]),
    ).model;

    const json = JSON.parse(JSON.stringify(Schema.encodeUnknownSync(ModelJson)(live)));

    expect(Schema.decodeUnknownSync(ModelJson)(json)).toEqual(live);
  });
});
