import { expect, test } from "bun:test";
import { generateStateSinkModule } from "./codegen.ts";

test("state-sink generation is deterministic and derives from one export", () => {
  const input = {
    modulePath: "../../domain/declaration.ts",
    exportName: "boardIssues",
    sink: {
      kind: "checked-state-sink" as const,
      collection: { name: "issues", type: "issue", primaryKey: "issueId" },
    },
  };
  const left = generateStateSinkModule(input);
  const right = generateStateSinkModule(input);
  expect(left).toBe(right);
  expect(left).toContain("RowOf<typeof boardIssues>");
  expect(left).toContain("boardIssues.collection.primaryKey");
  expect(left).not.toContain('route: "/state/');
});
