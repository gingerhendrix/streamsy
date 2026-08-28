/* oxlint-disable effecttsgo/node-builtin-import -- The topology test reads the checked-in deployment program and package declaration. */
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { Effect, Schema } from "effect";
import { describe, expect, test } from "vitest";
import stack, {
  Gateway,
  STACK_NAME,
  WORKSPACE_OBJECT_CLASS,
  WORKSPACE_OBJECT_MIGRATION,
  WorkspacePartitions,
} from "../alchemy.run.ts";

const packageDir = join(dirname(fileURLToPath(import.meta.url)), "..");

const DeclaredPackage = Schema.Struct({
  devDependencies: Schema.Struct({ alchemy: Schema.String }),
});
type InspectedResource = typeof Gateway | typeof stack;

function stringField(root: InspectedResource, ...path: readonly string[]): string {
  let current = root;
  for (const [index, key] of path.entries()) {
    const value = Object.getOwnPropertyDescriptor(current, key)?.value;
    if (index === path.length - 1) return Schema.decodeUnknownSync(Schema.String)(value);
    if (!(value instanceof Object)) throw new TypeError(`${path.join(".")} is not readable`);
    current = value;
  }
  throw new TypeError("a field path must not be empty");
}

describe("Integration 3A Alchemy topology", () => {
  test("is an import-safe Alchemy v2 description", () => {
    const declared = Schema.decodeUnknownSync(DeclaredPackage)(
      JSON.parse(readFileSync(join(packageDir, "package.json"), "utf8")),
    );
    expect(declared.devDependencies.alchemy.startsWith("2.")).toBe(true);
    expect(Effect.isEffect(stack)).toBe(true);
    expect(stringField(stack, "stackName")).toBe(STACK_NAME);
    expect(Object.getOwnPropertyDescriptor(stack, "providers")?.value).toBeDefined();
    expect(Object.getOwnPropertyDescriptor(stack, "state")?.value).toBeDefined();
  });

  test("declares one SQLite workspace object and one gateway Worker", () => {
    expect(WorkspacePartitions).toEqual({
      kind: "Cloudflare.DurableObject",
      name: "WorkspacePartitions",
      className: WORKSPACE_OBJECT_CLASS,
    });
    expect(Effect.isEffect(Gateway)).toBe(true);
    expect(stringField(Gateway, "LogicalId")).toBe("Gateway");
    expect(stringField(Gateway, "Platform", "key")).toContain("Cloudflare.Worker");
    expect(WORKSPACE_OBJECT_MIGRATION).toBe("new_sqlite_classes:WorkspacePartitionObject");
  });

  test("routes dynamic surfaces through the Worker and serves built assets", () => {
    const source = readFileSync(join(packageDir, "alchemy.run.ts"), "utf8");
    expect(source).toContain('main: "./server/cloudflare.ts"');
    expect(source).toContain('directory: "./dist/assets"');
    for (const route of ["/api/*", "/streams/*", "/state/*", "/feed/*", "/document/*"]) {
      expect(source).toContain(`"${route}"`);
    }
  });

  test("binds only placement and stage identity, never test failpoints", () => {
    const source = readFileSync(join(packageDir, "alchemy.run.ts"), "utf8");
    expect(source).toContain("WORKSPACES: WorkspacePartitions");
    expect(source).toContain("DEPLOYMENT: Alchemy.Stage");
    expect(source).not.toContain("TEST_FAILPOINTS");
    expect(source).not.toContain("domain:");
    expect(source).not.toContain("routes:");
  });
});
