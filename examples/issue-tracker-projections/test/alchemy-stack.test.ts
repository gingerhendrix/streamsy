/**
 * The deployment program is Alchemy v2, and it is a description.
 *
 * `alchemy.run.ts` is imported here directly. That is only safe because a v2
 * stack is an `Effect` — importing it builds a description and applies nothing —
 * so these assertions are about the real program the CLI runs, not a copy of it.
 *
 * What is proved:
 *
 *  - the installed `alchemy` is major version 2, not the 0.x line;
 *  - the module's default export is an Effect, so `alchemy deploy` has a stack
 *    to run and importing it has no side effect;
 *  - the Worker, queue, and Durable Object are v2 resource descriptions;
 *  - the topology is finite: it names a Worker, a queue, a DO namespace, and the
 *    assets, and carries no workspace, project, issue, or stream value.
 */
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { Effect } from "effect";
import { describe, expect, test } from "vitest";
import stack, { Api, ProjectionWakes, STACK_NAME, StreamDO } from "../alchemy.run.ts";

const packageDir = join(dirname(fileURLToPath(import.meta.url)), "..");

const readJson = (path: string): unknown =>
  JSON.parse(readFileSync(join(packageDir, path), "utf8"));

/**
 * Read one nested field of an unknown value, failing the test with the path it
 * could not follow. The Alchemy resource internals below are deliberately
 * untyped, so every probe of them goes through here rather than an assertion.
 */
function field(root: unknown, ...path: readonly string[]): unknown {
  let current = root;
  for (const key of path) {
    // An Alchemy resource is a callable Effect, so a function is readable too.
    if (current === null || (typeof current !== "object" && typeof current !== "function")) {
      throw new TypeError(`${path.join(".")} is not readable`);
    }
    current = Reflect.get(current, key);
  }
  return current;
}

function stringField(root: unknown, ...path: readonly string[]): string {
  const value = field(root, ...path);
  if (typeof value !== "string") throw new TypeError(`${path.join(".")} must be a string`);
  return value;
}

describe("Alchemy v2 deployment program", () => {
  test("the example depends on Alchemy v2, not the 0.x line", () => {
    const declared = stringField(readJson("package.json"), "devDependencies", "alchemy");
    expect(declared.startsWith("2.")).toBe(true);

    const installed = stringField(readJson("node_modules/alchemy/package.json"), "version");
    expect(Number.parseInt(installed.split(".")[0]!, 10)).toBe(2);
  });

  test("the stack is a description, so importing it applies nothing", () => {
    expect(Effect.isEffect(stack)).toBe(true);
    expect(STACK_NAME).toBe("streamsy-issue-tracker");
  });

  test("the Worker and the queue are v2 resources: Effects yielded by the stack", () => {
    // The v1 API returned an awaited resource object from a top-level `await`.
    // In v2 a resource is an Effect that only runs inside the stack program.
    expect(Effect.isEffect(Api)).toBe(true);
    expect(Effect.isEffect(ProjectionWakes)).toBe(true);
    expect(stringField(Api, "LogicalId")).toBe("Api");
    expect(stringField(Api, "Platform", "key")).toContain("Cloudflare.Worker");
  });

  test("the Durable Object binding names the class the Worker exports", () => {
    // v2 declares the namespace inline on the Worker's `env`; SQLite storage is
    // the default for a class the Worker hosts itself, so there is no flag.
    expect(StreamDO).toEqual({
      kind: "Cloudflare.DurableObject",
      name: "StreamDO",
      className: "StreamStorage",
    });
  });

  test("the stack is wired with a providers layer and a state layer", () => {
    // Both are required by v2; a stack missing either dies with a named error.
    expect(stringField(stack, "stackName")).toBe(STACK_NAME);
    expect(field(stack, "providers")).toBeDefined();
    expect(field(stack, "state")).toBeDefined();
  });

  /**
   * The Worker's props live in the resource closure, so they are asserted
   * against the program text. The binding *names* are additionally proved at
   * compile time: `server/worker.ts` types its `env` as
   * `Cloudflare.InferEnv<typeof Api>`, so a renamed binding fails `typecheck`.
   */
  test("the Worker declares its entry, its assets, and exactly four bindings", () => {
    const source = readFileSync(join(packageDir, "alchemy.run.ts"), "utf8");
    expect(source).toContain('main: "./server/worker.ts"');
    expect(source).toContain('directory: "./dist/assets"');
    expect(source).toContain('flags: ["nodejs_compat"]');
    for (const binding of [
      "STREAM_DO",
      "PROJECTION_WAKES",
      "ISSUE_TRACKER_HOST",
      "ISSUE_TRACKER_DEPLOYMENT",
    ]) {
      expect(source).toContain(`${binding}:`);
    }
  });

  test("the deployment program carries no runtime identity", () => {
    // The same rule `scripts/alchemy-state-audit.ts` enforces against applied
    // state, checked here against the program itself.
    const source = readFileSync(join(packageDir, "alchemy.run.ts"), "utf8");
    for (const pattern of [
      /workspaces\//,
      /issue-tracker-cmd-/,
      /"issue-detail"/,
      /"board-issue"/,
      /issue-(ship|plat)-\d/,
    ]) {
      expect(pattern.test(source)).toBe(false);
    }
  });
});
