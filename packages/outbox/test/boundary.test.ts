/* oxlint-disable effecttsgo/node-builtin-import -- This architectural check inspects the package's actual import statements. */
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { expect, test } from "vitest";

test("production sources have no serve, projection, storage, or Cloudflare dependency", () => {
  const directory = join(import.meta.dirname, "../src");

  for (const file of readdirSync(directory)) {
    const source = readFileSync(join(directory, file), "utf8");
    expect(source).not.toMatch(
      /(?:from\s*|import\s*\(|require\s*\()\s*["'](?:@streamsy\/(?:serve|projection|storage)|[^"']*[Cc]loudflare)/,
    );
  }
});
