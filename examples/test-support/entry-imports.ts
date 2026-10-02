/* oxlint-disable effecttsgo/node-builtin-import -- The walker reads built files synchronously so a test can call it outside Effect. */
import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";

const transpiler = new Bun.Transpiler({ loader: "js" });

const isRelative = (specifier: string) => specifier.startsWith("./") || specifier.startsWith("../");

/**
 * The bare modules that a built entry file can load. The walk starts at
 * `entry`, follows each relative import into its chunk, and collects every
 * other specifier as it is written (`effect/unstable/sql`, `node:sqlite`).
 * It reads static imports, `export ... from` and `import()` with a literal
 * path. It does not follow bare modules into `node_modules`.
 */
export const entryImports = (entry: string): ReadonlySet<string> => {
  const bare = new Set<string>();
  const seen = new Set<string>();
  const pending = [resolve(entry)];
  for (let file = pending.pop(); file !== undefined; file = pending.pop()) {
    if (seen.has(file)) continue;
    seen.add(file);
    for (const { path } of transpiler.scanImports(readFileSync(file, "utf8"))) {
      if (isRelative(path)) pending.push(resolve(dirname(file), path));
      else bare.add(path);
    }
  }
  return bare;
};

/** The modules in `imports` whose names start with one of `prefixes`. */
export const matching = (imports: ReadonlySet<string>, prefixes: ReadonlyArray<string>) =>
  [...imports].filter((name) => prefixes.some((prefix) => name.startsWith(prefix))).toSorted();
