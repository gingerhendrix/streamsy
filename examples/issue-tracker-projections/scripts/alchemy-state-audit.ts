/* oxlint-disable effecttsgo/global-console, effecttsgo/node-builtin-import -- This Bun audit executable walks the on-disk Alchemy state tree with the Node-compatible filesystem and path APIs and reports its findings and exit status directly to the invoking terminal. */
/**
 * Alchemy-state disposable-identifier audit.
 *
 * Deployment state must describe a finite topology only: one Worker, one
 * Durable Object namespace, one queue, and the assets. It must never carry a
 * runtime identity — a workspace, project, issue, stream name, cursor, producer
 * lane, or durable position. Those live in Streamsy storage by construction,
 * and an appearance in `.alchemy/` would mean the deployment had started owning
 * data it must not own.
 *
 * The audit is honest about what it can see: `.alchemy/` only exists after a
 * real `alchemy deploy`, so with no Cloudflare credentials there is nothing
 * local to inspect. It says so explicitly rather than reporting a pass it did
 * not earn.
 *
 * The stack uses Alchemy v2's `Alchemy.localState()`, which writes its state
 * tree to `.alchemy/state` under the process working directory — so both the
 * example directory and the repository root are scanned.
 */
import { readdirSync, readFileSync, statSync } from "node:fs";
import { dirname, join, relative } from "node:path";
import { fileURLToPath } from "node:url";

const packageDir = join(dirname(fileURLToPath(import.meta.url)), "..");
const repoRoot = join(packageDir, "..", "..");

/** Every shape a runtime identity could take in this demo. */
const FORBIDDEN: readonly { readonly label: string; readonly pattern: RegExp }[] = [
  { label: "workspace stream path", pattern: /workspaces\/[A-Za-z0-9_-]+/ },
  { label: "durable stream position", pattern: /\b\d{16}_\d{16}\b/ },
  { label: "command producer lane", pattern: /issue-tracker-cmd-[0-9a-f]{8}/ },
  { label: "mesh lineage row", pattern: /__streamsy\.mesh\./ },
  { label: "issue-detail collection row", pattern: /"issue-detail"/ },
  { label: "board-row collection row", pattern: /"board-issue"/ },
  { label: "seeded issue id", pattern: /issue-(ship|plat)-\d/ },
];

/**
 * Only the applied-state tree counts. `alchemy plan` also writes a version
 * check and a log under `.alchemy/`; scanning those would let the audit report
 * a pass it has not earned.
 */
const stateDirs = [
  join(packageDir, ".alchemy", "state"),
  join(repoRoot, ".alchemy", "state"),
].filter(exists);

if (stateDirs.length === 0) {
  console.log(
    [
      "alchemy-state audit skipped — no local Alchemy state to inspect.",
      "",
      "`.alchemy/state` is written by `alchemy deploy`. This environment has no",
      "Cloudflare credentials, so no stage has ever been applied and there is no",
      "state file on disk. The claim that deployment state holds no runtime",
      "identity therefore rests on the topology in `alchemy.run.ts` (which names",
      "only a Worker, a Durable Object namespace, a queue, and the assets) and",
      "remains unverified against applied state. `test/alchemy-stack.test.ts`",
      "checks the same patterns against the program itself.",
      "",
      "Run `STAGE=... bun run deploy:demo` and then this script to verify it.",
    ].join("\n"),
  );
  process.exit(0);
}

const findings: string[] = [];
let scanned = 0;

for (const dir of stateDirs) {
  for (const file of walk(dir)) {
    scanned++;
    const text = readFileSync(file, "utf8");
    for (const { label, pattern } of FORBIDDEN) {
      const match = pattern.exec(text);
      if (match !== null) {
        findings.push(`${relative(repoRoot, file)}: ${label} — ${match[0]}`);
      }
    }
  }
}

console.log(`alchemy-state audit scanned ${scanned} file(s) in ${stateDirs.length} state dir(s)`);
if (findings.length > 0) {
  console.error(`\nruntime identities found in deployment state:\n${findings.join("\n")}`);
  process.exit(1);
}
console.log("no workspace, project, issue, stream, cursor, or position value in Alchemy state");

function exists(path: string): boolean {
  try {
    return statSync(path).isDirectory();
  } catch {
    return false;
  }
}

function* walk(dir: string): Generator<string> {
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) {
      yield* walk(path);
    } else if (entry.isFile()) {
      yield path;
    }
  }
}
