// oxlint-disable-next-line effecttsgo/node-builtin-import -- Vitest global setup owns synchronous build subprocesses before the test runtime starts.
import { execFileSync } from "node:child_process";

/** Standalone test:unit runs need the same local assets as the HTTP and Worker smokes. */
export default function setup() {
  for (const script of ["build:web", "build:worker"]) {
    execFileSync(process.execPath, ["run", script], {
      cwd: `${import.meta.dirname}/..`,
      stdio: "inherit",
    });
  }
}
