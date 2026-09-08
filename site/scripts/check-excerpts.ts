import { spawn } from "node:child_process";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { setTimeout as delay } from "node:timers/promises";

const root = new URL("../../", import.meta.url);

export interface ExcerptPair {
  readonly doc: string;
  readonly source: string;
  readonly execute: boolean;
}

export const pairs: ReadonlyArray<ExcerptPair> = [
  { doc: "packages/core/README.md", source: "packages/core/test/readme.ts", execute: true },
  {
    doc: "site/content/docs/user/basic-usage.mdx",
    source: "packages/serve/test/basic-usage.ts",
    execute: true,
  },
  {
    doc: "site/content/docs/user/basic-usage.mdx",
    source: "packages/serve/test/host.ts",
    execute: true,
  },
  {
    doc: "site/content/docs/user/sql-storage.mdx",
    source: "packages/storage/test/shared-transaction-usage.ts",
    execute: true,
  },
  {
    doc: "site/content/docs/user/cloudflare-hosting.mdx",
    source: "packages/serve/test/cloudflare/example-worker.ts",
    execute: false,
  },
  {
    doc: "site/content/docs/user/cloudflare-hosting.mdx",
    source: "packages/serve/test/cloudflare-usage.ts",
    execute: true,
  },
  {
    doc: "site/content/docs/user/cloudflare-hosting.mdx",
    source: "hosted/alchemy.run.ts",
    execute: false,
  },
];

const DEFAULT_TIMEOUT_MS = 15_000;
const DEFAULT_GRACE_MS = 500;
const DEFAULT_REAP_TIMEOUT_MS = 2_000;

export interface ProcessRunOptions {
  readonly timeoutMs?: number;
  readonly graceMs?: number;
  readonly reapTimeoutMs?: number;
  readonly environment?: Readonly<Record<string, string>>;
  readonly onRoot?: (path: string) => void;
}

interface ChildExit {
  readonly code: number | null;
  readonly signal: NodeJS.Signals | null;
  readonly error?: Error;
}

const isNoSuchProcess = (error: Error): boolean => "code" in error && error.code === "ESRCH";

const signalGroup = (pid: number | undefined, signal: NodeJS.Signals): void => {
  if (pid === undefined) return;
  try {
    process.kill(-pid, signal);
  } catch (error) {
    if (!(error instanceof Error) || !isNoSuchProcess(error)) throw error;
  }
};

const groupExists = (pid: number): boolean => {
  try {
    process.kill(-pid, 0);
    return true;
  } catch (error) {
    if (error instanceof Error && isNoSuchProcess(error)) return false;
    throw error;
  }
};

const waitForGroupGone = async (pid: number, timeoutMs: number): Promise<boolean> => {
  const deadline = Date.now() + timeoutMs;
  while (groupExists(pid)) {
    if (Date.now() >= deadline) return false;
    await delay(10);
  }
  return true;
};

const combine = (primary: Error | undefined, cleanup: ReadonlyArray<Error>): never => {
  const errors = primary === undefined ? [...cleanup] : [primary, ...cleanup];
  if (errors.length === 1) throw errors[0];
  throw new AggregateError(errors, "Excerpt process failed and cleanup was incomplete", {
    cause: primary ?? errors[0],
  });
};

const runOwnedProcess = async (
  args: ReadonlyArray<string>,
  label: string,
  options: ProcessRunOptions = {},
): Promise<void> => {
  const ownedRoot = await mkdtemp(join(tmpdir(), ".streamsy-excerpt-"));
  const timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  const graceMs = options.graceMs ?? DEFAULT_GRACE_MS;
  const reapTimeoutMs = options.reapTimeoutMs ?? DEFAULT_REAP_TIMEOUT_MS;
  let child: ReturnType<typeof spawn> | undefined;
  let exit: ChildExit | undefined;
  let primaryError: Error | undefined;
  const cleanupErrors: Error[] = [];
  const startedAt = Date.now();
  const deadlineAt = startedAt + timeoutMs;
  let hardKillTimer: ReturnType<typeof setTimeout> | undefined;
  let hardKillError: Error | undefined;
  let hardKillScheduled = false;

  try {
    options.onRoot?.(ownedRoot);
    child = spawn(process.execPath, [...args], {
      cwd: fileURLToPath(root),
      detached: true,
      env: {
        ...process.env,
        TMPDIR: ownedRoot,
        STREAMSY_EXCERPT_ROOT: ownedRoot,
        ...options.environment,
      },
      stdio: ["ignore", "inherit", "inherit"],
    });
    hardKillTimer = setTimeout(
      () => {
        hardKillScheduled = true;
        try {
          signalGroup(child?.pid, "SIGKILL");
        } catch (error) {
          hardKillError = error instanceof Error ? error : new Error(String(error));
        }
      },
      Math.max(0, deadlineAt - Date.now()),
    );
    const closed = new Promise<ChildExit>((resolve) => {
      let settled = false;
      const finish = (value: ChildExit): void => {
        if (settled) return;
        settled = true;
        resolve(value);
      };
      child?.once("error", (error) => finish({ code: null, signal: null, error }));
      child?.once("close", (code, signal) => finish({ code, signal }));
    });
    const beforeDeadline = await Promise.race([
      closed,
      delay(Math.max(0, timeoutMs - graceMs)).then(() => undefined),
    ]);
    if (beforeDeadline === undefined) {
      primaryError = new Error(`${label} exceeded ${timeoutMs} ms`);
      signalGroup(child.pid, "SIGTERM");
      exit = await Promise.race([closed, delay(graceMs).then(() => undefined)]);
      if (exit === undefined) {
        signalGroup(child.pid, "SIGKILL");
        exit = await Promise.race([closed, delay(reapTimeoutMs).then(() => undefined)]);
      }
    } else {
      exit = beforeDeadline;
    }
    if (exit === undefined) {
      cleanupErrors.push(new Error(`${label} child did not close after termination`));
    } else if (exit.error !== undefined) {
      primaryError ??= exit.error;
    } else if (primaryError === undefined && (exit.code !== 0 || exit.signal !== null)) {
      primaryError = new Error(
        `${label} failed (status ${exit.code}, signal ${exit.signal ?? "none"})`,
      );
    }
  } catch (error) {
    primaryError ??= error instanceof Error ? error : new Error(String(error));
  } finally {
    if (child?.pid !== undefined) {
      let gone = false;
      try {
        gone = await waitForGroupGone(child.pid, Math.max(0, deadlineAt - Date.now()));
      } catch (error) {
        cleanupErrors.push(error instanceof Error ? error : new Error(String(error)));
      }
      if (!gone) {
        try {
          if (!hardKillScheduled) {
            hardKillScheduled = true;
            signalGroup(child.pid, "SIGKILL");
          }
          gone = await waitForGroupGone(child.pid, reapTimeoutMs);
        } catch (error) {
          cleanupErrors.push(error instanceof Error ? error : new Error(String(error)));
        }
      }
      if (!gone) {
        try {
          signalGroup(child.pid, "SIGKILL");
          gone = await waitForGroupGone(child.pid, reapTimeoutMs);
        } catch (error) {
          cleanupErrors.push(error instanceof Error ? error : new Error(String(error)));
        }
      }
      if (!gone) {
        cleanupErrors.push(
          new Error(`${label} process group remains; retained root: ${ownedRoot}`),
        );
      }
      if (hardKillError !== undefined) cleanupErrors.push(hardKillError);
      if (gone) {
        if (hardKillTimer !== undefined) clearTimeout(hardKillTimer);
        try {
          await rm(ownedRoot, { recursive: true, force: true });
        } catch (error) {
          cleanupErrors.push(
            error instanceof Error ? error : new Error(String(error)),
            new Error(`${label} root retained: ${ownedRoot}`),
          );
        }
      }
    } else {
      if (hardKillTimer !== undefined) clearTimeout(hardKillTimer);
      try {
        await rm(ownedRoot, { recursive: true, force: true });
      } catch (error) {
        cleanupErrors.push(
          error instanceof Error ? error : new Error(String(error)),
          new Error(`${label} root retained: ${ownedRoot}`),
        );
      }
    }
  }

  if (primaryError !== undefined || cleanupErrors.length > 0) combine(primaryError, cleanupErrors);
};

export const runExcerpt = (
  source: string,
  label: string,
  options?: ProcessRunOptions,
): Promise<void> => runOwnedProcess([source], label, options);

const repositorySourcePrefix =
  "https://github.com/gingerhendrix/streamsy/blob/effect-first-live-perimeter/";

const renderedLines = (text: string): ReadonlyArray<string> => {
  const lines = text.split("\n");
  let fence: { readonly marker: "`" | "~"; readonly length: number } | undefined;
  let htmlPre = false;
  let inComment = false;
  const visible: string[] = [];
  for (const line of lines) {
    let candidate = line;
    if (inComment) {
      const end = candidate.indexOf("-->");
      if (end < 0) continue;
      candidate = candidate.slice(end + 3);
      inComment = false;
    }
    const commentStart = candidate.indexOf("<!--");
    if (commentStart >= 0) {
      const commentEnd = candidate.indexOf("-->", commentStart + 4);
      if (commentEnd < 0) {
        inComment = true;
        candidate = candidate.slice(0, commentStart);
      } else {
        candidate = `${candidate.slice(0, commentStart)}${candidate.slice(commentEnd + 3)}`;
      }
    }
    if (htmlPre) {
      if (/<\/pre\s*>/i.test(candidate)) htmlPre = false;
      continue;
    }
    if (/<pre(?:\s|>)/i.test(candidate)) {
      htmlPre = !/<\/pre\s*>/i.test(candidate);
      continue;
    }
    const fenceMatch = candidate.match(/^(?: {0,3}>[ \t]?)*(?: {0,3})([`~]{3,})(.*)$/);
    const fenceMarker = fenceMatch?.[1]?.[0];
    const fenceLength = fenceMatch?.[1]?.length;
    const fenceRemainder = fenceMatch?.[2] ?? "";
    if (fence !== undefined) {
      if (
        fenceMarker === fence.marker &&
        fenceLength !== undefined &&
        fenceLength >= fence.length &&
        /^\s*$/.test(fenceRemainder)
      ) {
        fence = undefined;
      }
      continue;
    }
    if (fenceMarker !== undefined && fenceLength !== undefined) {
      fence = { marker: fenceMarker, length: fenceLength };
      continue;
    }
    if (/^(?: {4}|\t)/.test(candidate)) continue;
    visible.push(candidate.replace(/(`+)([\s\S]*?)\1/g, ""));
  }
  return visible;
};

const hasRenderedCitation = (text: string, source: string): boolean => {
  for (const line of renderedLines(text)) {
    const match = line.match(/^Compiled source: \[([^\]\r\n]+)\]\(([^)\s]+)\)\.(?:\s|$)/);
    if (match === null || match[1] !== source) continue;
    const destination = match[2];
    if (!destination.startsWith(repositorySourcePrefix)) continue;
    const remainder = destination.slice(repositorySourcePrefix.length);
    const suffix = remainder.slice(source.length);
    if (remainder.startsWith(source) && (suffix === "" || /^#L\d+(?:-L\d+)?$/.test(suffix))) {
      return true;
    }
  }
  return false;
};

export const assertExcerpt = (pair: ExcerptPair, text: string, code: string): void => {
  if (!hasRenderedCitation(text, pair.source))
    throw new Error(`Excerpt citation missing: ${pair.doc} must link ${pair.source}`);
  if (!text.includes(`\x60\x60\x60ts\n${code}\n\x60\x60\x60`)) {
    throw new Error(`Excerpt drift: ${pair.doc} must include ${pair.source} verbatim`);
  }
};

export const checkExcerpts = async (): Promise<void> => {
  await runOwnedProcess(["run", "--cwd", "packages/serve", "typecheck"], "serve Worker typecheck");
  await runOwnedProcess(["run", "--cwd", "hosted", "typecheck"], "hosted typecheck");

  for (const pair of pairs) {
    const text = await readFile(new URL(pair.doc, root), "utf8");
    const code = (await readFile(new URL(pair.source, root), "utf8")).trim();
    assertExcerpt(pair, text, code);
    console.log(`ok excerpt ${pair.doc} ← ${pair.source}`);
    if (!pair.execute) {
      console.log(`ok typechecked-only ${pair.source}`);
      continue;
    }
    await runExcerpt(pair.source, `Excerpt execution: ${pair.source}`);
    console.log(`ok executed ${pair.source} (15s bound)`);
  }
};

if (import.meta.main) await checkExcerpts();
