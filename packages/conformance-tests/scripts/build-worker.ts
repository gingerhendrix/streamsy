import {
  cpSync,
  existsSync,
  lstatSync,
  mkdirSync,
  mkdtempSync,
  readdirSync,
  rmSync,
} from "node:fs";
import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";
import { dirname, isAbsolute, join, relative, resolve } from "node:path";

const scriptDirectory = dirname(fileURLToPath(import.meta.url));
const packageRoot = resolve(scriptDirectory, "..");
const repositoryRoot = resolve(packageRoot, "../..");
const workerSource = join(packageRoot, "src", "worker.ts");
const outputRoot = join(packageRoot, "dist");
const uploadDirectory = join(outputRoot, "worker");
const workerPath = join(uploadDirectory, "worker.js");
const reportPath = join(outputRoot, "bundle-report.json");

const failPrerequisite = (message: string): never => {
  throw new Error(`${message}. Run bun run build first.`);
};

const lstatIfExists = (path: string): ReturnType<typeof lstatSync> | undefined => {
  try {
    return lstatSync(path);
  } catch (error) {
    if (error instanceof Error && "code" in error && error.code === "ENOENT") return undefined;
    throw error;
  }
};

const assertBuiltExports = (): void => {
  for (const path of [
    join(repositoryRoot, "packages", "core", "dist", "index.js"),
    join(repositoryRoot, "packages", "storage", "dist", "durable-object.js"),
    join(repositoryRoot, "packages", "serve", "dist", "cloudflare.js"),
  ]) {
    if (!existsSync(path)) failPrerequisite(`Missing built public export ${path}`);
  }
};

const assertOwnedDirectory = (path: string, label: string): void => {
  const stats = lstatIfExists(path);
  if (stats === undefined) return;
  if (!stats.isDirectory() || stats.isSymbolicLink()) {
    throw new Error(`${label} must be an owned regular directory: ${path}`);
  }
};

const assertOwnedAncestors = (path: string, label: string): void => {
  const absolute = resolve(path);
  const repository = resolve(repositoryRoot);
  const outside = relative(repository, absolute);
  if (outside.startsWith("..") || isAbsolute(outside)) {
    throw new Error(`${label} escaped the repository output boundary: ${absolute}`);
  }
  let current = absolute;
  for (;;) {
    const stats = lstatIfExists(current);
    if (stats !== undefined && (!stats.isDirectory() || stats.isSymbolicLink())) {
      throw new Error(`${label} has an unowned ancestor: ${current}`);
    }
    const parent = dirname(current);
    if (parent === current) break;
    current = parent;
  }
};

const invalidateReport = (): void => {
  const stats = lstatIfExists(reportPath);
  if (stats === undefined) return;
  if (!stats.isFile() || stats.isSymbolicLink()) {
    throw new Error(`Refusing to remove unexpected bundle report ${reportPath}`);
  }
  rmSync(reportPath);
};

const cleanOwnedUploadDirectory = (): void => {
  assertOwnedAncestors(outputRoot, "Worker output root");
  assertOwnedAncestors(uploadDirectory, "Worker upload directory");
  assertOwnedDirectory(outputRoot, "Worker output root");
  assertOwnedDirectory(uploadDirectory, "Worker upload directory");
  mkdirSync(outputRoot, { recursive: true });
  if (lstatIfExists(uploadDirectory) === undefined) {
    mkdirSync(uploadDirectory);
    return;
  }
  for (const entry of readdirSync(uploadDirectory)) {
    if (entry !== "worker.js") {
      throw new Error(
        `Refusing to remove unexpected worker output ${join(uploadDirectory, entry)}`,
      );
    }
    rmSync(join(uploadDirectory, entry), { force: true });
  }
};

const inspectUploadDirectory = (): number => {
  const entries = readdirSync(uploadDirectory);
  if (entries.length !== 1 || entries[0] !== "worker.js") {
    throw new Error("Worker upload directory must contain exactly worker.js");
  }
  const published = lstatSync(workerPath);
  if (!published.isFile() || published.isSymbolicLink()) {
    throw new Error("Published worker.js is not a regular non-symlink file");
  }
  return 1;
};

let outputRootValidated = false;

const outputFile = (outputs: Array<Bun.BuildArtifact>, label: string): Bun.BuildArtifact => {
  const output = outputs.find((candidate) => candidate.path.endsWith("worker.js"));
  if (output === undefined) throw new Error(`${label} build did not produce worker.js`);
  return output;
};

const readBytes = async (path: string): Promise<Uint8Array> =>
  new Uint8Array(await Bun.file(path).arrayBuffer());

const spawnBytes = async (
  command: Array<string>,
  input?: Uint8Array,
  cwd: string = repositoryRoot,
): Promise<Uint8Array> => {
  const process = Bun.spawn({
    cmd: command,
    cwd,
    stdin: input,
    stdout: "pipe",
    stderr: "pipe",
  });
  const [stdout, stderr, exitCode] = await Promise.all([
    process.stdout.bytes(),
    process.stderr.text(),
    process.exited,
  ]);
  if (exitCode !== 0) throw new Error(`${command.join(" ")} failed with ${exitCode}: ${stderr}`);
  return stdout;
};

const commandText = async (command: Array<string>, cwd: string = repositoryRoot): Promise<string> =>
  new TextDecoder().decode(await spawnBytes(command, undefined, cwd));

const build = async (): Promise<void> => {
  assertOwnedAncestors(outputRoot, "Worker output root");
  outputRootValidated = true;
  invalidateReport();
  assertOwnedAncestors(uploadDirectory, "Worker upload directory");
  assertBuiltExports();
  cleanOwnedUploadDirectory();
  let temporaryRoot: string | undefined;
  try {
    temporaryRoot = mkdtempSync(join(Bun.env.TMPDIR ?? "/tmp", "streamsy-worker-build-"));
    const minifiedRoot = join(temporaryRoot, "minified");
    const rawRoot = join(temporaryRoot, "raw");
    mkdirSync(minifiedRoot);
    mkdirSync(rawRoot);
    const minified = await Bun.build({
      entrypoints: [workerSource],
      outdir: minifiedRoot,
      naming: { entry: "worker.js" },
      target: "browser",
      format: "esm",
      minify: true,
      sourcemap: "none",
      external: ["cloudflare:workers"],
      metafile: true,
    });
    if (!minified.success || minified.metafile === undefined) {
      throw new Error(`Worker build failed: ${JSON.stringify(minified.logs)}`);
    }
    const minifiedOutput = outputFile(minified.outputs, "Minified");
    const raw = await Bun.build({
      entrypoints: [workerSource],
      outdir: rawRoot,
      naming: { entry: "worker.js" },
      target: "browser",
      format: "esm",
      minify: false,
      sourcemap: "none",
      external: ["cloudflare:workers"],
    });
    if (!raw.success) throw new Error(`Raw worker build failed: ${JSON.stringify(raw.logs)}`);
    const rawOutput = outputFile(raw.outputs, "Raw");
    const minifiedBytes = await readBytes(minifiedOutput.path);
    const rawBytes = (await readBytes(rawOutput.path)).byteLength;
    cpSync(minifiedOutput.path, workerPath);
    const outputModules = inspectUploadDirectory();
    const publishedBytes = await readBytes(workerPath);
    const gzipInput = new ArrayBuffer(publishedBytes.byteLength);
    new Uint8Array(gzipInput).set(publishedBytes);
    const gzipBytesCliFile = (await spawnBytes(["gzip", "-9", "-c", workerPath])).byteLength;
    const gzipBytesCliStdin = (await spawnBytes(["gzip", "-9", "-c"], publishedBytes)).byteLength;
    const gzipBytesBun = Bun.gzipSync(gzipInput, { level: 9 }).byteLength;
    const sourceSha = (await commandText(["git", "rev-parse", "HEAD"], repositoryRoot)).trim();
    const sourceStatus = await commandText(
      ["git", "status", "--porcelain=v1", "--untracked-files=all"],
      repositoryRoot,
    );
    const gzipVersion = (await commandText(["gzip", "--version"])).split("\n")[0] ?? "unknown";
    const report = {
      sourceSha,
      sourceDirty: sourceStatus.trim().length > 0,
      sourceStatus: sourceStatus.trim().length > 0 ? "dirty" : "clean",
      workerSha256: createHash("sha256").update(Buffer.from(publishedBytes)).digest("hex"),
      buildTimestamp: new Date().toISOString(),
      bunVersion: Bun.version,
      gzipToolVersion: gzipVersion,
      entrypoint: workerSource,
      flags: {
        target: "browser",
        format: "esm",
        minify: true,
        sourcemap: "none",
        external: ["cloudflare:workers"],
        compatibilityDate: "2026-07-30",
        compatibilityFlags: ["nodejs_compat"],
      },
      rawBytes,
      minifiedBytes: minifiedBytes.byteLength,
      gzipBytesCliFile,
      gzipBytesCliStdin,
      gzipBytesBun,
      inputModules: Object.keys(minified.metafile.inputs).length,
      outputModules,
      conformanceProfile: "single-object-chain",
      placement: { kind: "byKey", key: "conformance" },
      pathPrefix: "/",
      conformanceLongPollTimeoutMs: 1_500,
      acceptedBatchBGzipBytes: 81_574,
      proposalGzipBytes: 27_160,
      hostedStatus:
        "Hosted execution and acceptance remain blocked by remote permission, the missing uploaded-compressed-byte/startup-CPU policy, and Gareth's budget/topology decision.",
      hostedComparison:
        "Local bytes are not uploaded compressed bytes and do not establish hosted acceptance.",
    };
    await Bun.write(reportPath, `${JSON.stringify(report, null, 2)}\n`);
  } finally {
    if (temporaryRoot !== undefined) rmSync(temporaryRoot, { recursive: true, force: true });
  }
};

try {
  await build();
} catch (error) {
  // Only a validated output root may be touched by stale-report invalidation;
  // an unowned/symlinked root is left entirely untouched on every failure path.
  try {
    if (outputRootValidated) {
      assertOwnedAncestors(outputRoot, "Worker output root");
      invalidateReport();
    }
  } catch (cleanupError) {
    // oxlint-disable-next-line eslint(preserve-caught-error) -- AggregateError retains both the original build failure and report-cleanup failure.
    throw new AggregateError(
      [error, cleanupError],
      "Worker build failed and report cleanup failed",
      {
        cause: error,
      },
    );
  }
  throw error;
}
