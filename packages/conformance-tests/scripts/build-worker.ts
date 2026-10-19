import {
  cpSync,
  existsSync,
  lstatSync,
  mkdirSync,
  mkdtempSync,
  readdirSync,
  rmSync,
} from "node:fs";
import { fileURLToPath } from "node:url";
import { NodePath } from "@effect/platform-node";
import { Array, Context, DateTime, Effect, Layer, Option, Path, Record, Schema } from "effect";

const { dirname, isAbsolute, join, relative, resolve } = Context.get(
  Effect.runSync(Effect.scoped(Layer.build(NodePath.layer))),
  Path.Path,
);

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
  const output = Array.findFirst(outputs, (candidate) => candidate.path.endsWith("worker.js"));

  if (Option.isNone(output)) throw new Error(`${label} build did not produce worker.js`);

  return output.value;
};

const readBytes = (path: string) =>
  Effect.promise(() => Bun.file(path).arrayBuffer()).pipe(
    Effect.map((buffer) => new Uint8Array(buffer)),
  );

const spawnBytes = (command: Array<string>, input?: Uint8Array, cwd: string = repositoryRoot) =>
  Effect.gen(function* () {
    const child = yield* Effect.acquireRelease(
      Effect.sync(() =>
        Bun.spawn({ cmd: command, cwd, stdin: input, stdout: "pipe", stderr: "pipe" }),
      ),
      (process) =>
        Effect.sync(() => {
          if (process.exitCode === null) process.kill();
        }),
    );

    const [stdout, stderr, exitCode] = yield* Effect.all(
      [
        Effect.promise(() => child.stdout.bytes()),
        Effect.promise(() => child.stderr.text()),
        Effect.promise(() => child.exited),
      ],
      { concurrency: "unbounded" },
    );

    if (exitCode !== 0)
      throw new Error(`${Array.join(command, " ")} failed with ${exitCode}: ${stderr}`);

    return stdout;
  });

const commandText = (command: Array<string>, cwd: string = repositoryRoot) =>
  spawnBytes(command, undefined, cwd).pipe(Effect.map((bytes) => new TextDecoder().decode(bytes)));

const build = () =>
  Effect.gen(function* () {
    assertOwnedAncestors(outputRoot, "Worker output root");
    outputRootValidated = true;
    invalidateReport();
    assertOwnedAncestors(uploadDirectory, "Worker upload directory");
    assertBuiltExports();
    cleanOwnedUploadDirectory();

    const temporaryRoot = yield* Effect.acquireRelease(
      Effect.sync(() => mkdtempSync(join(Bun.env.TMPDIR ?? "/tmp", "streamsy-worker-build-"))),
      (root) => Effect.sync(() => rmSync(root, { recursive: true, force: true })),
    );

    const minifiedRoot = join(temporaryRoot, "minified");
    const rawRoot = join(temporaryRoot, "raw");
    mkdirSync(minifiedRoot);
    mkdirSync(rawRoot);

    const minified = yield* Effect.promise(() =>
      Bun.build({
        entrypoints: [workerSource],
        outdir: minifiedRoot,
        naming: { entry: "worker.js" },
        target: "browser",
        format: "esm",
        minify: true,
        sourcemap: "none",
        external: ["cloudflare:workers"],
        metafile: true,
      }),
    );

    if (!minified.success || minified.metafile === undefined) {
      throw new Error(`Worker build failed: ${Array.join(Array.map(minified.logs, String), "; ")}`);
    }

    const minifiedOutput = outputFile(minified.outputs, "Minified");

    const raw = yield* Effect.promise(() =>
      Bun.build({
        entrypoints: [workerSource],
        outdir: rawRoot,
        naming: { entry: "worker.js" },
        target: "browser",
        format: "esm",
        minify: false,
        sourcemap: "none",
        external: ["cloudflare:workers"],
      }),
    );

    if (!raw.success)
      throw new Error(`Raw worker build failed: ${Array.join(Array.map(raw.logs, String), "; ")}`);
    const rawOutput = outputFile(raw.outputs, "Raw");
    const minifiedBytes = yield* readBytes(minifiedOutput.path);
    const rawBytes = (yield* readBytes(rawOutput.path)).byteLength;
    cpSync(minifiedOutput.path, workerPath);
    const outputModules = inspectUploadDirectory();
    const publishedBytes = yield* readBytes(workerPath);
    const gzipInput = new ArrayBuffer(publishedBytes.byteLength);
    new Uint8Array(gzipInput).set(publishedBytes);
    const gzipBytesCliFile = (yield* spawnBytes(["gzip", "-9", "-c", workerPath])).byteLength;
    const gzipBytesCliStdin = (yield* spawnBytes(["gzip", "-9", "-c"], publishedBytes)).byteLength;
    const gzipBytesBun = Bun.gzipSync(gzipInput, { level: 9 }).byteLength;
    const sourceSha = (yield* commandText(["git", "rev-parse", "HEAD"], repositoryRoot)).trim();

    const sourceStatus = yield* commandText(
      ["git", "status", "--porcelain=v1", "--untracked-files=all"],
      repositoryRoot,
    );

    const gzipVersion = (yield* commandText(["gzip", "--version"])).split("\n")[0] ?? "unknown";

    const report = {
      sourceSha,
      sourceDirty: sourceStatus.trim().length > 0,
      sourceStatus: sourceStatus.trim().length > 0 ? "dirty" : "clean",
      workerSha256: new Bun.CryptoHasher("sha256").update(publishedBytes).digest("hex"),
      buildTimestamp: DateTime.formatIso(yield* DateTime.now),
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
      inputModules: Record.keys(minified.metafile.inputs).length,
      outputModules,
      conformanceProfile: "single-object-chain",
      placement: { kind: "byKey", key: "conformance" },
      pathPrefix: "/",
      conformanceLongPollTimeoutMs: 1_500,
      acceptedBatchBGzipBytes: 81_574,
      proposalGzipBytes: 27_160,
      hostedStatus:
        "Alchemy deployment for Step 3 is authorized. Hosted execution remains disabled in this local package pending independently reviewed live adapters and a reconciled run plan, including destroy/cleanup and required query scope. Hosted acceptance still requires hosted evidence, the uploaded-compressed-byte/startup-CPU policy, and Gareth's budget/topology decision. The accepted Batch B local signal is 81,574 B gzip against the unchanged 27,160 B proposal. The 542.85 ms first-object p95 proposal remains unmeasured.",
      hostedComparison:
        "Local bytes are not uploaded compressed bytes and do not establish hosted acceptance.",
    };

    const json = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown))(report);
    yield* Effect.promise(() => Bun.write(reportPath, `${json}\n`));
  }).pipe(Effect.scoped);

try {
  await Effect.runPromise(build());
} catch (error) {
  // Only a validated output root may be touched by stale-report invalidation;
  // an unowned/symlinked root is left entirely untouched on every failure path.
  try {
    if (outputRootValidated) {
      assertOwnedAncestors(outputRoot, "Worker output root");
      invalidateReport();
    }
  } catch (cleanupError) {
    // oxlint-disable-next-line eslint/preserve-caught-error -- AggregateError retains both the original build failure and report-cleanup failure.
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
