/* oxlint-disable effecttsgo/async-function -- Promise filesystem compatibility boundary; HTTP retries use Effect. */
/* oxlint-disable effecttsgo/global-date -- HTTP Retry-After dates and filesystem metadata use native milliseconds. */
/* oxlint-disable effecttsgo/global-fetch -- Injectable standard Request transport also serves in-process hosts. */
/* oxlint-disable effecttsgo/crypto-random-uuid -- Session and operation IDs are transport metadata. */
/* oxlint-disable effecttsgo/extends-native-error -- just-bash requires thrown errno errors, shared with the typed tools edge. */
import { Duration, Effect, Result, Schedule, Schema } from "effect";
import { FILE_LIMIT, FolderId, Version, Path as FilePath } from "../server/domain.ts";
import { ManifestFile } from "../server/streams.ts";
import { SearchResult } from "../server/search-result.ts";
import { folderRoot } from "../sync/layout.ts";
import { encodeFilePath } from "../sync/decisions.ts";

export type Errno =
  | "EINVAL"
  | "ENOENT"
  | "EFBIG"
  | "EACCES"
  | "EROFS"
  | "ENAMETOOLONG"
  | "EIO"
  | "EISDIR"
  | "ENOTDIR"
  | "EEXIST"
  | "ESTALE"
  | "ENOTSUP"
  | "ECANCELED";

/** Expected failures retain an errno at the shell edge and a reason at the tools edge. */
export class FileSystemError extends Error {
  constructor(
    readonly code: Errno,
    readonly path: string,
    readonly retryable = false,
    readonly retryAfterMs = 0,
  ) {
    super(`${code}: ${path}`);
  }
}

export interface AgentOptions {
  readonly server: string;
  readonly token?: string;
  readonly folders?: readonly string[];
  readonly writable?: readonly string[];
  readonly folderDirectory?: (id: string) => string;
  /** Transport injection for in-process hosts. */
  readonly fetch?: (request: Request) => Promise<Response>;
  /** Discovered overlaps are skipped once, with a host warning by default. */
  readonly onWarning?: (message: string) => void;
}

export interface FileHead {
  readonly version: number;
  readonly size: number;
  readonly mtimeMs: number | null;
  readonly deleted: boolean;
}

export interface NodeStat {
  readonly kind: "file" | "directory";
  readonly size: number;
  readonly version: number | null;
  readonly mtimeMs: number | null;
}

export interface MutationOptions {
  readonly precondition?:
    | { readonly kind: "any" | "absent" }
    | { readonly kind: "version"; readonly version: string };
  readonly createParents?: boolean;
  /** Pin the bytes read by an append or same-path copy, across asynchronous checks. */
  readonly observedVersion?: number;
  readonly signal?: AbortSignal;
}

const Written = Schema.Struct({ version: Version, overwrote: Schema.optionalKey(Version) });

const History = Schema.Array(
  Schema.Struct({
    version: Version,
    operation: Schema.Struct({
      type: Schema.Literals(["put", "delete"]),
      size: Schema.optionalKey(Schema.Int),
      updatedAt: Schema.optionalKey(Schema.String),
    }),
  }),
);

const statusCode = (status: number): Errno => {
  switch (status) {
    case 400:
      return "EINVAL";
    case 404:
      return "ENOENT";
    case 413:
      return "EFBIG";
    case 401:
    case 403:
      return "EACCES";
    default:
      return "EIO";
  }
};

const retryAfter = (value: string | null) => {
  if (value === null) return 0;
  const seconds = Number(value);

  if (Number.isFinite(seconds) && seconds >= 0) return seconds * 1000;
  const date = Date.parse(value);

  return Number.isFinite(date) ? Math.max(0, date - Date.now()) : 0;
};

/** The HTTP core deliberately imports neither filesystem library. No background work or head TTL. */
export class FolderHttpFileSystem {
  private readonly roots = new Map<string, string>();
  private readonly skippedFolders = new Set<string>();
  private readonly observedVersions = new Map<string, number>();
  private readonly emptyDirectories = new Set<string>();
  // Session writes overlay the eventually consistent manifest until newer rows arrive.
  private readonly mutations = new Map<
    string,
    { head: FileHead; folderId: string; file: string }
  >();
  private paths: string[] = ["/"];
  private readonly origin = crypto.randomUUID();
  private readonly transport: (request: Request) => Promise<Response>;

  private constructor(private readonly options: AgentOptions) {
    this.transport = options.fetch ?? ((request) => fetch(request));
  }

  static async create(options: AgentOptions): Promise<FolderHttpFileSystem> {
    const fs = new FolderHttpFileSystem(options);

    for (const id of new Set([...(options.folders ?? []), ...(options.writable ?? [])]))
      await fs.addFolder(id);
    await fs.snapshot();

    return fs;
  }

  private async addFolder(id: string, discovered = false) {
    if (discovered && this.skippedFolders.has(id)) return;

    if (this.options.folders !== undefined && !this.options.folders.includes(id)) return;

    if (this.roots.has(id)) return;

    if (id.length > 100 || new TextEncoder().encode(id).length > 249)
      throw new FileSystemError("ENAMETOOLONG", id);

    if (!Schema.is(FolderId)(id)) throw new FileSystemError("EINVAL", id);

    const mapped = await Effect.runPromise(
      Effect.result(
        folderRoot(id, (this.options.folderDirectory ?? ((value) => value))(id), this.roots),
      ),
    );

    if (Result.isFailure(mapped)) {
      if (discovered && mapped.failure.message.startsWith("Overlapping folder directories:")) {
        this.skippedFolders.add(id);
        const message = `Skipping discovered folder ${id}: ${mapped.failure.message}`;

        if (this.options.onWarning !== undefined) this.options.onWarning(message);
        else await Effect.runPromise(Effect.logWarning(message));

        return;
      }

      throw new FileSystemError("EINVAL", mapped.failure.message);
    }

    this.roots.set(id, mapped.success);
  }

  resolve(path: string, base = "/") {
    // POSIX paths, with traversal resolved inside the virtual root.
    // oxlint-disable-next-line eslint/no-control-regex -- POSIX paths reject control characters.
    if (!path.isWellFormed() || /[\\\x00-\x1f\x7f]/.test(path))
      throw new FileSystemError("EINVAL", path);
    const parts: string[] = [];

    for (const part of (path.startsWith("/") ? path : `${base}/${path}`).split("/")) {
      if (part === "..") parts.pop();
      else if (part !== "" && part !== ".") parts.push(part);
    }

    if (parts[0] === ".folder-sync") throw new FileSystemError("EACCES", path);

    return `/${parts.join("/")}`;
  }

  allPaths() {
    return [...this.paths];
  }
  folderPath(id: string, file: string) {
    const root = this.roots.get(id);

    return root === undefined ? undefined : `/${root}/${file}`;
  }
  private locate(path: string) {
    const resolved = this.resolve(path);

    for (const [folderId, root] of this.roots) {
      if (resolved.startsWith(`/${root}/`)) {
        const file = resolved.slice(root.length + 2);

        if (!Schema.is(FilePath)(file)) throw new FileSystemError("EINVAL", path);

        return { folderId, file, resolved };
      }
    }

    return undefined;
  }
  assertWritable(path: string) {
    const target = this.locate(path);

    if (target === undefined || !this.options.writable?.includes(target.folderId))
      throw new FileSystemError("EROFS", path);

    return target;
  }
  private url(folderId: string, file: string, kind = "files") {
    return `/api/folders/${encodeURIComponent(folderId)}/${kind}/${encodeFilePath(file)}`;
  }

  private async request(route: string, init: RequestInit = {}, signal?: AbortSignal) {
    if (signal?.aborted) throw new FileSystemError("ECANCELED", route);

    const work = Effect.tryPromise({
      try: async () => {
        const response = await this.transport(
          new Request(`${this.options.server.replace(/\/$/, "")}${route}`, {
            ...init,
            signal,
            redirect: "error",
          }),
        );

        if (!response.ok) {
          const error = new FileSystemError(
            statusCode(response.status),
            route,
            [408, 409, 429, 503].includes(response.status),
            retryAfter(response.headers.get("retry-after")),
          );

          await response.body?.cancel();
          throw error;
        }

        return response;
      },
      catch: (error) =>
        error instanceof FileSystemError
          ? error
          : new FileSystemError(signal?.aborted ? "ECANCELED" : "EIO", route),
    }).pipe(
      Effect.retry({
        while: (error) => error.retryable,
        schedule: Schedule.exponential("100 millis").pipe(
          Schedule.upTo({ times: 3 }),
          Schedule.modifyDelay(({ input, duration }) =>
            Effect.succeed(
              Math.max(Math.min(Duration.toMillis(duration), 1000), input.retryAfterMs),
            ),
          ),
        ),
      }),
    );

    try {
      const result = await Effect.runPromise(Effect.result(work), { signal });

      if (Result.isFailure(result)) throw result.failure;

      return result.success;
    } catch (error) {
      if (signal?.aborted) throw new FileSystemError("ECANCELED", route);
      throw error;
    }
  }

  private async decode<S extends Schema.ConstraintDecoder<unknown>>(
    response: Response,
    schema: S,
  ): Promise<S["Type"]> {
    try {
      return Schema.decodeUnknownSync(schema)(await response.json());
    } catch {
      throw new FileSystemError("EIO", "Invalid API response");
    }
  }

  private async snapshot(signal?: AbortSignal) {
    const rows = await this.decode(
      await this.request("/api/ls", {}, signal),
      Schema.Array(ManifestFile),
    );

    if (this.options.folders === undefined)
      for (const row of rows) await this.addFolder(row.folderId, true);
    const files = new Map<string, FileHead>();

    for (const row of rows) {
      const path = this.folderPath(row.folderId, row.path);

      if (path !== undefined)
        files.set(path, {
          version: row.version,
          size: row.size,
          deleted: false,
          mtimeMs: Date.parse(row.updatedAt),
        });
    }

    for (const path of this.mutations.keys()) {
      const current = await this.head(path, signal);
      const remote = files.get(path);

      if (current !== undefined && remote !== undefined && remote.version >= current.version) {
        this.mutations.delete(path);
      } else if (current === undefined || current.deleted) {
        files.delete(path);
      } else {
        files.set(path, current);
      }
      // The receipt remains an observation until the manifest catches up.
      // Reading the authoritative head here also notices external deletions.
    }

    const directories = new Set(["/", ...this.emptyDirectories]);

    const addParents = (path: string) => {
      const parts = path.split("/").slice(1);

      for (let i = 1; i <= parts.length; i++) directories.add(`/${parts.slice(0, i).join("/")}`);
    };

    for (const root of this.roots.values()) addParents(`/${root}`);

    for (const path of files.keys()) addParents(path.slice(0, path.lastIndexOf("/")));
    this.paths = [...directories, ...files.keys()].toSorted();

    return { files, directories };
  }

  async head(path: string, signal?: AbortSignal): Promise<FileHead | undefined> {
    const target = this.locate(path);

    if (target === undefined) return undefined;

    let history: typeof History.Type;

    try {
      history = await this.decode(
        await this.request(this.url(target.folderId, target.file, "history"), {}, signal),
        History,
      );
    } catch (error) {
      if (error instanceof FileSystemError && error.code === "ENOENT") return undefined;
      throw error;
    }

    const last = history.at(-1);

    return last === undefined
      ? undefined
      : {
          version: last.version,
          deleted: last.operation.type === "delete",
          size: last.operation.size ?? 0,
          mtimeMs:
            last.operation.updatedAt === undefined ? null : Date.parse(last.operation.updatedAt),
        };
  }

  async stat(path: string, signal?: AbortSignal): Promise<NodeStat | undefined> {
    const resolved = this.resolve(path);
    const snapshot = await this.snapshot(signal);
    const head = await this.head(resolved, signal);

    if (head !== undefined && !head.deleted) return { kind: "file", ...head };

    if (snapshot.directories.has(resolved))
      return { kind: "directory", size: 0, version: null, mtimeMs: null };

    return undefined;
  }

  async read(path: string, signal?: AbortSignal) {
    const target = this.locate(path);

    if (target === undefined) {
      if ((await this.stat(path, signal))?.kind === "directory")
        throw new FileSystemError("EISDIR", path);
      throw new FileSystemError("ENOENT", path);
    }

    try {
      const response = await this.request(this.url(target.folderId, target.file), {}, signal);
      const version = Number(response.headers.get("x-file-version"));

      if (!Schema.is(Version)(version)) throw new FileSystemError("EIO", path);
      const bytes = new Uint8Array(await response.arrayBuffer());

      this.observedVersions.set(target.resolved, version);

      return { bytes, version, resolved: target.resolved };
    } catch (error) {
      if (
        error instanceof FileSystemError &&
        error.code === "ENOENT" &&
        (await this.stat(path, signal))?.kind === "directory"
      )
        throw new FileSystemError("EISDIR", path);
      throw error;
    }
  }

  async list(path: string, signal?: AbortSignal) {
    const resolved = this.resolve(path);
    const { files, directories } = await this.snapshot(signal);

    if (!directories.has(resolved))
      throw new FileSystemError(files.has(resolved) ? "ENOTDIR" : "ENOENT", path);
    const prefix = resolved === "/" ? "/" : `${resolved}/`;
    const entries = new Map<string, "file" | "directory">();

    for (const candidate of [...directories, ...files.keys()]) {
      if (candidate === resolved || !candidate.startsWith(prefix)) continue;
      const suffix = candidate.slice(prefix.length);

      if (!suffix.includes("/"))
        entries.set(suffix, directories.has(candidate) ? "directory" : "file");
    }

    return [...entries]
      .toSorted(([a], [b]) => a.localeCompare(b))
      .map(([name, kind]) => ({ name, kind }));
  }

  async missingParents(path: string, signal?: AbortSignal) {
    const missing: string[] = [];
    let parent = this.resolve(path).slice(0, this.resolve(path).lastIndexOf("/")) || "/";

    while (parent !== "/") {
      const stat = await this.stat(parent, signal);

      if (stat !== undefined) {
        if (stat.kind !== "directory") throw new FileSystemError("ENOTDIR", parent);
        break;
      }

      missing.unshift(parent);
      parent = parent.slice(0, parent.lastIndexOf("/")) || "/";
    }

    return missing;
  }

  async mkdir(path: string, recursive = false, signal?: AbortSignal) {
    const target = this.assertWritable(path);

    if (await this.stat(path, signal)) throw new FileSystemError("EEXIST", path);
    const parents = await this.missingParents(path, signal);

    if (!recursive && parents.length > 0) throw new FileSystemError("ENOENT", path);

    for (const parent of parents) {
      this.assertWritable(parent);
      this.emptyDirectories.add(parent);
    }

    this.emptyDirectories.add(target.resolved);
    await this.snapshot(signal);
  }

  async mutate(path: string, bytes: Uint8Array | null, options: MutationOptions = {}) {
    const target = this.assertWritable(path);
    const observedVersion = options.observedVersion ?? this.observedVersions.get(target.resolved);

    if (options.signal?.aborted) throw new FileSystemError("ECANCELED", path);

    if (bytes !== null && bytes.length > FILE_LIMIT) throw new FileSystemError("EFBIG", path);
    const stat = await this.stat(path, options.signal);

    if (stat?.kind === "directory") throw new FileSystemError("EISDIR", path);
    const head = await this.head(path, options.signal);
    const exists = head !== undefined && !head.deleted;
    const condition = options.precondition;

    if (condition?.kind === "absent" && exists) throw new FileSystemError("EEXIST", path);

    if (condition?.kind === "version" && (!exists || String(head?.version) !== condition.version))
      throw new FileSystemError("ESTALE", path);

    if (bytes === null && !exists) throw new FileSystemError("ENOENT", path);
    const parents = await this.missingParents(path, options.signal);

    if (parents.length > 0 && !options.createParents) throw new FileSystemError("ENOENT", path);

    for (const parent of parents) this.assertWritable(parent);

    const headers = new Headers({
      "x-file-op-id": crypto.randomUUID(),
      "x-file-author": "agent",
      "x-file-origin": this.origin,
    });

    // Metadata checks must not claim the session saw an external edit.
    const base = observedVersion ?? head?.version;

    if (base !== undefined) headers.set("x-file-base", String(base));

    if (this.options.token !== undefined)
      headers.set("authorization", `Bearer ${this.options.token}`);

    // The request and opId stay identical across retries. Base is an observation, not CAS.
    const receipt = await this.decode(
      await this.request(
        this.url(target.folderId, target.file),
        {
          method: bytes === null ? "DELETE" : "PUT",
          headers,
          body: bytes === null ? undefined : new Uint8Array(bytes),
        },
        options.signal,
      ),
      Written,
    );

    this.observedVersions.set(target.resolved, receipt.version);

    for (const parent of parents) this.emptyDirectories.add(parent);
    this.mutations.set(target.resolved, {
      ...target,
      head: {
        version: receipt.version,
        size: bytes?.length ?? 0,
        deleted: bytes === null,
        mtimeMs: Date.now(),
      },
    });
    // Keep globs useful immediately, without another network call after a successful write.
    this.paths = this.paths.filter((value) => value !== target.resolved);

    if (bytes !== null)
      this.paths.push(target.resolved, ...parents.filter((value) => !this.paths.includes(value)));

    return {
      resolved: target.resolved,
      version: bytes === null ? null : String(receipt.version),
      size: bytes?.length ?? null,
      createdDirectories: parents,
    };
  }

  async search(query: string, signal?: AbortSignal) {
    return (await this.searchCandidates(query, signal)).hits;
  }

  async searchCandidates(query: string, signal?: AbortSignal) {
    const hits = await this.decode(
      await this.request(`/api/search?q=${encodeURIComponent(query)}`, {}, signal),
      Schema.Array(SearchResult),
    );

    if (this.options.folders === undefined)
      for (const hit of hits) await this.addFolder(hit.folderId, true);

    const candidates = hits.flatMap((hit) => {
      const path = this.folderPath(hit.folderId, hit.path);

      return path === undefined
        ? []
        : [{ path, snippet: hit.snippet.map((part) => part.text).join("") }];
    });

    return { hits: candidates, truncated: hits.length >= 100 };
  }
}
