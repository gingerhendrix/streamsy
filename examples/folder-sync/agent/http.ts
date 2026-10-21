/* oxlint-disable effecttsgo/async-function -- Promise filesystem compatibility boundary; HTTP retries use Effect. */
/* oxlint-disable effecttsgo/global-date -- HTTP Retry-After dates and filesystem metadata use native milliseconds. */
/* oxlint-disable effecttsgo/global-fetch -- Injectable standard Request transport also serves in-process hosts. */
/* oxlint-disable effecttsgo/crypto-random-uuid -- Session and operation IDs are transport metadata. */
/* oxlint-disable effecttsgo/extends-native-error -- just-bash requires thrown errno errors, shared with the typed tools edge. */
import { Duration, Effect, Result, Schedule, Schema } from "effect";
import { hex } from "../server/bytes.ts";
import { FILE_LIMIT, FolderId, Version, File, Path as FilePath } from "../server/domain.ts";
import { SearchResult } from "../server/index-rows.ts";
import { applyChanges, FileMessage, FolderMessage, isReset, isUpsert } from "../server/messages.ts";
import {
  blobUrl,
  fileUrl,
  folderStreamUrl,
  FOLDERS_STREAM_URL,
  streamRead,
} from "../server/routes.ts";
import { folderRoot } from "../sync/layout.ts";

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

const Written = Schema.Struct({
  version: Schema.optionalKey(Version),
  txid: Schema.optionalKey(Schema.String),
});

const statusCode = (status: number): Errno => {
  switch (status) {
    case 400:
      return "EINVAL";
    case 404:
      return "ENOENT";
    case 412:
      return "ESTALE";
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
  private readonly files = new Map<string, Map<string, File>>();
  private readonly cursors = new Map<string, string>();
  private folderCursor = "-1";
  private paths: string[] = ["/"];
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
  private async request(
    route: string,
    init: RequestInit = {},
    signal?: AbortSignal,
    allowed: readonly number[] = [],
  ) {
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

        if (!response.ok && !allowed.includes(response.status)) {
          const retryable = [408, 409, 429].includes(response.status) || response.status >= 500;
          const retryAfterMs = retryAfter(response.headers.get("retry-after"));

          // Refuse excessive server delays instead of sleeping beyond the session retry ceiling.
          const error = new FileSystemError(
            statusCode(response.status),
            route,
            retryable && retryAfterMs <= 5000,
            retryAfterMs,
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

  private async refreshFolders(signal?: AbortSignal) {
    if (this.options.folders !== undefined) return;

    while (true) {
      const from = this.folderCursor;

      const response = await this.request(streamRead(FOLDERS_STREAM_URL, from), {}, signal, [410]);

      if (from !== this.folderCursor) continue;

      if (response.status === 410) {
        if (from === "-1") throw new FileSystemError("EIO", "Folder bootstrap offset is gone");
        this.roots.clear();
        this.skippedFolders.clear();
        this.files.clear();
        this.cursors.clear();
        this.folderCursor = "-1";
        continue;
      }

      const changes =
        response.status === 204 ? [] : await this.decode(response, Schema.Array(FolderMessage));

      if (from !== this.folderCursor) continue;

      for (const change of changes) {
        if (isReset(change)) {
          this.roots.clear();
          this.skippedFolders.clear();
          this.files.clear();
          this.cursors.clear();
        } else if (isUpsert(change)) await this.addFolder(change.value.folderId, true);
        else {
          this.roots.delete(change.key);
          this.files.delete(change.key);
          this.cursors.delete(change.key);
        }
      }

      const cursor = response.headers.get("stream-next-offset");

      if (cursor === null) throw new FileSystemError("EIO", "Missing folder cursor");
      this.folderCursor = cursor;

      if (response.headers.get("stream-up-to-date") === "true" || response.status === 204) return;
    }
  }

  private async refreshFolder(folderId: string, signal?: AbortSignal) {
    while (true) {
      const from = this.cursors.get(folderId) ?? "-1";

      const response = await this.request(
        streamRead(folderStreamUrl(folderId), from),
        {},
        signal,
        [410, 404],
      );

      if (from !== (this.cursors.get(folderId) ?? "-1")) continue;

      if (response.status === 404) return;

      if (response.status === 410) {
        if (from === "-1") throw new FileSystemError("EIO", "File bootstrap offset is gone");
        this.cursors.delete(folderId);
        this.files.delete(folderId);
        continue;
      }

      const changes =
        response.status === 204 ? [] : await this.decode(response, Schema.Array(FileMessage));

      if (from !== (this.cursors.get(folderId) ?? "-1")) continue;

      const files = applyChanges(this.files.get(folderId) ?? new Map<string, File>(), changes);

      const cursor = response.headers.get("stream-next-offset");

      if (cursor === null) throw new FileSystemError("EIO", "Missing file cursor");
      this.files.set(folderId, files);
      this.cursors.set(folderId, cursor);

      if (response.headers.get("stream-up-to-date") === "true" || response.status === 204) return;
    }
  }

  private async snapshot(signal?: AbortSignal) {
    await this.refreshFolders(signal);
    const files = new Map<string, FileHead>();

    for (const folderId of this.roots.keys()) {
      await this.refreshFolder(folderId, signal);

      for (const row of this.files.get(folderId)?.values() ?? []) {
        const path = this.folderPath(folderId, row.path);

        if (path !== undefined)
          files.set(path, { version: row.version, size: row.size, deleted: false, mtimeMs: null });
      }
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

  /** The head of a path in the folder maps as they are now. No network call. */
  private cachedHead(path: string): FileHead | undefined {
    const target = this.locate(path);

    const row =
      target === undefined ? undefined : this.files.get(target.folderId)?.get(target.file);

    return row === undefined
      ? undefined
      : { version: row.version, size: row.size, deleted: false, mtimeMs: null };
  }

  /** A path in one snapshot: a file, a directory, or nothing. No network call. */
  private statIn(directories: ReadonlySet<string>, resolved: string): NodeStat | undefined {
    const head = this.cachedHead(resolved);

    if (head !== undefined && !head.deleted) return { kind: "file", ...head };

    if (directories.has(resolved))
      return { kind: "directory", size: 0, version: null, mtimeMs: null };

    return undefined;
  }

  /** The parents of a path that do not exist in one snapshot, nearest last. No network call. */
  private missingParentsIn(directories: ReadonlySet<string>, path: string) {
    const missing: string[] = [];
    const resolved = this.resolve(path);
    let parent = resolved.slice(0, resolved.lastIndexOf("/")) || "/";

    while (parent !== "/") {
      const stat = this.statIn(directories, parent);

      if (stat !== undefined) {
        if (stat.kind !== "directory") throw new FileSystemError("ENOTDIR", parent);
        break;
      }

      missing.unshift(parent);
      parent = parent.slice(0, parent.lastIndexOf("/")) || "/";
    }

    return missing;
  }

  async head(path: string, signal?: AbortSignal): Promise<FileHead | undefined> {
    const target = this.locate(path);

    if (target === undefined) return undefined;

    await this.refreshFolder(target.folderId, signal);

    return this.cachedHead(path);
  }

  async stat(path: string, signal?: AbortSignal): Promise<NodeStat | undefined> {
    const { directories } = await this.snapshot(signal);

    return this.statIn(directories, this.resolve(path));
  }

  async read(path: string, signal?: AbortSignal) {
    const target = this.locate(path);

    if (target === undefined) {
      if ((await this.stat(path, signal))?.kind === "directory")
        throw new FileSystemError("EISDIR", path);
      throw new FileSystemError("ENOENT", path);
    }

    await this.refreshFolder(target.folderId, signal);
    const row = this.files.get(target.folderId)?.get(target.file);

    if (row === undefined) {
      this.observedVersions.delete(target.resolved);

      if ((await this.stat(path, signal))?.kind === "directory")
        throw new FileSystemError("EISDIR", path);
      throw new FileSystemError("ENOENT", path);
    }

    const bytes = new Uint8Array(
      await (await this.request(blobUrl(row.hash), {}, signal)).arrayBuffer(),
    );

    this.observedVersions.set(target.resolved, row.version);

    return { bytes, version: row.version, resolved: target.resolved };
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
    const { directories } = await this.snapshot(signal);

    return this.missingParentsIn(directories, path);
  }

  async mkdir(path: string, recursive = false, signal?: AbortSignal) {
    const target = this.assertWritable(path);
    const { directories } = await this.snapshot(signal);

    if (this.statIn(directories, target.resolved)) throw new FileSystemError("EEXIST", path);
    const parents = this.missingParentsIn(directories, path);

    if (!recursive && parents.length > 0) throw new FileSystemError("ENOENT", path);

    for (const parent of parents) {
      this.assertWritable(parent);
      this.emptyDirectories.add(parent);
    }

    this.emptyDirectories.add(target.resolved);
    await this.snapshot(signal);
  }

  /**
   * The conditional headers of a write. A content write without a read or an
   * explicit precondition must be a create. A delete always needs a version.
   */
  private conditionHeaders(
    path: string,
    bytes: Uint8Array | null,
    head: FileHead | undefined,
    condition: MutationOptions["precondition"],
    observedVersion: number | undefined,
  ) {
    const exists = head !== undefined && !head.deleted;

    if (condition?.kind === "absent" && exists) throw new FileSystemError("EEXIST", path);

    if (condition?.kind === "version" && (!exists || String(head?.version) !== condition.version))
      throw new FileSystemError("ESTALE", path);

    if (bytes === null && !exists) throw new FileSystemError("ENOENT", path);
    const base = condition?.kind === "version" ? Number(condition.version) : observedVersion;

    // A delete needs an observed version too; If-None-Match is not valid on DELETE.
    if (bytes === null && base === undefined) throw new FileSystemError("ESTALE", path);
    const headers = new Headers({ "content-type": "application/json" });

    if (condition?.kind === "absent" || base === undefined) {
      if (bytes !== null) headers.set("if-none-match", "*");
    } else headers.set("if-match", `"${base}"`);

    if (this.options.token !== undefined)
      headers.set("authorization", `Bearer ${this.options.token}`);

    return headers;
  }

  private async uploadBlob(hash: string, bytes: Uint8Array, signal?: AbortSignal) {
    await this.request(
      blobUrl(hash),
      {
        method: "PUT",
        headers:
          this.options.token === undefined ? {} : { authorization: `Bearer ${this.options.token}` },
        body: new Uint8Array(bytes),
      },
      signal,
    );
  }

  /** Commit a write. A `409 blob-missing` for this content uploads the blob and commits once more. */
  private async commit(
    target: { folderId: string; file: string },
    init: RequestInit,
    upload: (() => Promise<void>) | undefined,
    hash: string | undefined,
    signal?: AbortSignal,
  ) {
    const url = fileUrl(target.folderId, target.file);
    const response = await this.request(url, init, signal, [409]);

    if (response.status !== 409) return this.decode(response, Written);

    const missing = await this.decode(
      response,
      Schema.Struct({ error: Schema.Literal("blob-missing"), hash: Schema.String }),
    );

    if (upload === undefined || missing.hash !== hash)
      throw new FileSystemError("EIO", target.file);
    await upload();

    return this.decode(await this.request(url, init, signal), Written);
  }

  async mutate(path: string, bytes: Uint8Array | null, options: MutationOptions = {}) {
    const target = this.assertWritable(path);

    // A listed entry is enough to identify a delete. Pin it before metadata refreshes.
    // Content writes still need a read observation or an explicit precondition.
    const observedVersion =
      options.observedVersion ??
      this.observedVersions.get(target.resolved) ??
      (bytes === null ? this.files.get(target.folderId)?.get(target.file)?.version : undefined);

    if (options.signal?.aborted) throw new FileSystemError("ECANCELED", path);

    if (bytes !== null && bytes.length > FILE_LIMIT) throw new FileSystemError("EFBIG", path);

    // One refresh of the registry and every folder. The checks below read that one view.
    const { directories } = await this.snapshot(options.signal);

    if (this.statIn(directories, target.resolved)?.kind === "directory")
      throw new FileSystemError("EISDIR", path);

    const headers = this.conditionHeaders(
      path,
      bytes,
      this.cachedHead(target.resolved),
      options.precondition,
      observedVersion,
    );

    const parents = this.missingParentsIn(directories, path);

    if (parents.length > 0 && !options.createParents) throw new FileSystemError("ENOENT", path);

    for (const parent of parents) this.assertWritable(parent);
    const txid = crypto.randomUUID();

    const hash =
      bytes === null
        ? undefined
        : hex(new Uint8Array(await crypto.subtle.digest("SHA-256", new Uint8Array(bytes))));

    const upload =
      bytes === null || hash === undefined
        ? undefined
        : () => this.uploadBlob(hash, bytes, options.signal);

    // All content goes to the blob store before the commit names it.
    if (upload !== undefined) await upload();

    const content =
      bytes === null
        ? { txid, author: "agent" }
        : { txid, author: "agent", hash, size: bytes.length };

    const receipt = await this.commit(
      target,
      { method: bytes === null ? "DELETE" : "PUT", headers, body: JSON.stringify(content) },
      upload,
      hash,
      options.signal,
    );

    if (bytes !== null && receipt.version === undefined) throw new FileSystemError("EIO", path);

    if (receipt.version === undefined) this.observedVersions.delete(target.resolved);
    else this.observedVersions.set(target.resolved, receipt.version);

    for (const parent of parents) this.emptyDirectories.add(parent);
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
