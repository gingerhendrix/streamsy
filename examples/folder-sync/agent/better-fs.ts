/* oxlint-disable effecttsgo/async-function -- Implements the external Promise filesystem contract. */
import { Array } from "effect";
import { posixPaths } from "@better-fs-tools/fs";
import type {
  FileSystemError as ReadError,
  MutationError,
  OpenFile,
  WritableFileSystem,
} from "@better-fs-tools/fs";
import { FILE_LIMIT } from "../server/domain.ts";
import { FileSystemError, FolderHttpFileSystem } from "./http.ts";

// oxlint-disable-next-line anti-slop/no-unknown-parameters -- Catch boundary parses errno errors before mapping the tools contract.
const mappedError = (error: unknown): MutationError => {
  if (!(error instanceof FileSystemError))
    return { reason: "io", detail: "Filesystem request failed" };
  const cause = { code: error.code };

  switch (error.code) {
    case "ENOENT":
    case "ENOTDIR":
      return { reason: "not-found", cause };
    case "EACCES":
      return { reason: "permission-denied", cause };
    case "EROFS":
      return { reason: "read-only", cause };
    case "EEXIST":
      return { reason: "exists", cause };
    case "ESTALE":
      return { reason: "changed", cause };
    case "ECANCELED":
      return { reason: "aborted", cause };
    case "EFBIG":
      return { reason: "too-large", limit: FILE_LIMIT, size: null, cause };
    case "EISDIR":
      return {
        reason: "not-a-file",
        kind: "directory",
        target: { resolvedPath: error.path, displayPath: error.path },
        cause,
      };
    case "ENOTSUP":
      return { reason: "unsupported", cause };
    default:
      return { reason: "io", cause };
  }
};

// oxlint-disable-next-line anti-slop/no-unknown-parameters -- Read and mutation catch boundaries share the errno parser.
const readError = (error: unknown): ReadError => {
  const mapped = mappedError(error);

  switch (mapped.reason) {
    case "changed":
    case "exists":
    case "read-only":
    case "no-space":
      return { reason: "io", cause: mapped.cause };
    case "not-a-file":
    case "too-large":
      return mapped;
    default:
      return {
        reason: mapped.reason,
        cause: mapped.cause,
      };
  }
};

/**
 * Whole-file snapshots. A write is atomic. A precondition is checked against a
 * fresh snapshot before the commit, not next to it, so there is no compare-and-swap.
 */
export function betterFileSystem(core: FolderHttpFileSystem): WritableFileSystem {
  return {
    id: "folder-sync",
    capabilities: { streaming: false, identity: false },
    paths: posixPaths,
    writeCapabilities: { atomic: true, compareAndSwap: false, preserveMode: false },
    async open(path, options) {
      try {
        const opened = await core.read(path, options.signal);
        let consumed = false;
        let closed = false;

        const file: OpenFile = {
          info: {
            resolvedPath: opened.resolved,
            displayPath: opened.resolved,
            size: opened.bytes.length,
            mtimeMs: null,
            identity: null,
            mimeType: null,
            version: String(opened.version),
          },
          bytes() {
            if (consumed) throw new TypeError("File bytes are single-use");
            consumed = true;

            return {
              async *[Symbol.asyncIterator]() {
                if (!closed && !options.signal?.aborted && opened.bytes.length > 0)
                  yield opened.bytes;
              },
            };
          },
          async verify() {
            try {
              const head = await core.head(opened.resolved, options.signal);

              return {
                ok: true,
                changed: head === undefined || head.deleted || head.version !== opened.version,
              };
            } catch (error) {
              return { ok: false, error: readError(error) };
            }
          },
          async close() {
            closed = true;
            consumed = true;
          },
        };

        return { ok: true, file };
      } catch (error) {
        return { ok: false, error: readError(error) };
      }
    },
    async list(path, options) {
      try {
        if (!Number.isSafeInteger(options.limit) || options.limit <= 0)
          throw new FileSystemError("EINVAL", path);
        const entries = await core.list(path, options.signal);

        return {
          ok: true,
          entries: Array.map(entries.slice(0, options.limit), (entry) => ({
            name: entry.name,
            type: entry.kind,
          })),
          truncated: entries.length > options.limit,
        };
      } catch (error) {
        return { ok: false, error: readError(error) };
      }
    },
    async stat(path, options) {
      try {
        const resolved = core.resolve(path);
        const stat = await core.stat(resolved, options.signal);

        if (stat?.kind === "directory") throw new FileSystemError("EISDIR", resolved);

        if (stat === undefined)
          return {
            ok: true,
            stat: {
              exists: false,
              resolvedPath: resolved,
              displayPath: resolved,
              missingDirectories: await core.missingParents(resolved, options.signal),
            },
          };

        return {
          ok: true,
          stat: {
            exists: true,
            resolvedPath: resolved,
            displayPath: resolved,
            size: stat.size,
            mtimeMs: stat.mtimeMs,
            version: String(stat.version),
            identity: null,
            mode: null,
            hardLinks: null,
          },
        };
      } catch (error) {
        return { ok: false, error: readError(error) };
      }
    },
    async write(path, bytes, options) {
      try {
        const result = await core.mutate(path, bytes, options);

        return {
          ok: true,
          file: {
            ...result,
            resolvedPath: result.resolved,
            displayPath: result.resolved,
            identity: null,
            atomic: true,
          },
        };
      } catch (error) {
        return { ok: false, error: mappedError(error) };
      }
    },
    async remove(path, options) {
      try {
        const result = await core.mutate(path, null, options);

        return {
          ok: true,
          file: {
            ...result,
            resolvedPath: result.resolved,
            displayPath: result.resolved,
            identity: null,
            atomic: true,
          },
        };
      } catch (error) {
        return { ok: false, error: mappedError(error) };
      }
    },
  };
}
