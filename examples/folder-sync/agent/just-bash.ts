/* oxlint-disable effecttsgo/async-function -- Implements just-bash Promise filesystem methods. */
/* oxlint-disable effecttsgo/global-date -- FsStat requires native Date values. */
import { Predicate } from "effect";
import { InMemoryFs, MountableFs } from "just-bash";
import type { BufferEncoding, FileContent, FsStat, IFileSystem } from "just-bash";
import { ShellDevices } from "./devices.ts";
import { FileSystemError, FolderHttpFileSystem } from "./http.ts";

// just-bash's contract supports encodings beyond TextDecoder. Buffer is Bun's byte compatibility API.
const encoding = (options?: { encoding?: BufferEncoding | null } | BufferEncoding) =>
  Predicate.isString(options) ? options : (options?.encoding ?? "utf8");

const bytes = (content: FileContent, options?: { encoding?: BufferEncoding } | BufferEncoding) =>
  Predicate.isString(content) ? new Uint8Array(Buffer.from(content, encoding(options))) : content;

export function justBashFileSystem(core: FolderHttpFileSystem): IFileSystem {
  const stat = async (path: string): Promise<FsStat> => {
    const entry = await core.stat(path);

    if (entry === undefined) throw new FileSystemError("ENOENT", path);

    return {
      isFile: entry.kind === "file",
      isDirectory: entry.kind === "directory",
      isSymbolicLink: false,
      mode: entry.kind === "file" ? 0o644 : 0o755,
      size: entry.size,
      mtime: new Date(entry.mtimeMs ?? 0),
    };
  };

  const unsupported = async (path: string): Promise<never> => {
    core.assertWritable(path);
    throw new FileSystemError("ENOTSUP", path);
  };

  const fs: IFileSystem = {
    async readFile(path, options) {
      return Buffer.from((await core.read(path)).bytes).toString(encoding(options));
    },
    async readFileBuffer(path) {
      return (await core.read(path)).bytes;
    },
    async writeFile(path, content, options) {
      await core.mutate(path, bytes(content, options));
    },
    async appendFile(path, content, options) {
      core.assertWritable(path);
      let old = new Uint8Array();
      let observedVersion: number | undefined;

      try {
        const opened = await core.read(path);
        old = opened.bytes;
        observedVersion = opened.version;
      } catch (error) {
        if (!(error instanceof FileSystemError && error.code === "ENOENT")) throw error;
      }

      const extra = bytes(content, options);
      const combined = new Uint8Array(old.length + extra.length);
      combined.set(old);
      combined.set(extra, old.length);
      await core.mutate(path, combined, { observedVersion });
    },
    async exists(path) {
      return (await core.stat(path)) !== undefined;
    },
    stat,
    lstat: stat,
    async mkdir(path, options) {
      core.assertWritable(path);

      if (options?.recursive && (await core.stat(path))?.kind === "directory") return;
      await core.mkdir(path, options?.recursive);
    },
    async readdir(path) {
      return (await core.list(path)).map((entry) => entry.name);
    },
    async readdirWithFileTypes(path) {
      return (await core.list(path)).map((entry) => ({
        name: entry.name,
        isFile: entry.kind === "file",
        isDirectory: entry.kind === "directory",
        isSymbolicLink: false,
      }));
    },
    async rm(path, options) {
      core.assertWritable(path);
      const entry = await core.stat(path);

      if (entry === undefined && options?.force) return;

      if (entry?.kind === "directory") await unsupported(path);
      await core.mutate(path, null);
    },
    async cp(src, dest) {
      core.assertWritable(dest);

      if ((await stat(src)).isDirectory) await unsupported(src);
      const opened = await core.read(src);
      // A source version describes the destination only for a same-path copy.
      const observedVersion = core.resolve(src) === core.resolve(dest) ? opened.version : undefined;
      await core.mutate(dest, opened.bytes, { observedVersion });
    },
    async mv(_src, dest) {
      return unsupported(dest);
    },
    resolvePath(base, path) {
      return core.resolve(path, base);
    },
    getAllPaths() {
      return core.allPaths();
    },
    async chmod(path) {
      return unsupported(path);
    },
    async symlink(_target, linkPath) {
      return unsupported(linkPath);
    },
    async link(_existing, newPath) {
      return unsupported(newPath);
    },
    async readlink(path) {
      await stat(path);
      throw new FileSystemError("EINVAL", path);
    },
    async realpath(path) {
      await stat(path);

      return core.resolve(path);
    },
    async utimes(path) {
      core.assertWritable(path);

      if (!(await stat(path)).isFile) await unsupported(path);
      // The file API has no timestamps to change. A writable file can still be touched.
    },
  };

  return new MountableFs({
    base: fs,
    mounts: [
      { mountPoint: "/dev", filesystem: new ShellDevices() },
      { mountPoint: "/tmp", filesystem: new InMemoryFs() },
    ],
  });
}
