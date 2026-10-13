/* oxlint-disable effecttsgo/async-function -- just-bash session and command Promise compatibility edge. */
import { Bash } from "just-bash";
import { betterFileSystem } from "./better-fs.ts";
import { searchGrep } from "./grep.ts";
import { FolderHttpFileSystem } from "./http.ts";
import type { AgentOptions } from "./http.ts";
import { justBashFileSystem } from "./just-bash.ts";

export { FolderHttpFileSystem, FileSystemError } from "./http.ts";

export type { AgentOptions } from "./http.ts";

export { betterFileSystem } from "./better-fs.ts";

export { justBashFileSystem } from "./just-bash.ts";

export { searchGrep } from "./grep.ts";

/** One session shares folder selection and write observations across both compatibility edges. */
export async function createAgentFileSystem(options: AgentOptions) {
  const core = await FolderHttpFileSystem.create(options);
  const fs = justBashFileSystem(core);

  return {
    core,
    fs,
    tools: betterFileSystem(core),
    bash: new Bash({ fs, cwd: "/", customCommands: [searchGrep(core, fs)] }),
  };
}
