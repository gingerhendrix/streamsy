/* oxlint-disable effecttsgo/async-function -- just-bash session and command Promise compatibility edge. */
import { Bash, defineCommand } from "just-bash";
import type { IFileSystem } from "just-bash";
import { FolderHttpFileSystem } from "./http.ts";

const quote = (text: string) => `'${text.replaceAll("'", "'\\''")}'`;

/** Explicit FTS candidate scope, followed by the real just-bash grep over current file bytes. */
export const searchGrep = (core: FolderHttpFileSystem, fs: IFileSystem) =>
  defineCommand("grep", async (args, ctx) => {
    if (args[0] !== "--search" || args[1] === undefined)
      return {
        stdout: "",
        stderr: "grep: use --search FTS_QUERY [OPTIONS] PATTERN [PATH ...]\n",
        exitCode: 2,
      };

    try {
      const query = args[1];
      const rest = args.slice(2);
      const flags: string[] = [];

      while (rest[0]?.startsWith("-")) {
        const flag = rest.shift();

        if (flag === "--") break;

        if (flag === undefined || !/^-[FinwlcvqHE]+$/.test(flag))
          return {
            stdout: "",
            stderr: "grep: supported options are -F -E -i -n -w -l -c -v -q -H\n",
            exitCode: 2,
          };
        flags.push(flag);
      }

      const pattern = rest.shift();

      if (pattern === undefined)
        return { stdout: "", stderr: "grep: missing pattern\n", exitCode: 2 };

      const scopes =
        rest.length === 0 ? [ctx.cwd] : rest.map((path) => core.resolve(path, ctx.cwd));

      const hits = await core.search(query);

      const candidates = [...new Set(hits.map((hit) => hit.path))].filter((path) =>
        scopes.some((scope) => scope === "/" || path === scope || path.startsWith(`${scope}/`)),
      );

      if (candidates.length === 0) return { stdout: "", stderr: "", exitCode: 1 };
      const real = new Bash({ fs, cwd: ctx.cwd, commands: ["grep"], defenseInDepth: false });

      return await real.exec(["grep", ...flags, "--", pattern, ...candidates].map(quote).join(" "));
    } catch (error) {
      return {
        stdout: "",
        stderr: `grep: ${error instanceof Error ? error.message : "search failed"}\n`,
        exitCode: 2,
      };
    }
  });
