/* oxlint-disable effecttsgo/async-function -- just-bash session and command Promise compatibility edge. */
import { defineCommand } from "just-bash";
import { FolderHttpFileSystem } from "./http.ts";

/** Explicit FTS candidate scope, followed by the real just-bash grep over current file bytes. */
export const searchGrep = (core: FolderHttpFileSystem) =>
  defineCommand("grep", async (args, ctx) => {
    try {
      if (ctx.origCommand === undefined) throw new Error("Built-in grep is unavailable");

      if (args[0] !== "--search") return await ctx.origCommand(args);

      if (args[1] === undefined)
        return { stdout: "", stderr: "grep: missing FTS query\n", exitCode: 2 };
      const query = args[1];
      const rest = args.slice(2);
      const flags: string[] = [];

      while (rest[0]?.startsWith("-")) {
        const flag = rest.shift();

        if (flag === "--") break;

        if (flag === undefined || !/^-[FinwlcvqHErR]+$/.test(flag))
          return {
            stdout: "",
            stderr: "grep: supported options are -F -E -i -n -w -l -c -v -q -H -r -R\n",
            exitCode: 2,
          };
        flags.push(flag);
      }

      const pattern = rest.shift();

      if (pattern === undefined)
        return { stdout: "", stderr: "grep: missing pattern\n", exitCode: 2 };

      const scopes =
        rest.length === 0 ? [ctx.cwd] : rest.map((path) => core.resolve(path, ctx.cwd));

      const { hits, truncated } = await core.searchCandidates(query, ctx.signal);
      const note = truncated
        ? "grep: search reached the 100-hit limit; candidates may be incomplete\n"
        : "";

      const candidates = [...new Set(hits.map((hit) => hit.path))].filter((path) =>
        scopes.some((scope) => scope === "/" || path === scope || path.startsWith(`${scope}/`)),
      );

      if (candidates.length === 0) return { stdout: "", stderr: note, exitCode: 1 };
      // The interpreter supplies the original command with stdin, limits, and defence context intact.
      const result = await ctx.origCommand([...flags, "--", pattern, ...candidates]);

      return { ...result, stderr: note + result.stderr };
    } catch (error) {
      return {
        stdout: "",
        stderr: `grep: ${error instanceof Error ? error.message : "search failed"}\n`,
        exitCode: 2,
      };
    }
  });
