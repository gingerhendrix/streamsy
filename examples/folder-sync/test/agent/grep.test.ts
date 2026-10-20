/* oxlint-disable effecttsgo/async-function -- Exercise the just-bash command Promise compatibility edge. */
import { expect, test } from "vitest";
import { createCommandContext } from "just-bash";
import { FolderHttpFileSystem, justBashFileSystem, searchGrep } from "../../agent/index.ts";

test("candidate grep warns at the API limit before selection and preserves original command status", async () => {
  const core = await FolderHttpFileSystem.create({
    server: "http://localhost",
    folders: ["docs"],
    fetch: async (request) =>
      new URL(request.url).pathname.startsWith("/streams/")
        ? Response.json([], {
            headers: {
              "stream-next-offset": "0000000000000000_0000000000000000",
              "stream-up-to-date": "true",
            },
          })
        : Response.json(
            Array.from({ length: 100 }, (_, index) => ({
              folderId: "docs",
              path: `a-${index}`,
              version: 1,
              snippet: [{ text: "needle", match: true }],
            })),
          ),
  });

  const args: string[][] = [];
  const command = searchGrep(core);

  const ctx = createCommandContext({
    fs: justBashFileSystem(core),
    cwd: "/",
    origCommand: async (argv) => {
      args.push(argv);

      return { stdout: "", stderr: "", exitCode: 1 };
    },
  });

  const limited = await command.execute(["--search", "needle", "-r", "needle", "/docs"], ctx);
  expect(limited.exitCode).toBe(1);
  expect(limited.stderr).toContain("100-hit limit");
  expect(args[0]?.slice(0, 3)).toEqual(["-r", "--", "needle"]);
  expect(args[0]).toHaveLength(103);
  const excluded = await command.execute(["--search", "needle", "needle", "/elsewhere"], ctx);
  expect(excluded.stderr).toContain("100-hit limit");
  expect(args).toHaveLength(1);
  const ordinaryArgs = ["-rn", "needle", "/docs"];
  const ordinary = await command.execute(ordinaryArgs, ctx);
  expect(ordinary.stderr).toBe("");
  expect(args.at(-1)).toEqual(ordinaryArgs);
});
