// oxlint-disable effecttsgo/async-function, effecttsgo/global-fetch -- This test drives a real Node child process over HTTP.
import { Array, Option } from "effect";
import { afterEach, expect, it } from "vitest";
import { fileURLToPath } from "node:url";

const main = fileURLToPath(new URL("./support/node-start-gap-main.ts", import.meta.url));

/** The child runs `src/node.ts` directly, so `node` must strip TypeScript types. */
const nodeRunsTypeScript = (() => {
  if (Bun.which("node") === null) return false;
  const probe = Bun.spawnSync(["node", "-p", "process.features.typescript"]);
  const mode = probe.stdout.toString().trim();

  return probe.exitCode === 0 && (mode === "strip" || mode === "transform");
})();

let child: ReturnType<typeof spawnHost> | undefined;

afterEach(() => {
  child?.process.kill();
  child = undefined;
});

const spawnHost = () => {
  const process = Bun.spawn(["node", main], { stdin: "pipe", stdout: "pipe", stderr: "inherit" });
  const reader = process.stdout.pipeThrough(new TextDecoderStream()).getReader();
  let buffered = "";

  /** Reads stdout until a line starts with `prefix`, and returns that line. */
  const line = async (prefix: string): Promise<string> => {
    for (;;) {
      const found = Array.findFirst(buffered.split("\n"), (entry) => entry.startsWith(prefix));

      if (Option.isSome(found)) return found.value;
      const { value, done } = await reader.read();

      if (done) throw new Error(`node exited before "${prefix}": ${buffered}`);
      buffered += value;
    }
  };

  return { process, line };
};

it.skipIf(!nodeRunsTypeScript)(
  "the Node listener answers 503 before the routes serve, then serves",
  async () => {
    child = spawnHost();
    const port = Number((await child.line("listening ")).split(" ")[1]);
    const url = `http://127.0.0.1:${port}/`;

    const early = await fetch(url, { signal: AbortSignal.timeout(2000) });
    expect(early.status).toBe(503);
    expect(early.headers.get("retry-after")).toBe("1");
    expect(await early.text()).toBe("service unavailable");

    await child.process.stdin.write("serve\n");
    await child.process.stdin.flush();
    await child.line("serving");

    const served = await fetch(url, { signal: AbortSignal.timeout(2000) });
    expect(served.status).toBe(200);
    expect(await served.text()).toBe("served");
  },
  15_000,
);
