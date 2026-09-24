import { expect, test } from "bun:test";

test("configured stream ids remain literal when a segment starts with a colon", () => {
  const modulePath = new URL("../../src/server/stream-resources.ts", import.meta.url).pathname;
  const child = Bun.spawnSync(
    [
      process.execPath,
      "-e",
      `
    import { HackerNewsSource, HackerNewsTarget } from ${JSON.stringify(modulePath)};
    console.log(JSON.stringify({
      source: HackerNewsSource.at({}).id,
      target: HackerNewsTarget.at({}).id,
      sourceOwnsLiteral: HackerNewsSource.match("session/:literal/source"),
      sourceOwnsOther: HackerNewsSource.match("session/other/source"),
      targetOwnsLiteral: HackerNewsTarget.match("session/:literal/target"),
    }));
  `,
    ],
    {
      env: {
        ...process.env,
        HN_SOURCE_STREAM_ID: "session/:literal/source",
        HN_TARGET_STREAM_ID: "session/:literal/target",
      },
    },
  );
  expect(child.exitCode).toBe(0);
  expect(child.stderr.toString()).toBe("");
  expect(JSON.parse(child.stdout.toString())).toEqual({
    source: "session/:literal/source",
    target: "session/:literal/target",
    sourceOwnsLiteral: true,
    sourceOwnsOther: false,
    targetOwnsLiteral: true,
  });
});
