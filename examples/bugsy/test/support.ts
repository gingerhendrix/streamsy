import { Effect, FileSystem, Path, Schedule, Schema } from "effect";
import { HttpClient } from "effect/http";

export { post as postUrl, readBoard } from "../scripts/support.ts";

import { post as postUrl } from "../scripts/support.ts";

export const scratchDirectory = (name: string) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: `${name}-` });

    return { directory, database: path.join(directory, "tracker.sqlite") };
  });

export const startServer = (port: number, database: string) =>
  Bun.spawn(["bun", "server/index.ts"], {
    cwd: Bun.fileURLToPath(new URL("..", import.meta.url)),
    env: { ...process.env, PORT: String(port), BUGSY_DB: database, BUGSY_WORKSPACES: "acme,live" },
    stdout: "pipe",
    stderr: "pipe",
  });

export const stopServer = (process: ReturnType<typeof startServer>) =>
  Effect.gen(function* () {
    process.kill("SIGTERM");
    yield* Effect.promise(() => process.exited);
  });

export const waitForServer = (baseUrl: string) =>
  HttpClient.get(baseUrl).pipe(
    Effect.flatMap((response) => (response.status === 200 ? Effect.void : new ServerStarting({}))),
    Effect.retry(Schedule.spaced("50 millis")),
    Effect.timeout("10 seconds"),
    Effect.asVoid,
  );

export const post = (baseUrl: string, path: string, body: Parameters<typeof postUrl>[1]) =>
  postUrl(`${baseUrl}${path}`, body);

class ServerStarting extends Schema.TaggedError<ServerStarting>()("ServerStarting", {}) {}
