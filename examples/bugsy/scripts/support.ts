import { spawnServer } from "@streamsy/test-support/server";
import { Effect, FileSystem, Path, Schema } from "effect";
import { HttpBody, HttpClient, HttpClientResponse } from "effect/http";
import { ProjectBoardCard } from "../domain/outputs.ts";
import { ServerConfig, storageChoice } from "../server/config.ts";

export class SmokeFailure extends Schema.TaggedError<SmokeFailure>()("SmokeFailure", {
  message: Schema.String,
}) {}

export const check = (condition: boolean, message: string) =>
  condition ? Effect.void : Effect.fail(new SmokeFailure({ message }));

/** A SQLite path in a temporary directory that the scope removes. */
export const scratchDatabase = (prefix: string) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: `${prefix}-` });
    return path.join(directory, "tracker.sqlite");
  });

/**
 * The database that a spawned server uses, as a name for the smoke output. The
 * server reads the same config: `STREAMSY_PG_URL` or `STREAMSY_MYSQL_URL` when
 * set, else the SQLite file. Both URLs fail here, before any server starts.
 */
export const serverDatabase = Effect.gen(function* () {
  const choice = yield* storageChoice(yield* ServerConfig);
  return choice.dialect === "pg" ? "Postgres" : choice.dialect === "mysql" ? "MySQL" : "SQLite";
});

/**
 * Starts the server on `database`, a SQLite file. With a database URL set,
 * the server uses that database instead, and a restart finds the same data.
 * Pass the old port to restart it at the same URL.
 */
export const startServer = (database: string, port?: number) =>
  Effect.gen(function* () {
    const path = yield* Path.Path;
    return yield* spawnServer({
      cwd: path.resolve(import.meta.dir, ".."),
      entry: "server/index.ts",
      env: { BUGSY_DB: database, BUGSY_WORKSPACES: "acme,live" },
      port,
      isReady: (status) => status === 200,
    });
  });

const ok = (method: string, url: string) => (response: HttpClientResponse.HttpClientResponse) =>
  response.status >= 200 && response.status < 300
    ? Effect.succeed(response)
    : Effect.flatMap(response.text, (text) =>
        Effect.fail(new SmokeFailure({ message: `${method} ${url}: ${response.status} ${text}` })),
      );

/** GET a JSON body, fail on a non-2xx status, and decode it with `schema`. */
export const getJson = <A, I, RD>(url: string, schema: Schema.Codec<A, I, RD>) =>
  HttpClient.get(url).pipe(
    Effect.flatMap(ok("GET", url)),
    Effect.flatMap(HttpClientResponse.schemaBodyJson(schema)),
  );

type JsonValue =
  | string
  | number
  | boolean
  | null
  | ReadonlyArray<JsonValue>
  | { readonly [key: string]: JsonValue };

/** POST a JSON body and fail on a non-2xx status. */
export const post = (url: string, body: JsonValue) =>
  HttpClient.post(url, { body: HttpBody.jsonUnsafe(body) }).pipe(
    Effect.flatMap(ok("POST", url)),
    Effect.asVoid,
  );

const BoardPage = Schema.Array(
  Schema.Struct({
    key: Schema.String,
    value: Schema.optionalKey(ProjectBoardCard),
    headers: Schema.Struct({ operation: Schema.Literals(["upsert", "delete"]) }),
  }),
);

/** Reconstruct the board by replaying every retained page, starting at -1. */
export const readBoard = (baseUrl: string, workspaceId: string) =>
  Effect.gen(function* () {
    const rows = new Map<string, ProjectBoardCard>();
    let offset = "-1";
    for (;;) {
      const url = `${baseUrl}/state/workspaces/${workspaceId}/issues?offset=${offset}`;
      const response = yield* HttpClient.get(url).pipe(Effect.flatMap(ok("GET", url)));
      const changes = yield* HttpClientResponse.schemaBodyJson(BoardPage)(response);
      for (const change of changes) {
        if (change.headers.operation === "delete") rows.delete(change.key);
        else if (change.value !== undefined) rows.set(change.key, change.value);
      }
      if (response.headers["stream-up-to-date"] !== undefined) return { rows: [...rows.values()] };
      const next = response.headers["stream-next-offset"];
      if (next === undefined || next === offset) {
        return yield* new SmokeFailure({ message: "board replay made no progress" });
      }
      offset = next;
    }
  });
