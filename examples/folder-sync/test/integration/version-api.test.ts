import { BunServices } from "@effect/platform-bun";
import { layer } from "@effect/vitest";
import { expect } from "vitest";
import { Offset, StreamAddress } from "@streamsy/core";
import { spawnServer } from "@streamsy/test-support/server";
import { Effect, Layer, Schedule, Schema } from "effect";
import { FetchHttpClient, HttpBody, HttpClient } from "effect/http";
import { FileOp, Version } from "../../server/domain.ts";
import { Change, ManifestFile } from "../../server/streams.ts";
import { testDirectory } from "../../scripts/test-directory.ts";
import { startWorker } from "../../scripts/worker-harness.ts";

class Waiting extends Schema.TaggedError<Waiting>()("VersionIndexWaiting", {}) {}

const Written = Schema.Struct({ version: Version, overwrote: Schema.optionalKey(Version) });

const History = Schema.Array(Schema.Struct({ version: Version, operation: FileOp }));

const Events = Schema.Array(StreamAddress.stateChange({ type: "file", schema: ManifestFile }));

layer(Layer.mergeAll(BunServices.layer, FetchHttpClient.layer), { excludeTestServices: true })(
  (it) => {
    for (const mode of ["transient", "sqlite", "worker"] as const) {
      it.effect(
        `${mode}: HTTP catch-up batches entries while history, at and replay keep integer versions`,
        () =>
          Effect.gen(function* () {
            const directory = yield* testDirectory("version-api-");
            const token = "version-test-token";

            const host =
              mode === "worker"
                ? yield* Effect.acquireRelease(
                    Effect.promise(() => startWorker(directory, token)),
                    (runtime) => Effect.promise(runtime.stop),
                  )
                : yield* spawnServer({
                    cwd: `${import.meta.dirname}/../..`,
                    entry: "server/index.ts",
                    env: { FOLDER_SYNC_DB: mode === "sqlite" ? `${directory}/files.sqlite` : "" },
                  });

            const headers = {
              authorization: `Bearer ${token}`,
              "x-file-author": "test",
              "x-file-origin": "version-test",
            };

            const created = yield* HttpClient.post(`${host.url}/api/folders`, {
              headers,
              body: yield* HttpBody.json({ folderId: "versions", title: "Versions" }),
            });

            expect(created.status).toBe(200);
            yield* created.text;
            const url = `${host.url}/api/folders/versions/files/a.md`;

            for (let version = 1; version <= 12; version++) {
              const metadata = { ...headers, "x-file-op-id": `op-${version}` };

              const response = yield* HttpClient.put(url, {
                headers:
                  version === 1 ? metadata : { ...metadata, "x-file-base": String(version - 1) },
                body: HttpBody.text(`text-${version}`),
              });

              expect(response.status).toBe(200);
              expect(yield* Schema.decodeUnknownEffect(Written)(yield* response.json)).toEqual({
                version,
              });
            }

            const replay = yield* HttpClient.put(url, {
              headers: { ...headers, "x-file-op-id": "op-1" },
              body: HttpBody.text("replayed"),
            });

            expect(yield* Schema.decodeUnknownEffect(Written)(yield* replay.json)).toEqual({
              version: 1,
            });
            const history = yield* HttpClient.get(`${host.url}/api/folders/versions/history/a.md`);
            const rows = yield* Schema.decodeUnknownEffect(History)(yield* history.json);
            expect(rows.map((row) => row.version)).toEqual(
              Array.from({ length: 12 }, (_, n) => n + 1),
            );
            expect(rows.map((row) => row.operation.version)).toEqual(
              rows.map((row) => row.version),
            );

            for (const version of [1, 9, 10, 12]) {
              const response = yield* HttpClient.get(`${url}?at=${version}&minVersion=12`);
              expect(response.status).toBe(200);
              expect(response.headers["x-file-version"]).toBe(String(version));
              expect(yield* response.text).toBe(`text-${version}`);
            }

            for (const invalid of [
              "0",
              "-1",
              "1.5",
              "9007199254740992",
              "0000000000000001_0000000000000000",
            ]) {
              const response = yield* HttpClient.get(`${url}?at=${invalid}`);
              expect(response.status).toBe(400);
              yield* response.text;

              const write = yield* HttpClient.put(url, {
                headers: {
                  ...headers,
                  "x-file-op-id": `invalid-${invalid}`,
                  "x-file-base": invalid,
                },
                body: HttpBody.text("invalid"),
              });

              expect(write.status).toBe(400);
              yield* write.text;
            }

            // Bun projections are eventual. A file head read does not imply index readiness.
            yield* Effect.gen(function* () {
              const response = yield* HttpClient.get(`${host.url}/api/ls`);

              const files = yield* Schema.decodeUnknownEffect(Schema.Array(ManifestFile))(
                yield* response.json,
              );

              if (files[0]?.version !== 12) return yield* new Waiting();

              return yield* Effect.void;
            }).pipe(
              Effect.retry({
                while: Schema.is(Waiting),
                schedule: Schedule.spaced("10 millis").pipe(Schedule.upTo({ times: 300 })),
              }),
            );
            const changes = yield* HttpClient.get(`${host.url}/streams/changes?offset=-1`);

            const feed = yield* Schema.decodeUnknownEffect(Schema.Array(Change))(
              yield* changes.json,
            );

            expect(feed).toHaveLength(12);
            expect(feed.map((row) => row.version)).toEqual(rows.map((row) => row.version));
            expect(changes.headers["stream-up-to-date"]).toBe("true");
            expect(Schema.is(Offset)(changes.headers["stream-next-offset"])).toBe(true);
            const manifest = yield* HttpClient.get(`${host.url}/streams/manifest?offset=-1`);
            const events = yield* Schema.decodeUnknownEffect(Events)(yield* manifest.json);
            expect(events).toHaveLength(12);
            expect(
              events.flatMap((event) => ("value" in event ? [event.value.version] : [])),
            ).toEqual(rows.map((row) => row.version));
            expect(manifest.headers["stream-up-to-date"]).toBe("true");
            expect(Schema.is(Offset)(manifest.headers["stream-next-offset"])).toBe(true);
          }),
        { timeout: 20000 },
      );
    }
  },
);
