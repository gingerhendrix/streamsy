/**
 * The CLI seam. `inspect` is the interesting one: reading durable agent state
 * must work in a separate process with no provider credentials at all.
 */
import { BunServices } from "@effect/platform-bun";
import { FileSystem } from "effect";
import { Layer } from "effect";
import { layer } from "@effect/vitest";
import { describe, expect } from "vitest";

import { Path } from "effect";
import { SessionId, startSession } from "@humanlayer/fold-core";
import { Array, Effect, Schema } from "effect";
import { exampleAgent } from "../src/agent.ts";
import { sessionStreamId } from "../src/session-log.ts";
import { sqliteLayer } from "../src/storage.ts";
import { makeEventLog, readFoldLog, streamsyEventLog } from "../src/streamsy-event-log.ts";
import { scriptedModel, textTurn, toolCallTurn } from "./fixtures/scripted-model.ts";

const cli = Bun.fileURLToPath(new URL("../src/cli.ts", import.meta.url));

const worker = Bun.fileURLToPath(new URL("./support/sqlite-process-worker.ts", import.meta.url));

const runCli = (args: ReadonlyArray<string>, env: Record<string, string>) =>
  Effect.gen(function* () {
    const proc = Bun.spawn(["bun", "run", cli, ...args], {
      env: { ...process.env, ...env },
      stdout: "pipe",
      stderr: "pipe",
    });

    const [stdout, stderr, exitCode] = yield* Effect.promise(() =>
      Promise.all([
        new Response(proc.stdout).text(),
        new Response(proc.stderr).text(),
        proc.exited,
      ]),
    );

    return { stdout, stderr, exitCode };
  });

const runWorker = (args: ReadonlyArray<string>) =>
  Effect.gen(function* () {
    const proc = Bun.spawn(["bun", "run", worker, ...args], {
      stdout: "pipe",
      stderr: "pipe",
    });

    const [stdout, stderr, exitCode] = yield* Effect.promise(() =>
      Promise.all([
        new Response(proc.stdout).text(),
        new Response(proc.stderr).text(),
        proc.exited,
      ]),
    );

    return { stdout, stderr, exitCode };
  });

const withTempDir = <A, E, R>(run: (dir: string) => Effect.Effect<A, E, R>) =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const dir = yield* fs.makeTempDirectoryScoped({ prefix: "fold-streamsy-cli-" });

      return yield* run(dir);
    }),
  );

/** Step 2 SQLite gate: seed a real durable session so `inspect` has something honest to read. */
const seedSession = (filename: string, sessionId: SessionId) =>
  Effect.scoped(
    Effect.gen(function* () {
      const scripted = yield* scriptedModel([
        toolCallTurn("provider-call-1", "text_stats", { text: "hello" }),
        textTurn("Five characters."),
      ]);

      const session = yield* startSession({
        agent: exampleAgent(scripted.model),
        log: yield* streamsyEventLog({ session: sessionId, mode: "create" }),
        sessionId,
      });

      yield* session.send("How long is 'hello'?");
    }),
  ).pipe(withLayer(sqliteLayer(filename)));

layer(BunServices.layer, { excludeTestServices: true })((it) => {
  describe("fold-agent CLI", () => {
    it.effect(
      "inspects a seeded SQLite session without provider credentials",
      () =>
        withTempDir((dir) =>
          Effect.gen(function* () {
            const path = yield* Path.Path;
            const filename = path.join(dir, "agent.sqlite");
            const sessionId = SessionId.create();
            const streamId = sessionStreamId(sessionId);
            yield* seedSession(filename, sessionId);

            const result = yield* runCli(["inspect", streamId], {
              FOLD_AGENT_DB: filename,
              OPENAI_API_KEY: "",
              ANTHROPIC_API_KEY: "",
            });

            expect(result.exitCode).toBe(0);
            expect(result.stdout).toContain("session_started");
            expect(result.stdout).toContain("text_stats");
            expect(result.stdout).toContain("agent-finished");
            expect(result.stdout).toContain(`stream id: ${streamId}`);
          }),
        ).pipe(Effect.asVoid),
      60_000,
    );

    it.effect(
      "runs start, resume and inspect as separate CLI processes over FOLD_AGENT_DB",
      () =>
        withTempDir((dir) =>
          Effect.gen(function* () {
            const path = yield* Path.Path;
            const filename = path.join(dir, "agent.sqlite");

            const common = {
              FOLD_AGENT_DB: filename,
              OPENAI_API_KEY: "",
              ANTHROPIC_API_KEY: "",
            };

            const first = yield* runCli(["start", "How long is hello?"], {
              ...common,
              FOLD_AGENT_SCRIPT: yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Json))([
                { type: "tool", id: "cli-call-1", name: "text_stats", params: { text: "hello" } },
                { type: "text", text: "Five characters." },
              ]),
            });

            expect(first.exitCode).toBe(0);
            const streamId = /^stream id:\s+(.+)$/m.exec(first.stdout)?.[1];
            expect(streamId).toMatch(/^fold\/sessions\//);

            if (streamId === undefined) throw new Error(`missing stream id: ${first.stdout}`);

            const second = yield* runCli(["resume", streamId, "What happened?"], {
              ...common,
              FOLD_AGENT_SCRIPT: yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Json))([
                { type: "text", text: "The tool counted hello." },
              ]),
            });

            expect(second.exitCode).toBe(0);
            expect(second.stdout).toContain("The tool counted hello.");

            const inspected = yield* runCli(["inspect", streamId], {
              ...common,
              FOLD_AGENT_SCRIPT: "",
            });

            expect(inspected.exitCode).toBe(0);
            expect(inspected.stdout).toContain("text_stats");
            expect(inspected.stdout).toContain("The tool counted hello.");
          }),
        ).pipe(Effect.asVoid),
      60_000,
    );

    it.effect(
      "prints a usage error or a command failure once, with exit code 1",
      () =>
        Effect.gen(function* () {
          const usage = yield* runCli(["inspect", "not-a-session"], {});
          expect(usage.exitCode).toBe(1);
          expect(usage.stdout).toContain("fold-agent inspect [flags] <stream-id>");
          expect(usage.stderr).toContain(
            'Invalid value for argument <stream-id>: "not-a-session". Expected: fold/sessions/<session>/events',
          );

          const missing = yield* runCli(["start", "hello"], {
            OPENAI_API_KEY: "",
            ANTHROPIC_API_KEY: "",
            FOLD_AGENT_SCRIPT: "",
          });

          expect(missing.exitCode).toBe(1);
          expect(missing.stdout).toBe("");
          expect(missing.stderr.trim()).toBe(
            "ERROR\n  No provider credentials found. Set OPENAI_API_KEY (optionally FOLD_AGENT_MODEL) or ANTHROPIC_API_KEY.",
          );
        }),
      60_000,
    );
  });

  describe("Fold retained-file process recovery", () => {
    it.effect(
      "a second process that resumes the same session fences the first on its next append",
      () =>
        withTempDir((dir) =>
          Effect.gen(function* () {
            const path = yield* Path.Path;
            const filename = path.join(dir, "agent.sqlite");
            const session = SessionId.create();
            expect((yield* runWorker(["create", filename, session])).exitCode).toBe(0);
            const store = yield* Layer.build(sqliteLayer(filename));

            const first = yield* Effect.scoped(makeEventLog({ session, mode: "resume" })).pipe(
              Effect.provideContext(store),
            );

            const second = yield* runWorker(["append", filename, session, "second process"]);
            expect(second.exitCode).toBe(0);

            const failure = yield* Effect.flip(
              first.append({
                _tag: "session_title",
                agentId: null,
                parentAgentId: null,
                toolCallId: null,
                title: "first process is stale",
              }),
            );

            expect(failure.message).toContain("Fenced: another writer advanced the log");
            expect(
              Array.map(
                yield* readFoldLog(session).pipe(Effect.provideContext(store)),
                (row) => row.seq,
              ),
            ).toEqual([0, 1]);
          }),
        ).pipe(Effect.asVoid),
      60_000,
    );
  });
});

/** Build local fixtures in a scope and supply their Context to the program. */
const withLayer =
  <ROut, LE, RIn>(fixture: Layer.Layer<ROut, LE, RIn>) =>
  <A, E, R>(program: Effect.Effect<A, E, R>) =>
    Effect.scoped(
      Effect.gen(function* () {
        const services = yield* Layer.build(fixture);

        return yield* program.pipe(Effect.provideContext(services));
      }),
    );
