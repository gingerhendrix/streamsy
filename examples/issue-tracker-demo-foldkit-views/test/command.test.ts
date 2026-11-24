import { Effect } from "effect";
import { HttpClient, HttpClientResponse } from "effect/http";
import { expect, test } from "vitest";
import { SendWrite } from "../src/command.ts";
import { Intent } from "../src/domain/intent.ts";
import { Message } from "../src/message.ts";
import { init, update } from "../src/update.ts";
import { stamp } from "./support.ts";

const server = (status: number, body: string) =>
  HttpClient.make((request) =>
    Effect.succeed(HttpClientResponse.fromWeb(request, new Response(body, { status }))),
  );

const sent = (status: number, body: string) =>
  Effect.gen(function* () {
    const stamped = update(
      init({ workspaceId: "main", displayName: "Ada" }).model,
      Message.CompletedGenerateStamp({
        intent: Intent.CreateIssue({ projectId: "p1", title: "Ship" }),
        stamp: stamp("tx-1"),
      }),
    ).model;

    const pending = stamped.pending[0];
    expect(pending).toBeDefined();

    if (pending === undefined) return yield* Effect.die("Expected a pending write");

    const answer = yield* SendWrite({ workspaceId: "main", pending }).effect.pipe(
      Effect.provideService(HttpClient.HttpClient, server(status, body)),
    );

    return update(stamped, answer).model;
  });

test.each(["not JSON", "{}", ""])(
  "an accepted write with the body %j leaves no pending row and no notice",
  (body) =>
    Effect.gen(function* () {
      const model = yield* sent(201, body);
      expect(model.pending).toEqual([]);
      expect(model.notices).toEqual([]);
    }).pipe(Effect.runPromise),
);

test("a refused write with an undecodable body leaves a notice", () =>
  Effect.gen(function* () {
    const model = yield* sent(500, "not JSON");
    expect(model.pending).toEqual([]);
    expect(model.notices).toEqual([{ id: 0, message: "Request failed with status 500" }]);
  }).pipe(Effect.runPromise));
