import { compare } from "@streamsy/core";
/* oxlint-disable effecttsgo/strict-effect-provide -- Test entry points supply the child process and HTTP services. */
/* oxlint-disable effecttsgo/global-fetch -- The browser transport probes forward native requests and observe their abort signals. */
import { BunServices } from "@effect/platform-bun";
import { BrowserRuntime } from "@effect/platform-browser";
import {
  decodeStateEvents,
  issueStatuses,
} from "@streamsy/issue-tracker-demo-contract/state-schema";
import {
  Written,
  type CommentInput,
  type IssueInput,
  type ProjectInput,
} from "@streamsy/issue-tracker-demo-contract/api-schema";
import { spawnServer } from "@streamsy/test-support/server";
import {
  Array,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Option,
  Predicate,
  Record,
  Ref,
  Scope,
  Stream,
} from "effect";
import { FetchHttpClient, HttpClient, HttpClientRequest, HttpClientResponse } from "effect/http";
import {
  all,
  expect as check,
  expectAll,
  given,
  nth,
  scene,
  selector,
  text,
  within,
} from "foldkit/scene";
import { afterEach, beforeAll, afterAll, expect, test, vi } from "vitest";
import { SendWrite } from "../src/command.ts";
import { Intent } from "../src/domain/intent.ts";
import { Message } from "../src/message.ts";
import { emptyRows, workspaceView } from "../src/domain/workspace.ts";
import type { Model } from "../src/model.ts";
import { workspaceMessages } from "../src/subscription.ts";
import { init, update } from "../src/update.ts";
import { view } from "../src/view/view.ts";
import {
  applyChanges,
  commentsOn,
  issueCountByProject,
  issuesIn,
  statusCounts,
  visibleIssues,
} from "./selectors.ts";
import { stamp } from "./support.ts";

const nativeFetch = globalThis.fetch;

const serverScope = Scope.makeUnsafe();

let origin: string;

beforeAll(
  () =>
    spawnServer({
      cwd: "../issue-tracker-demo",
      entry: "server/index.ts",
      env: { NODE_ENV: "production", ISSUE_TRACKER_DB: ":memory:" },
    }).pipe(
      Effect.tap((server) =>
        Effect.sync(() => {
          origin = server.url;
        }),
      ),
      Scope.provide(serverScope),
      Effect.provide([BunServices.layer, FetchHttpClient.layer]),
      Effect.runPromise,
    ),
  15_000,
);

afterAll(() => Effect.runPromise(Scope.close(serverScope, Exit.void)), 15_000);

afterEach(() => vi.unstubAllGlobals());

const requestUrl = (input: RequestInfo | URL) =>
  new URL(Predicate.isString(input) ? input : input instanceof URL ? input.href : input.url);

const startModel = (workspaceId: string) => init({ workspaceId, displayName: "Ada" }).model;

const matchesModel = (model: Ref.Ref<Model>, ...checks: ReadonlyArray<(value: Model) => void>) =>
  Effect.promise(() =>
    vi.waitFor(
      () => {
        const value = Effect.runSync(Ref.get(model));

        for (const assert of checks) assert(value);

        return value;
      },
      { timeout: 3000 },
    ),
  );

const consume = (workspaceId: string, model: Ref.Ref<Model>) =>
  workspaceMessages(workspaceId, `${origin}/streams`).pipe(
    Stream.runForEach((message) => Ref.update(model, (current) => update(current, message).model)),
  );

const live = (model: Model) => expect(model.sync._tag).toBe("Live");

test("replays the workspace and matches the old selectors after every API write", () =>
  Effect.gen(function* () {
    const client = yield* HttpClient.HttpClient;
    const model = yield* Ref.make(startModel("main"));
    yield* consume("main", model).pipe(Effect.forkScoped);
    yield* matchesModel(model, live);

    const verify = Effect.gen(function* () {
      const response = yield* client.get(`${origin}/streams/workspace/main?offset=-1`, {
        headers: { "cache-control": "no-cache" },
      });

      expect(response.headers["stream-up-to-date"]).toBe("true");

      const events = yield* response.json.pipe(
        Effect.map(decodeStateEvents),
        Effect.flatMap(Effect.fromResult),
      );

      const rows = applyChanges(emptyRows, events);
      const original = workspaceView(rows, []);

      const state = yield* matchesModel(model, (actual) => {
        expect(actual.rows).toEqual(rows);
        expect(actual.counts.issueCountByProject).toEqual(issueCountByProject(original));

        const expectedGroups = Array.flatMap(Record.keys(rows.projects), (projectId) => {
          const counts = statusCounts(issuesIn(original, projectId));

          return Array.flatMap(issueStatuses, (status) =>
            counts[status] === 0 ? [] : [{ projectId, status, count: counts[status] }],
          );
        });

        expect(actual.counts.statusCounts).toHaveLength(expectedGroups.length);
        expect(actual.counts.statusCounts).toEqual(expect.arrayContaining(expectedGroups));
      });

      for (const filter of ["all", ...issueStatuses] as const) {
        const selected = { ...state, filter, maybeSelectedProjectId: Option.some("proj_views") };
        const issues = visibleIssues(issuesIn(original, "proj_views"), filter);
        scene(
          { update, view },
          given(selected),
          ...Array.flatMap(issues, ({ row }, index) => {
            const article = nth(all.selector(".issue-row"), index);
            const comments = commentsOn(original, row.id);

            return [
              check(within(article, selector(".issue-title-button"))).toHaveText(row.title),
              ...Array.map(comments, ({ row: comment }, commentIndex) =>
                check(within(article, nth(all.selector(".comment"), commentIndex))).toContainText(
                  comment.body,
                ),
              ),
            ];
          }),
          expectAll(all.selector(".issue-row")).toHaveCount(issues.length),
        );
      }
    });

    const write = (
      method: "POST" | "PATCH",
      path: string,
      body: ProjectInput | IssueInput | CommentInput,
    ) =>
      client
        .execute(
          HttpClientRequest.make(method)(`${origin}/api/w/main/${path}`).pipe(
            HttpClientRequest.bodyJsonUnsafe(body),
          ),
        )
        .pipe(
          Effect.flatMap(HttpClientResponse.filterStatusOk),
          Effect.tap(() => verify),
        );

    yield* write("POST", "projects", { id: "proj_views", name: "Views" });
    const txid = "00000000-0000-4000-8000-000000000008";
    const intent = Intent.CreateIssue({ projectId: "proj_views", title: "Replay" });

    const pending = yield* Ref.updateAndGet(
      model,
      (current) =>
        update(current, Message.CompletedGenerateStamp({ intent, stamp: stamp(txid) })).model,
    );

    scene(
      { update, view },
      given({ ...pending, maybeSelectedProjectId: Option.some("proj_views") }),
      check(selector(".issue-row.pending .issue-title-button")).toHaveText("Replay"),
      check(selector(".filter-chip.selected .filter-count")).toHaveText("0"),
    );

    const written = yield* write("POST", "issues", {
      id: "issue_abcdef12",
      projectId: "proj_views",
      title: "Replay",
      txid,
    });

    const receipt = yield* HttpClientResponse.schemaBodyJson(Written)(written);
    yield* Ref.update(
      model,
      (current) => update(current, Message.SucceededSendWrite(receipt)).model,
    );
    yield* matchesModel(model, (current) => expect(current.pending).toHaveLength(0));
    yield* write("POST", "issues", {
      id: "issue_views_b",
      projectId: "proj_views",
      title: "Steady state",
    });
    yield* write("PATCH", "issues/issue_abcdef12", { status: "done" });
    yield* write("POST", "comments", {
      id: "comment_views_a",
      issueId: "issue_abcdef12",
      body: "First",
    });
    yield* write("POST", "comments", {
      id: "comment_views_b",
      issueId: "issue_abcdef12",
      body: "Second",
    });
  }).pipe(Effect.scoped, Effect.provide(FetchHttpClient.layer), Effect.runPromise));

test("waits for caughtUp and aborts the parked requests when the page leaves", () =>
  Effect.gen(function* () {
    const model = yield* Ref.make(startModel("main"));
    const closed = yield* Deferred.make();
    let active = 0;
    let aborted = 0;
    const releaseHead = yield* Deferred.make();
    // oxlint-disable-next-line effecttsgo/run-effect-inside-effect -- The native fetch callback waits on this promise.
    const firstHead = Effect.runPromise(Deferred.await(releaseHead));

    vi.stubGlobal("fetch", (input: RequestInfo | URL, options?: RequestInit) => {
      if (options?.method === "HEAD") return firstHead.then(() => nativeFetch(input, options));

      if (requestUrl(input).searchParams.get("live") !== "long-poll")
        return nativeFetch(input, options);

      active++;

      return nativeFetch(input, options).finally(() => {
        active--;

        if (options?.signal?.aborted) aborted++;
      });
    });

    BrowserRuntime.runMain(consume("main", model), {
      teardown: () => {
        Deferred.doneUnsafe(closed, Effect.void);
      },
    });

    const leave = Effect.sync(() => {
      globalThis.dispatchEvent(new Event("pagehide"));
    });

    yield* Effect.gen(function* () {
      const loaded = yield* matchesModel(model, (actual) => {
        expect(Record.size(actual.rows.projects)).toBeGreaterThan(0);
        expect(actual.sync._tag).toBe("Loading");
      });

      scene(
        { update, view },
        given(loaded),
        check(text("Loading durable stream database…")).toExist(),
      );

      yield* Deferred.succeed(releaseHead, undefined);
      yield* matchesModel(model, live);
      yield* Effect.promise(() => vi.waitFor(() => expect(active).toBe(1)));
      yield* leave;
      yield* Deferred.await(closed).pipe(Effect.timeout("3 seconds"));
      yield* Effect.promise(() => vi.waitFor(() => expect(active).toBe(0)));
      expect(aborted).toBe(1);
    }).pipe(Effect.ensuring(Effect.andThen(Deferred.succeed(releaseHead, undefined), leave)));
  }).pipe(Effect.runPromise));

test.each([
  { method: "HEAD", error: "Input stream workspace/main is missing" },
  {
    method: "GET",
    error:
      "Required history of workspace/main after 0000000000000000_0000000000000000 is unavailable",
  },
])("renders a terminal View.live $method failure in the page", ({ method, error }) =>
  Effect.gen(function* () {
    vi.stubGlobal("fetch", (input: RequestInfo | URL, options?: RequestInit) => {
      const isViewReplay =
        requestUrl(input).searchParams.get("offset") === "0000000000000000_0000000000000000";

      return options?.method === method && (method === "HEAD" || isViewReplay)
        ? Promise.resolve(new Response(null, { status: 404 }))
        : nativeFetch(input, options);
    });
    const model = yield* Ref.make(startModel("main"));
    const reader = yield* consume("main", model).pipe(Effect.forkScoped);

    const failed = yield* matchesModel(model, (actual) => {
      expect(actual.sync).toEqual({ _tag: "Failed", error });
    });

    scene({ update, view }, given(failed), check(text(error)).toExist());
    yield* Fiber.join(reader);
  }).pipe(Effect.scoped, Effect.runPromise),
);

test.each([
  { order: "response first", suffix: "face0001", responseFirst: true },
  { order: "snapshot first", suffix: "face0002", responseFirst: false },
])("confirms a write over HTTP with the $order", ({ order, suffix, responseFirst }) =>
  Effect.gen(function* () {
    const client = yield* HttpClient.HttpClient;
    const model = yield* Ref.make(startModel("main"));
    const release = yield* Deferred.make<void>();
    const issueId = `issue_${suffix}`;
    const title = `Confirm with ${order}`;

    const apply = (message: Message) =>
      Ref.update(model, (current) => update(current, message).model);

    yield* workspaceMessages("main", `${origin}/streams`).pipe(
      Stream.tap((message) =>
        responseFirst && message._tag === "ReceivedViews" && message.rows.issues[issueId]
          ? Deferred.await(release)
          : Effect.void,
      ),
      Stream.runForEach(apply),
      Effect.forkScoped,
    );
    yield* matchesModel(model, live);

    const txid = `${suffix}-0000-4000-8000-000000000001`;
    const intent = Intent.CreateIssue({ projectId: "proj_streamsy", title });
    yield* apply(
      Message.CompletedGenerateStamp({
        intent,
        stamp: { ...stamp(txid), entropy: txid },
      }),
    );
    const pending = (yield* Ref.get(model)).pending[0];

    const commandClient = client.pipe(
      HttpClient.mapRequest(HttpClientRequest.prependUrl(origin)),
      HttpClient.tap(() => (responseFirst ? Effect.void : Deferred.await(release))),
    );

    const send = yield* SendWrite({ workspaceId: "main", pending }).effect.pipe(
      Effect.provideService(HttpClient.HttpClient, commandClient),
      Effect.forkScoped,
    );

    const applyResponse = Effect.gen(function* () {
      const response = yield* Fiber.join(send);
      expect(response._tag).toBe("SucceededSendWrite");

      if (response._tag !== "SucceededSendWrite") return yield* Effect.die(response.error);

      yield* apply(response);

      return response.awaitOffset;
    });

    const assertShown = (current: Model, isPending: boolean) => {
      const shown = Array.filter(
        workspaceView(current.rows, current.pending).issues,
        ({ row }) => row.id === issueId,
      );

      expect(shown).toEqual([{ row: expect.objectContaining({ title }), isPending }]);
    };

    const awaitOffset = yield* Effect.gen(function* () {
      if (responseFirst) {
        const offset = yield* applyResponse;
        const current = yield* Ref.get(model);
        expect(current.rows.issues[issueId]).toBeUndefined();
        expect(current.pending[0]?.awaitOffset).toBe(offset);
        assertShown(current, true);
        yield* Deferred.succeed(release, undefined);

        return offset;
      }

      const current = yield* matchesModel(model, (value) => {
        expect(value.rows.issues[issueId]?.title).toBe(title);
      });

      expect(current.pending[0]?.awaitOffset).toBeUndefined();
      assertShown(current, true);
      yield* Deferred.succeed(release, undefined);

      return yield* applyResponse;
    });

    yield* matchesModel(model, (current) => {
      expect(compare(current.offset, awaitOffset)).toBeGreaterThanOrEqual(0);
      expect(current.pending).toHaveLength(0);
      assertShown(current, false);
    });
  }).pipe(Effect.scoped, Effect.provide(FetchHttpClient.layer), Effect.runPromise),
);
