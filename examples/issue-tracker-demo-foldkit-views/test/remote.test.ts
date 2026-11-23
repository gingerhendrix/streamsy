/* oxlint-disable effecttsgo/strict-effect-provide -- Test entry points supply the child process and HTTP services. */
/* oxlint-disable effecttsgo/global-fetch -- The browser transport probes forward native requests and observe their abort signals. */
import { BunServices } from "@effect/platform-bun";
import { BrowserRuntime } from "@effect/platform-browser";
import {
  decodeStateEvents,
  issueStatuses,
} from "@streamsy/issue-tracker-demo-contract/state-schema";
import type {
  CommentInput,
  IssueInput,
  ProjectInput,
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
        .pipe(Effect.flatMap(HttpClientResponse.filterStatusOk), Effect.andThen(verify));

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

    yield* write("POST", "issues", {
      id: "issue_abcdef12",
      projectId: "proj_views",
      title: "Replay",
      txid,
    });
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
      yield* Effect.promise(() => vi.waitFor(() => expect(active).toBe(2)));
      yield* leave;
      yield* Deferred.await(closed).pipe(Effect.timeout("3 seconds"));
      yield* Effect.promise(() => vi.waitFor(() => expect(active).toBe(0)));
      expect(aborted).toBe(2);
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
    // A failing startup HEAD or first Views GET leaves the other reader intact.
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

test("keeps live views and writes available when only confirmation stops", () =>
  Effect.gen(function* () {
    const client = yield* HttpClient.HttpClient;
    const refuseConfirmation = yield* Deferred.make();
    // oxlint-disable-next-line effecttsgo/run-effect-inside-effect -- The native fetch callback waits on this promise.
    const refuse = Effect.runPromise(Deferred.await(refuseConfirmation));

    vi.stubGlobal("fetch", (input: RequestInfo | URL, options?: RequestInit) =>
      requestUrl(input).searchParams.get("offset") === "-1"
        ? refuse.then(() => new Response(null, { status: 404 }))
        : nativeFetch(input, options),
    );

    const model = yield* Ref.make(startModel("main"));
    yield* consume("main", model).pipe(Effect.forkScoped);

    yield* Effect.gen(function* () {
      const ready = yield* matchesModel(model, (actual) => {
        live(actual);
        expect(actual.rows.projects["proj_streamsy"]?.name).toBe("Streamsy Demo");
        expect(actual.counts.issueCountByProject["proj_streamsy"]).toBe(4);
      });

      const intent = Intent.CreateIssue({ projectId: "proj_streamsy", title: "Shows from Views" });
      const txid = "00000000-0000-4000-8000-000000000009";
      const request = Message.CompletedGenerateStamp({ intent, stamp: stamp(txid) });

      const pending = yield* Ref.updateAndGet(model, (current) => update(current, request).model);
      expect(pending.pending).toHaveLength(1);

      yield* Deferred.succeed(refuseConfirmation, undefined);

      const degraded = yield* matchesModel(model, (actual) => {
        live(actual);
        expect(actual.canConfirmWrites).toBe(false);
        expect(actual.rows).toEqual(ready.rows);
        expect(actual.counts).toEqual(ready.counts);
        expect(actual.pending).toHaveLength(0);
      });

      scene(
        { update, view },
        given({ ...degraded, maybeSelectedProjectId: Option.some("proj_streamsy") }),
        check(
          text(
            "Write confirmation stopped. Changes still sync. Reload to restore pending indicators.",
          ),
        ).toExist(),
        expectAll(all.selector(".issue-row")).toHaveCount(4),
        check(selector(".filter-chip.selected .filter-count")).toHaveText("4"),
      );

      const unstaged = update(degraded, request);
      expect(unstaged.model.pending).toHaveLength(0);
      expect(unstaged.commands).toHaveLength(1);

      const body: IssueInput = { projectId: "proj_streamsy", title: "Shows from Views", txid };
      yield* HttpClientRequest.post(`${origin}/api/w/main/issues`).pipe(
        HttpClientRequest.bodyJsonUnsafe(body),
        client.execute,
        Effect.flatMap(HttpClientResponse.filterStatusOk),
      );

      yield* matchesModel(model, (actual) => {
        live(actual);
        expect(actual.canConfirmWrites).toBe(false);
        expect(actual.counts.issueCountByProject["proj_streamsy"]).toBe(5);
        expect(Record.values(actual.rows.issues)).toEqual(
          expect.arrayContaining([expect.objectContaining({ title: body.title })]),
        );
      });
    }).pipe(Effect.ensuring(Deferred.succeed(refuseConfirmation, undefined)));
  }).pipe(Effect.scoped, Effect.provide(FetchHttpClient.layer), Effect.runPromise));
