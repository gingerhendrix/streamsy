/* oxlint-disable anti-slop/no-unknown-parameters -- The test codec exercises the sink's external wire boundary. */
import { describe, expect, test } from "bun:test";
import { Effect } from "effect";
import {
  STREAM_SINK_ERROR_TAGS,
  STREAM_SINK_CONTRACT_HEADER,
  STREAM_SINK_VERSION_HEADER,
  defineStreamSink,
} from "../stream.ts";
import { handleStreamSink, StreamSinkSourceFailure, type StreamSinkPage } from "./stream.ts";

interface Transition {
  readonly issueId: string;
  readonly status: string;
}

const sink = defineStreamSink({
  name: "test.issue-transitions",
  from: {
    kind: "change-stream",
    name: "test.issues.changes",
    key: "issueId",
    order: "arrival",
  },
  event: {
    decode: (value: unknown): Transition => {
      if (!(value instanceof Object) || !("issueId" in value) || !("status" in value)) {
        throw new Error("not a transition");
      }
      return { issueId: String(value.issueId), status: String(value.status) };
    },
  },
  route: "/feed/:workspaceId/issue-transitions",
  params: { workspaceId: { decode: (value: string) => value } },
  feed: { name: "issue-transitions", type: "issue-transition" },
  protocol: {
    sessionVersion: 1,
    transport: "durable-stream",
    resume: true,
    order: "arrival",
    fallback: "replay-from-start",
  },
  errors: STREAM_SINK_ERROR_TAGS,
});

const page = (events: readonly unknown[], nextOffset = "3"): StreamSinkPage => ({
  events,
  nextOffset,
  upToDate: true,
});

interface FeedBody {
  readonly order: string;
  readonly events: readonly Transition[];
  readonly nextOffset: string;
  readonly upToDate: boolean;
  readonly feed: { readonly subjectKey: string };
}

const serve = (
  request: Request,
  read: (
    params: { readonly workspaceId: string },
    offset: string | undefined,
  ) => Effect.Effect<StreamSinkPage, StreamSinkSourceFailure>,
): Effect.Effect<Response> => handleStreamSink(sink, request, { read });

const get = (path: string, headers: HeadersInit = {}): Request =>
  new Request(`http://host${path}`, { headers });

describe("handleStreamSink", () => {
  test("serves the page in the order it was read, decoded through the declared schema", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const response = yield* serve(get("/feed/main/issue-transitions"), () =>
          Effect.succeed(
            page([
              { issueId: "b", status: "todo" },
              { issueId: "a", status: "done" },
              { issueId: "b", status: "done" },
            ]),
          ),
        );
        expect(response.status).toBe(200);
        const body: FeedBody = yield* Effect.promise(() => response.json());
        expect(body.order).toBe("arrival");
        expect(body.events.map((event) => `${event.issueId}:${event.status}`)).toEqual([
          "b:todo",
          "a:done",
          "b:done",
        ]);
        expect(body.feed.subjectKey).toBe("issueId");
        expect(response.headers.get("stream-next-offset")).toBe("3");
        expect(response.headers.get(STREAM_SINK_CONTRACT_HEADER)).toBe(sink.fingerprint);
        expect(response.headers.get("cache-control")).toBe("no-store");
      }),
    ));

  test("passes a native offset through, and treats -1 as the start of the feed", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const seen: (string | undefined)[] = [];
        const record = (_params: { readonly workspaceId: string }, offset: string | undefined) => {
          seen.push(offset);
          return Effect.succeed(page([]));
        };
        yield* serve(get("/feed/main/issue-transitions?offset=7"), record);
        yield* serve(get("/feed/main/issue-transitions?offset=-1"), record);
        yield* serve(get("/feed/main/issue-transitions"), record);
        expect(seen).toEqual(["7", undefined, undefined]);
      }),
    ));

  test("an unusable resume position declares replay-from-start", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const response = yield* serve(get("/feed/main/issue-transitions?offset=nope"), () =>
          Effect.fail(new StreamSinkSourceFailure({ reason: "invalid-offset", detail: "bad" })),
        );
        expect(response.status).toBe(409);
        expect(yield* Effect.promise(() => response.json())).toEqual({
          _tag: "ResumeRejected",
          sink: "test.issue-transitions",
          reason: "invalid-offset",
          recovery: "replay-from-start",
        });
      }),
    ));

  test("an unavailable feed is a typed 503, not a silently empty page", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const response = yield* serve(get("/feed/main/issue-transitions"), () =>
          Effect.fail(new StreamSinkSourceFailure({ reason: "unavailable", detail: "gone" })),
        );
        expect(response.status).toBe(503);
        expect(yield* Effect.promise(() => response.json())).toMatchObject({
          _tag: "FeedUnavailable",
          detail: "gone",
        });
      }),
    ));

  test("an event the declared schema rejects fails the read instead of being served", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const response = yield* serve(get("/feed/main/issue-transitions"), () =>
          Effect.succeed(page([{ issueId: "a", status: "todo" }, { nonsense: true }])),
        );
        expect(response.status).toBe(500);
        expect(yield* Effect.promise(() => response.json())).toMatchObject({
          _tag: "WireDecodeFailed",
        });
      }),
    ));

  test("protocol and contract mismatches declare the same recovery", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const version = yield* serve(
          get("/feed/main/issue-transitions", { [STREAM_SINK_VERSION_HEADER]: "2" }),
          () => Effect.succeed(page([])),
        );
        expect(version.status).toBe(409);
        expect(yield* Effect.promise(() => version.json())).toMatchObject({
          _tag: "ProtocolVersionUnsupported",
          supported: 1,
          received: "2",
          recovery: "replay-from-start",
        });

        const contract = yield* serve(
          get("/feed/main/issue-transitions", { [STREAM_SINK_CONTRACT_HEADER]: "retired" }),
          () => Effect.succeed(page([])),
        );
        expect(contract.status).toBe(409);
        expect(yield* Effect.promise(() => contract.json())).toMatchObject({
          _tag: "ResumeRejected",
          reason: "contract-changed",
        });
      }),
    ));

  test("a path outside the checked route never reaches the capability", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        expect((yield* serve(get("/feed/main/other"), failRead)).status).toBe(404);
        expect((yield* serve(get("/feed/a%2Fb/issue-transitions"), failRead)).status).toBe(400);
      }),
    ));
});

const failRead = () => Effect.die("the capability must not run");
