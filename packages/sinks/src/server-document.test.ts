/* oxlint-disable anti-slop/no-unknown-parameters -- The test codec exercises the sink's external wire boundary. */
import { describe, expect, test } from "bun:test";
import { Effect } from "effect";
import { defineDocumentSink } from "./document.ts";
import { DOCUMENT_SINK_ERROR_TAGS } from "./errors.ts";
import { DOCUMENT_SINK_CONTRACT_HEADER } from "./protocol.ts";
import type { CanonicalValue } from "./fingerprint.ts";
import { DocumentSinkSourceFailure, handleDocumentSink } from "./server-document.ts";

interface Summary {
  readonly workspaceId: string;
  readonly issues: number;
}

const sink = defineDocumentSink({
  name: "test.workspace-summary",
  from: [{ name: "test.issues" }],
  document: {
    decode: (value: unknown): Summary => {
      if (!(value instanceof Object) || !("workspaceId" in value) || !("issues" in value)) {
        throw new Error("not a summary");
      }
      return { workspaceId: String(value.workspaceId), issues: Number(value.issues) };
    },
  },
  route: "/document/:workspaceId/summary",
  params: { workspaceId: { decode: (value: string) => value } },
  cache: { visibility: "private", maxAgeSeconds: 0, mustRevalidate: true },
  errors: DOCUMENT_SINK_ERROR_TAGS,
});

const serve = (
  request: Request,
  document: (params: {
    readonly workspaceId: string;
  }) => Effect.Effect<CanonicalValue, DocumentSinkSourceFailure>,
): Effect.Effect<Response> => handleDocumentSink(sink, request, { document });

const summary = (issues: number): CanonicalValue => ({ issues, workspaceId: "main" });
const get = (headers: HeadersInit = {}, method = "GET"): Request =>
  new Request("http://host/document/main/summary", { method, headers });

describe("handleDocumentSink", () => {
  test("serves the canonical document with the declared cache policy", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const response = yield* serve(get(), () => Effect.succeed(summary(2)));
        expect(response.status).toBe(200);
        expect(yield* Effect.promise(() => response.text())).toBe(
          '{"issues":2,"workspaceId":"main"}',
        );
        expect(response.headers.get("cache-control")).toBe("private, max-age=0, must-revalidate");
        expect(response.headers.get("etag")).toMatch(/^"[0-9a-f]{16}"$/);
        expect(response.headers.get(DOCUMENT_SINK_CONTRACT_HEADER)).toBe(sink.fingerprint);
      }),
    ));

  test("the entity tag is stable for an unchanged document and moves when it changes", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const first = yield* serve(get(), () => Effect.succeed(summary(2)));
        const again = yield* serve(get(), () => Effect.succeed({ workspaceId: "main", issues: 2 }));
        const changed = yield* serve(get(), () => Effect.succeed(summary(3)));
        expect(again.headers.get("etag")).toBe(first.headers.get("etag"));
        expect(changed.headers.get("etag")).not.toBe(first.headers.get("etag"));
      }),
    ));

  test("a conditional request on the current tag is 304 with no body", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const first = yield* serve(get(), () => Effect.succeed(summary(2)));
        const etag = first.headers.get("etag") ?? "";
        const conditional = yield* serve(get({ "if-none-match": etag }), () =>
          Effect.succeed(summary(2)),
        );
        expect(conditional.status).toBe(304);
        expect(yield* Effect.promise(() => conditional.text())).toBe("");
        expect(conditional.headers.get("etag")).toBe(etag);
        expect(conditional.headers.get("cache-control")).toBe(sink.cacheControl);

        const weak = yield* serve(get({ "if-none-match": `W/${etag}` }), () =>
          Effect.succeed(summary(2)),
        );
        expect(weak.status).toBe(304);

        const stale = yield* serve(get({ "if-none-match": etag }), () =>
          Effect.succeed(summary(3)),
        );
        expect(stale.status).toBe(200);
      }),
    ));

  test("HEAD reports the validator without the body", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const response = yield* serve(get({}, "HEAD"), () => Effect.succeed(summary(2)));
        expect(response.status).toBe(200);
        expect(yield* Effect.promise(() => response.text())).toBe("");
        expect(response.headers.get("etag")).toMatch(/^"[0-9a-f]{16}"$/);
      }),
    ));

  test("a document the declared schema rejects is a typed failure, never a cached wrong answer", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const response = yield* serve(get(), () => Effect.succeed({ workspaceId: "main" }));
        expect(response.status).toBe(500);
        expect(yield* Effect.promise(() => response.json())).toMatchObject({
          _tag: "WireDecodeFailed",
        });
        expect(response.headers.get("cache-control")).toBe("no-store");
      }),
    ));

  test("an unavailable document and a retired contract are typed and uncacheable", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const unavailable = yield* serve(get(), () =>
          Effect.fail(new DocumentSinkSourceFailure({ detail: "store down" })),
        );
        expect(unavailable.status).toBe(503);
        expect(yield* Effect.promise(() => unavailable.json())).toMatchObject({
          _tag: "DocumentUnavailable",
        });

        const retired = yield* serve(get({ [DOCUMENT_SINK_CONTRACT_HEADER]: "retired" }), () =>
          Effect.succeed(summary(2)),
        );
        expect(retired.status).toBe(409);
        expect(yield* Effect.promise(() => retired.json())).toEqual({
          _tag: "ContractChanged",
          sink: "test.workspace-summary",
          recovery: "refetch",
        });
      }),
    ));

  test("a path outside the checked route never reaches the capability", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const response = yield* handleDocumentSink(
          sink,
          new Request("http://host/document/main/other"),
          { document: () => Effect.die("the capability must not run") },
        );
        expect(response.status).toBe(404);
      }),
    ));
});
