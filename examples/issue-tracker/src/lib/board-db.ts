/**
 * The caller-owned local database, bound to the `boardIssues` sink.
 *
 * The application constructs the `DurableStream` handle and the `StreamDB`
 * that wraps it, and the application disposes both. `createStreamDB` receives
 * the handle rather than creating one, so connection lifetime, transport
 * options and the fetch used for every request stay owned here.
 *
 * Deviation from the draft, stated precisely: the drafted API was
 * `durableStateCollection(boardIssues, { params, database: durableDb })`, with
 * a caller-supplied local database object. The installed
 * `@durable-streams/state` has no such parameter — the caller-owned object it
 * accepts is the `DurableStream` handle, and it builds the TanStack DB
 * collections itself. This is the nearest honest binding: ownership is still
 * explicit and still the application's, and the sink contract still supplies
 * the route, the scope, the wire tag and the key.
 *
 * Rows are decoded through the declared `IssueRow` schema before they reach the
 * local collection, so a malformed row from the network is rejected rather than
 * rendered.
 */
import { DurableStream } from "@durable-streams/client";
import { createStateSchema } from "@durable-streams/state";
import { createStreamDB, type StreamDB } from "@durable-streams/state/db";
import { Schema } from "effect";
import { boardIssues } from "../../domain/declaration.ts";
import { IssueRow } from "../../domain/issue.ts";
import type { IssueRow as IssueRowType } from "../../domain/issue.ts";

/** Header the sink answers with; mirrored from `server/sink-http.ts`. */
const RESUME_HEADER = "x-streamsy-resume";

/**
 * The consumer half of the sink contract.
 *
 * `type: "issue"` and `primaryKey: "issueId"` are the sink's declared wire tag
 * and key. They are written once, here, and everything downstream is derived.
 */
export const boardCollections = createStateSchema({
  issues: {
    schema: Schema.toStandardSchemaV1(IssueRow),
    type: "issue",
    primaryKey: "issueId",
  },
});

export type BoardDb = StreamDB<typeof boardCollections>;

export type SinkStatus =
  | { readonly kind: "connecting" }
  | { readonly kind: "live"; readonly resume: string | undefined }
  | { readonly kind: "resetting"; readonly reason: string }
  | { readonly kind: "failed"; readonly detail: string };

export interface BoardConnection {
  readonly db: BoardDb;
  readonly preload: () => Promise<void>;
  readonly close: () => void;
}

export interface BoardConnectionOptions {
  readonly workspaceId: string;
  readonly origin: string;
  readonly onStatus: (status: SinkStatus) => void;
}

/**
 * Open one sink session.
 *
 * The fetch wrapper is where the sink's resume contract lives on the client:
 * it presents the newest token when a session starts from the beginning,
 * records the token the sink mints for every response, and — on the sink's
 * typed `resume-expired` answer — drops the token and takes the declared
 * `snapshot-then-live` fallback rather than failing the session.
 */
export function createBoardConnection(options: BoardConnectionOptions): BoardConnection {
  const route = boardIssues.route.replace(":workspaceId", encodeURIComponent(options.workspaceId));
  let resume: string | undefined;

  // Typed as the fetch shape `DurableStream` accepts. Bun's ambient `fetch`
  // type carries extra members the browser does not have, so the wrapper is
  // written against the signature the client actually calls.
  const guardedFetch = async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    // SAFETY: `Request` accepts a URL as well as a `RequestInfo`; the assertion
    // only reconciles the two overloads of the same constructor.
    // oxlint-disable-next-line anti-slop/require-safety-comment-for-type-assertion -- Justified immediately above.
    const request = new Request(input as RequestInfo, init);
    const url = new URL(request.url);
    const offset = url.searchParams.get("offset");
    const fromStart = offset === null || offset === "-1";
    if (resume !== undefined && fromStart) url.searchParams.set("resume", resume);

    let response = await globalThis.fetch(new Request(url, request));

    if (response.status === 409) {
      // The sink declares `snapshot-then-live` for a token it will not honour,
      // so the recovery is to forget the token and read the product again.
      resume = undefined;
      const reason = await response.clone().text();
      options.onStatus({ kind: "resetting", reason: reason.slice(0, 200) });
      url.searchParams.delete("resume");
      url.searchParams.set("offset", "-1");
      response = await globalThis.fetch(new Request(url, request));
    }

    const minted = response.headers.get(RESUME_HEADER);
    if (minted !== null) resume = minted;
    if (response.ok) options.onStatus({ kind: "live", resume });
    return response;
  };

  const stream = new DurableStream({
    url: new URL(route, options.origin).toString(),
    contentType: "application/json",
    warnOnHttp: false,
    params: { scope: boardIssues.auth.value },
    // SAFETY: `DurableStream` calls its `fetch` with a request and options and
    // awaits a `Response`, which is exactly `guardedFetch`. The declared type is
    // the ambient `fetch`, whose extra members the client never touches.
    // oxlint-disable-next-line anti-slop/require-safety-comment-for-type-assertion -- Justified immediately above.
    fetch: guardedFetch as typeof globalThis.fetch,
  });

  const db = createStreamDB({ stream, state: boardCollections });

  return {
    db,
    preload: () => db.preload(),
    close: () => {
      db.close();
    },
  };
}

/** The maintained rows a board column renders, in a stable order. */
export function sortRows(rows: readonly IssueRowType[]): readonly IssueRowType[] {
  return [...rows].sort(
    (left, right) =>
      right.updatedAt.localeCompare(left.updatedAt) || left.issueId.localeCompare(right.issueId),
  );
}
