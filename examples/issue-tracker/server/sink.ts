/**
 * The `stateSink` runtime.
 *
 * `boardIssues` declares one public product: keyed issue rows, published
 * through Durable State, resumable, falling back to snapshot-then-live. This
 * module is the only thing that writes that product, and it owns the whole
 * public contract — the State stream, the snapshot boundary, and the resume
 * token.
 *
 * The view graph stays private. A consumer binds to the sink, never to
 * `issue-tracker.issues`.
 */
import type { StreamProtocolFactory } from "@streamsy/core";
import {
  createDurableStateProtocol,
  type DurableStateProtocol,
  type DurableStateStream,
} from "@streamsy/state";
import { Context, Effect, Layer } from "effect";
import { boardIssues, streamNames } from "../domain/declaration.ts";
import { decodeIssueRow, type IssueRow } from "../domain/issue.ts";
import type { Change } from "../views/contracts.ts";
import { AppendRejected, SessionResumeExpired, StreamUnavailable } from "./errors.ts";
import { AppConfig } from "./config.ts";

/**
 * The sink's public collection map.
 *
 * `type: "issue"` is the wire tag a consumer's TanStack DB collection binds to,
 * and `primaryKey: "issueId"` is the sink's declared key expression made
 * concrete. Both are part of the contract, not implementation detail.
 */
/* oxlint-disable anti-slop/no-unknown-parameters, anti-slop/no-unknown-returns -- This codec IS the JSON wire boundary the rule points at: `encode` hands a decoded row to the protocol's JSON writer, and `decode` runs the declared `IssueRow` schema over whatever the wire produced. */
export const boardStateSchema = {
  issues: {
    schema: {
      encode: (value: IssueRow): unknown => value,
      decode: (value: unknown): IssueRow => decodeIssueRow(value),
    },
    type: "issue",
    primaryKey: "issueId",
  },
} as const;
/* oxlint-enable anti-slop/no-unknown-parameters, anti-slop/no-unknown-returns */

export type BoardStateProtocol = DurableStateProtocol<typeof boardStateSchema>;
export type BoardStateStream = DurableStateStream<typeof boardStateSchema>;

/** A verified resume position for one sink session. */
export interface ResumePosition {
  readonly workspaceId: string;
  readonly offset: string;
}

export interface IssueSinkService {
  /** Create the sink's State stream when it does not exist yet. */
  readonly ensure: (workspaceId: string) => Effect.Effect<void, StreamUnavailable>;
  /** Publish keyed changes as live Durable State messages. */
  readonly publish: (
    workspaceId: string,
    changes: readonly Change<IssueRow>[],
  ) => Effect.Effect<void, StreamUnavailable | AppendRejected>;
  /**
   * Re-publish the complete current relation as a fresh snapshot.
   *
   * This is the sink's convergence path when publication fell behind a
   * committed checkpoint: the durable rows are the authority, not the messages
   * a lost process meant to send.
   *
   * The snapshot is a complete set of upserts bounded by `snapshot-start` and
   * `snapshot-end`, with no `reset`. This relation has no exits — an issue row
   * enters and is updated, never removed — so re-upserting every row is already
   * a complete rebuild, and nothing a consumer holds can be stale afterwards.
   * `reset` belongs to the first view that can drop a row.
   */
  readonly republish: (
    workspaceId: string,
    rows: readonly IssueRow[],
  ) => Effect.Effect<void, StreamUnavailable | AppendRejected>;
  /** Mint the resume token a consumer sends back to continue this session. */
  readonly mintResume: (workspaceId: string, offset: string) => Effect.Effect<string>;
  /** Verify a resume token, or explain by typed policy why it will not be honoured. */
  readonly verifyResume: (
    workspaceId: string,
    token: string,
  ) => Effect.Effect<ResumePosition, SessionResumeExpired>;
}

export class IssueSink extends Context.Service<IssueSink, IssueSinkService>()(
  "issue-tracker/IssueSink",
) {}

const expired = (
  reason: "malformed" | "expired" | "wrong-sink" | "out-of-window",
): SessionResumeExpired =>
  new SessionResumeExpired({
    sink: boardIssues.name,
    reason,
    fallback: boardIssues.protocol.fallback,
  });

/** Raised by the sink route when the underlying stream can no longer serve an offset. */
export const resumeOutOfWindow = (): SessionResumeExpired => expired("out-of-window");

interface TokenBody {
  readonly s: string;
  readonly w: string;
  readonly o: string;
  readonly e: number;
}

export const sinkLayer = (
  protocol: StreamProtocolFactory,
): Layer.Layer<IssueSink, never, AppConfig> =>
  Layer.effect(
    IssueSink,
    Effect.gen(function* () {
      const config = yield* AppConfig;
      const state: BoardStateProtocol = createDurableStateProtocol(protocol, boardStateSchema);

      const signingKey = yield* Effect.promise(() =>
        crypto.subtle.importKey(
          "raw",
          new TextEncoder().encode(config.resumeTokenSecret),
          { name: "HMAC", hash: "SHA-256" },
          false,
          ["sign"],
        ),
      ).pipe(Effect.cached);

      const sign = (payload: string): Effect.Effect<string> =>
        signingKey.pipe(
          Effect.flatMap((key) =>
            Effect.promise(() =>
              crypto.subtle.sign("HMAC", key, new TextEncoder().encode(payload)),
            ),
          ),
          Effect.map((bytes) => base64url(new Uint8Array(bytes))),
        );

      const open = (workspaceId: string): Effect.Effect<BoardStateStream, StreamUnavailable> => {
        const streamId = streamNames.boardState(workspaceId);
        return Effect.promise(() => state.get(streamId)).pipe(
          Effect.flatMap((result) =>
            result.status === "ok"
              ? Effect.succeed(result.stream)
              : Effect.fail(new StreamUnavailable({ streamId, status: result.status })),
          ),
        );
      };

      /**
       * Every append is checked. A Durable State message that the protocol
       * refuses is a failed publication, never a silent gap in the product.
       */
      const appended = (
        workspaceId: string,
        run: () => Promise<{ status: string }>,
      ): Effect.Effect<void, AppendRejected> =>
        Effect.promise(run).pipe(
          Effect.flatMap((result) =>
            result.status === "appended"
              ? Effect.void
              : Effect.fail(
                  new AppendRejected({
                    stream: streamNames.boardState(workspaceId),
                    status: result.status,
                  }),
                ),
          ),
        );

      return IssueSink.of({
        ensure: Effect.fn("IssueSink.ensure")(function* (workspaceId: string) {
          const streamId = streamNames.boardState(workspaceId);
          const created = yield* Effect.promise(() => state.create(streamId));
          if (created.status === "created" || created.status === "exists") return;
          return yield* new StreamUnavailable({ streamId, status: created.status });
        }),

        publish: Effect.fn("IssueSink.publish")(function* (
          workspaceId: string,
          changes: readonly Change<IssueRow>[],
        ) {
          if (changes.length === 0) return;
          const stream = yield* open(workspaceId);
          for (const change of changes) {
            if (change.kind === "exit") {
              yield* appended(workspaceId, () =>
                stream.state.delete("issues", change.key, { oldValue: change.before }),
              );
              continue;
            }
            yield* appended(workspaceId, () =>
              stream.state.upsert("issues", change.after, { key: change.key }),
            );
          }
        }),

        republish: Effect.fn("IssueSink.republish")(function* (
          workspaceId: string,
          rows: readonly IssueRow[],
        ) {
          const stream = yield* open(workspaceId);
          yield* appended(workspaceId, () => stream.state.snapshotStart());
          for (const row of rows) {
            yield* appended(workspaceId, () =>
              stream.state.upsert("issues", row, { key: row.issueId }),
            );
          }
          yield* appended(workspaceId, () => stream.state.snapshotEnd());
        }),

        mintResume: Effect.fn("IssueSink.mintResume")(function* (
          workspaceId: string,
          offset: string,
        ) {
          const body: TokenBody = {
            s: boardIssues.name,
            w: workspaceId,
            o: offset,
            e: Date.now() + config.resumeTokenTtlSeconds * 1_000,
          };
          const payload = base64url(new TextEncoder().encode(JSON.stringify(body)));
          return `${payload}.${yield* sign(payload)}`;
        }),

        verifyResume: Effect.fn("IssueSink.verifyResume")(function* (
          workspaceId: string,
          token: string,
        ) {
          const [payload, signature] = token.split(".");
          if (payload === undefined || signature === undefined) return yield* expired("malformed");
          if ((yield* sign(payload)) !== signature) return yield* expired("malformed");

          // SAFETY: the payload's signature has already been verified against
          // this sink's key, so the bytes are ones this sink minted. Every
          // field is still checked below, because a token minted by an older
          // version of this sink is not a token this one can honour.
          // oxlint-disable-next-line anti-slop/require-safety-comment-for-type-assertion -- Justified immediately above.
          const body = yield* Effect.try({
            try: () => JSON.parse(decodeBase64url(payload)) as TokenBody,
            catch: () => expired("malformed"),
          });
          if (body.s !== boardIssues.name || body.w !== workspaceId) {
            return yield* expired("wrong-sink");
          }
          /* oxlint-disable anti-slop/no-runtime-typeof -- A token body is untrusted JSON until each field is established; these two checks are that parse, and every failure is the typed `SessionResumeExpired` the sink declares. */
          if (typeof body.e !== "number" || body.e <= Date.now()) return yield* expired("expired");
          if (typeof body.o !== "string" || body.o.length === 0) {
            return yield* expired("malformed");
          }
          /* oxlint-enable anti-slop/no-runtime-typeof */
          return { workspaceId, offset: body.o };
        }),
      });
    }),
  );

function base64url(bytes: Uint8Array): string {
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary).replaceAll("+", "-").replaceAll("/", "_").replaceAll("=", "");
}

function decodeBase64url(value: string): string {
  const padded = value.replaceAll("-", "+").replaceAll("_", "/");
  return atob(padded.padEnd(padded.length + ((4 - (padded.length % 4)) % 4), "="));
}
