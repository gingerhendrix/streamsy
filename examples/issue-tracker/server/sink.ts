/**
 * The `stateSink` runtime.
 *
 * `boardIssues` declares one public product: keyed issue rows, published
 * through Durable State, resumable by its native offset, and falling back to
 * snapshot-then-live. This module is the only thing that writes that product,
 * and it owns the State stream and snapshot boundary.
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
import { streamNames } from "../domain/declaration.ts";
import { decodeIssueRow, type IssueRow } from "../domain/issue.ts";
import type { Change } from "@streamsy/views-ir";
import { AppendRejected, StreamUnavailable } from "./errors.ts";

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

export interface IssueSinkService {
  /** Create the sink's State stream when it does not exist yet. */
  readonly ensure: (workspaceId: string) => Effect.Effect<void, StreamUnavailable>;
  /** Publish keyed changes as live Durable State messages. */
  readonly publish: (
    workspaceId: string,
    changes: readonly Change<IssueRow, string>[],
  ) => Effect.Effect<void, StreamUnavailable | AppendRejected>;
  /** Re-publish the complete current relation as a fresh snapshot. */
  readonly republish: (
    workspaceId: string,
    rows: readonly IssueRow[],
  ) => Effect.Effect<void, StreamUnavailable | AppendRejected>;
}

export class IssueSink extends Context.Service<IssueSink, IssueSinkService>()(
  "issue-tracker/IssueSink",
) {}

export const sinkLayer = (protocol: StreamProtocolFactory): Layer.Layer<IssueSink> =>
  Layer.effect(
    IssueSink,
    Effect.sync(() => {
      const state: BoardStateProtocol = createDurableStateProtocol(protocol, boardStateSchema);

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
          changes: readonly Change<IssueRow, string>[],
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
      });
    }),
  );
