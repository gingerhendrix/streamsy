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
import { boardIssues, boardLabelCounts, streamNames } from "../domain/declaration.ts";
import {
  decodeLabelCountRow,
  decodeProjectBoardCard,
  type LabelCountRow,
  type ProjectBoardCard,
} from "../domain/issue.ts";
import type { Change } from "@streamsy/views-ir";
import { AppendRejected, StreamUnavailable } from "./errors.ts";

/**
 * The sink's public collection map.
 *
 * `type: "issue"` is the wire tag a consumer's TanStack DB collection binds to,
 * and its primary key is the key `projectBoard` declares, carried through the
 * sink rather than restated here. Both are part of the contract, not
 * implementation detail.
 */
/* oxlint-disable anti-slop/no-unknown-parameters, anti-slop/no-unknown-returns -- This codec IS the JSON wire boundary the rule points at: `encode` hands a decoded row to the protocol's JSON writer, and `decode` runs the declared `IssueRow` schema over whatever the wire produced. */
export const boardStateSchema = {
  [boardIssues.collection.name]: {
    schema: {
      encode: (value: ProjectBoardCard): unknown => value,
      decode: (value: unknown): ProjectBoardCard => decodeProjectBoardCard(value),
    },
    type: boardIssues.collection.type,
    primaryKey: boardIssues.collection.primaryKey,
  },
} as const;

/** The label-count sink's public collection map, derived the same way. */
export const labelCountStateSchema = {
  [boardLabelCounts.collection.name]: {
    schema: {
      encode: (value: LabelCountRow): unknown => value,
      decode: (value: unknown): LabelCountRow => decodeLabelCountRow(value),
    },
    type: boardLabelCounts.collection.type,
    primaryKey: boardLabelCounts.collection.primaryKey,
  },
} as const;
/* oxlint-enable anti-slop/no-unknown-parameters, anti-slop/no-unknown-returns */

export type BoardStateProtocol = DurableStateProtocol<typeof boardStateSchema>;
export type BoardStateStream = DurableStateStream<typeof boardStateSchema>;
export type LabelCountStateProtocol = DurableStateProtocol<typeof labelCountStateSchema>;
export type LabelCountStateStream = DurableStateStream<typeof labelCountStateSchema>;

export interface IssueSinkService {
  /** Create the sink's State stream when it does not exist yet. */
  readonly ensure: (workspaceId: string) => Effect.Effect<void, StreamUnavailable>;
  /** Publish keyed changes as live Durable State messages. */
  readonly publish: (
    workspaceId: string,
    changes: readonly Change<ProjectBoardCard, string>[],
  ) => Effect.Effect<void, StreamUnavailable | AppendRejected>;
  /** Re-publish the complete current relation as a fresh snapshot. */
  readonly republish: (
    workspaceId: string,
    rows: readonly ProjectBoardCard[],
  ) => Effect.Effect<void, StreamUnavailable | AppendRejected>;
  readonly publishLabelCounts: (
    workspaceId: string,
    changes: readonly Change<LabelCountRow, string>[],
  ) => Effect.Effect<void, StreamUnavailable | AppendRejected>;
  readonly republishLabelCounts: (
    workspaceId: string,
    rows: readonly LabelCountRow[],
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
      const countState: LabelCountStateProtocol = createDurableStateProtocol(
        protocol,
        labelCountStateSchema,
      );

      const openCounts = (
        workspaceId: string,
      ): Effect.Effect<LabelCountStateStream, StreamUnavailable> => {
        const streamId = streamNames.labelCountState(workspaceId);
        return Effect.promise(() => countState.get(streamId)).pipe(
          Effect.flatMap((result) =>
            result.status === "ok"
              ? Effect.succeed(result.stream)
              : Effect.fail(new StreamUnavailable({ streamId, status: result.status })),
          ),
        );
      };

      const appendedCount = (
        workspaceId: string,
        run: () => Promise<{ status: string }>,
      ): Effect.Effect<void, AppendRejected> =>
        Effect.promise(run).pipe(
          Effect.flatMap((result) =>
            result.status === "appended"
              ? Effect.void
              : Effect.fail(
                  new AppendRejected({
                    stream: streamNames.labelCountState(workspaceId),
                    status: result.status,
                  }),
                ),
          ),
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
          changes: readonly Change<ProjectBoardCard, string>[],
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
          rows: readonly ProjectBoardCard[],
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

        /**
         * Publish the label-count deltas, exits included.
         *
         * An exit here *is* a State delete on the sink's own stream, and that is
         * deliberate — it is the checked sink's normal exit protocol, the same
         * one the board has used since Integration 1, and it is what tells a
         * resuming client that a key left the relation. It is the opposite
         * direction from the delete rule the catalog states: a `delete` envelope
         * arriving on an *ingested* State collection is still refused as
         * `UnsupportedStateOperation`, because exclusion from a maintained
         * relation belongs in the plan where it is declared and checkable.
         * Inbound deletes are rejected; outbound exits are published.
         */
        publishLabelCounts: Effect.fn("IssueSink.publishLabelCounts")(function* (
          workspaceId: string,
          changes: readonly Change<LabelCountRow, string>[],
        ) {
          if (changes.length === 0) return;
          const stream = yield* openCounts(workspaceId);
          for (const change of changes) {
            if (change.kind === "exit") {
              yield* appendedCount(workspaceId, () =>
                stream.state.delete("labelCounts", change.key, { oldValue: change.before }),
              );
              continue;
            }
            yield* appendedCount(workspaceId, () =>
              stream.state.upsert("labelCounts", change.after, { key: change.key }),
            );
          }
        }),

        republishLabelCounts: Effect.fn("IssueSink.republishLabelCounts")(function* (
          workspaceId: string,
          rows: readonly LabelCountRow[],
        ) {
          const stream = yield* openCounts(workspaceId);
          yield* appendedCount(workspaceId, () => stream.state.snapshotStart());
          for (const row of rows) {
            yield* appendedCount(workspaceId, () =>
              stream.state.upsert("labelCounts", row, { key: row.labelId }),
            );
          }
          yield* appendedCount(workspaceId, () => stream.state.snapshotEnd());
        }),
      });
    }),
  );
