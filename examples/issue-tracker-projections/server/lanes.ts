/**
 * Producer lanes as a service.
 *
 * A lane is derived once per runtime from a processor identity and its
 * source/target pair. Derivation is the mesh library's async API, so it is
 * wrapped exactly once here — at the adapter boundary — instead of appearing as
 * an `Effect.promise` inside every projection call site.
 *
 * `Cache` gives the memoisation and the concurrent-lookup dedupe this needs; the
 * durable authority remains the in-band lineage written to each target, so the
 * cache is a latency device and never a source of truth.
 */
import { streamIdentity } from "@streamsy/experimental/causal";
import { deriveProducerLane, type ProducerLane } from "@streamsy/experimental/ivm-mesh";
import { Cache, Context, Effect, Layer } from "effect";
import { streamNames } from "../shared/domain.ts";

export const PROCESSOR_VERSION = "1.0.0";
export const OUTPUT_GENERATION = "generation-1";
export const PRODUCER_EPOCH = 1;

/** How many distinct lanes one runtime keeps derived. */
const LANE_CAPACITY = 1_024;

interface LaneKey {
  readonly processorId: string;
  readonly source: string;
  readonly target: string;
}

export interface ProjectionLaneResolver {
  readonly issueDetail: (workspaceId: string, issueId: string) => Effect.Effect<ProducerLane>;
  readonly projectBoard: (workspaceId: string, projectId: string) => Effect.Effect<ProducerLane>;
}

export class ProjectionLanes extends Context.Service<ProjectionLanes, ProjectionLaneResolver>()(
  "issue-tracker-projections/ProjectionLanes",
) {}

const derive = (key: LaneKey): Effect.Effect<ProducerLane> =>
  Effect.promise(() =>
    deriveProducerLane({
      processorId: key.processorId,
      processorVersion: PROCESSOR_VERSION,
      outputGeneration: OUTPUT_GENERATION,
      source: streamIdentity(key.source),
      target: streamIdentity(key.target),
      producerEpoch: PRODUCER_EPOCH,
    }),
  );

export const layer: Layer.Layer<ProjectionLanes> = Layer.effect(
  ProjectionLanes,
  Effect.gen(function* () {
    // Cache keys must compare structurally, so the lookup takes the encoded
    // triple rather than an object literal.
    const cache = yield* Cache.make<string, ProducerLane>({
      capacity: LANE_CAPACITY,
      lookup: (encoded) => {
        const [processorId, source, target] = encoded.split(" ");
        if (processorId === undefined || source === undefined || target === undefined) {
          throw new TypeError(`a lane key must be "processorId source target": ${encoded}`);
        }
        return derive({ processorId, source, target });
      },
    });

    const lane = (key: LaneKey) =>
      Cache.get(cache, `${key.processorId} ${key.source} ${key.target}`);

    return ProjectionLanes.of({
      issueDetail: (workspaceId, issueId) =>
        lane({
          processorId: "issue-detail",
          source: streamNames.issueEvents(workspaceId, issueId),
          target: streamNames.issueDetail(workspaceId, issueId),
        }),
      projectBoard: (workspaceId, projectId) =>
        lane({
          processorId: "project-board",
          source: streamNames.membership(workspaceId, projectId),
          target: streamNames.board(workspaceId, projectId),
        }),
    });
  }),
);
