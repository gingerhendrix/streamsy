import { describe, expect, test } from "vitest";
import { streamIdentity } from "../causal.ts";
import { MAX_PRODUCER_ID_LENGTH, canonicalLaneInput, deriveProducerLane } from "./lane.ts";
import {
  MESH_LINEAGE_KEY,
  MESH_LINEAGE_TYPE,
  assertFactTypeAllowed,
  assertLineageCompatible,
  createLineageEvent,
  decodeLineageEvent,
} from "./state-meta.ts";

const config = {
  processorId: "counter",
  processorVersion: "1.0.0",
  outputGeneration: "blue",
  source: streamIdentity("source"),
  target: streamIdentity("target"),
  producerEpoch: 7,
};

describe("producer lane", () => {
  test("is canonical, deterministic, bounded, and changes with every semantic input", async () => {
    const first = await deriveProducerLane(config);
    const second = await deriveProducerLane(config);
    expect(first).toEqual(second);
    expect(first.producerId).toHaveLength(MAX_PRODUCER_ID_LENGTH);
    expect(canonicalLaneInput(config)).toBe(
      '["streamsy.mesh.producer-lane.v1","counter","1.0.0","blue","streamsy.identity.v1:source","streamsy.identity.v1:target"]',
    );

    for (const changed of [
      { ...config, processorId: "other" },
      { ...config, processorVersion: "2.0.0" },
      { ...config, outputGeneration: "green" },
      { ...config, source: streamIdentity("other-source") },
      { ...config, target: streamIdentity("other-target") },
    ]) {
      expect((await deriveProducerLane(changed)).producerId).not.toBe(first.producerId);
    }
  });

  test("epoch is fixed configuration, not part of producer-id derivation", async () => {
    const first = await deriveProducerLane(config);
    const restarted = await deriveProducerLane({ ...config, producerEpoch: 7 });
    const deliberatelyBumped = await deriveProducerLane({ ...config, producerEpoch: 8 });
    expect(restarted).toEqual(first);
    expect(deliberatelyBumped.producerId).toBe(first.producerId);
    expect(deliberatelyBumped.producerEpoch).toBe(8);
  });
});

describe("lineage metadata", () => {
  test("round-trips the canonical reserved row and validates its lane", async () => {
    const lane = await deriveProducerLane(config);
    const event = createLineageEvent(lane, { sourceThrough: "00000001", nextProducerSeq: 4 });
    expect(event.type).toBe(MESH_LINEAGE_TYPE);
    expect(event.key).toBe(MESH_LINEAGE_KEY);
    expect(event.headers.operation).toBe("upsert");
    expect(decodeLineageEvent(JSON.parse(JSON.stringify(event)))).toEqual(event);
    expect(() => assertLineageCompatible(event, lane)).not.toThrow();
  });

  test("rejects malformed, sentinel, and incompatible metadata", async () => {
    const lane = await deriveProducerLane(config);
    expect(() => createLineageEvent(lane, { sourceThrough: "now", nextProducerSeq: 1 })).toThrow();
    expect(() => decodeLineageEvent({ type: MESH_LINEAGE_TYPE })).toThrow();
    const event = createLineageEvent(lane, { sourceThrough: "00000001", nextProducerSeq: 1 });
    const other = await deriveProducerLane({ ...config, outputGeneration: "green" });
    expect(() => assertLineageCompatible(event, other)).toThrow(/outputGeneration|producerId/);
  });

  test("reserves the complete Streamsy namespace from fact collections", () => {
    expect(() => assertFactTypeAllowed({ type: "counter" })).not.toThrow();
    expect(() => assertFactTypeAllowed({ type: "__streamsy.application" })).toThrow(/reserved/);
  });
});
