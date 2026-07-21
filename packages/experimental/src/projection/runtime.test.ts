import { describe, expect, it } from "vitest";
import { ZERO_OFFSET, createMemoryStorageAdapter, createStreamProtocol } from "@streamsy/core";
import type { StorageAdapter, StreamProtocolFactory } from "@streamsy/core";

import { ProjectionRuntime, type ProjectionAdapter } from "./runtime.ts";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

const SOURCE = "events";
const OUTPUT = "projection";
const POISON = -999;

interface MetaRecord {
  kind: "meta";
  sum: number;
  sourceThroughOffset: string;
  sourceSeq: number;
}

/** A trivial running-sum projection used to exercise the runtime primitive. */
function counterAdapter(
  overrides: Partial<Pick<ProjectionAdapter<number, number>, "processorId" | "generation">> = {},
): ProjectionAdapter<number, number> {
  return {
    processorId: overrides.processorId ?? "counter",
    generation: overrides.generation ?? "v1",
    reducerVersion: "sum-1",
    sourceStreamId: SOURCE,
    outputStreamId: OUTPUT,
    initial: () => 0,
    decodeSourceMessage: (data) => JSON.parse(decoder.decode(data)) as number,
    reduce: (state, event) => {
      if (event === POISON) throw new Error("poison event");
      return state + event;
    },
    encodeTransition: ({ next, meta }) => [
      {
        kind: "meta",
        sum: next,
        sourceThroughOffset: meta.sourceThroughOffset,
        sourceSeq: meta.sourceSeq,
      } satisfies MetaRecord,
    ],
    decodeCheckpoint: (messages) => {
      let latest: MetaRecord | null = null;
      for (const data of messages) {
        const value = JSON.parse(decoder.decode(data)) as MetaRecord;
        if (value.kind === "meta") latest = value;
      }
      if (!latest) return null;
      return {
        state: latest.sum,
        sourceThroughOffset: latest.sourceThroughOffset,
        sourceSeq: latest.sourceSeq,
      };
    },
  };
}

async function newProtocol(): Promise<{
  protocol: StreamProtocolFactory;
  adapter: StorageAdapter;
}> {
  const adapter = createMemoryStorageAdapter();
  return { protocol: createStreamProtocol({ storage: { adapter } }), adapter };
}

async function seedSource(protocol: StreamProtocolFactory, values: number[]): Promise<string[]> {
  const created = await protocol.create(SOURCE, { contentType: "application/json" });
  const stream =
    created.status === "created" || created.status === "exists" ? created.stream : null;
  const target = stream ?? (await getStream(protocol, SOURCE));
  const offsets: string[] = [];
  for (const value of values) {
    const appended = await target.append({
      contentType: "application/json",
      data: encoder.encode(JSON.stringify(value)),
    });
    if (appended.status !== "appended") throw new Error(`seed append failed: ${appended.status}`);
    offsets.push(appended.offset);
  }
  return offsets;
}

async function getStream(protocol: StreamProtocolFactory, id: string) {
  const got = await protocol.get(id);
  if (got.status !== "ok") throw new Error(`stream ${id} not available: ${got.status}`);
  return got.stream;
}

/** Count committed projection messages (one meta item per applied transition). */
async function outputMessageCount(protocol: StreamProtocolFactory): Promise<number> {
  const stream = await getStream(protocol, OUTPUT);
  const read = await stream.read({});
  return read.status === "ok" ? read.messages.length : 0;
}

describe("ProjectionRuntime", () => {
  it("materializes with an embedded watermark and equals the source fold", async () => {
    const { protocol } = await newProtocol();
    const offsets = await seedSource(protocol, [1, 2, 3, 4]);
    const runtime = new ProjectionRuntime({ protocol, adapter: counterAdapter() });

    const { applied, status } = await runtime.catchUp();
    expect(applied).toBe(4);
    expect(runtime.currentState()).toBe(10);
    expect(status.sourceThroughOffset).toBe(offsets[3]);
    expect(status.sourceSeq).toBe(3);
    expect(status.caughtUp).toBe(true);
    expect(await outputMessageCount(protocol)).toBe(4);
  });

  it("resumes strictly after the durable watermark with no gaps or duplicates", async () => {
    const { protocol } = await newProtocol();
    await seedSource(protocol, [1, 2]);
    const first = await new ProjectionRuntime({ protocol, adapter: counterAdapter() }).catchUp();
    expect(first.applied).toBe(2);

    await seedSource(protocol, [3, 4, 5]);
    const runtime = new ProjectionRuntime({ protocol, adapter: counterAdapter() });
    const second = await runtime.catchUp();
    expect(second.applied).toBe(3); // only the new events
    expect(runtime.currentState()).toBe(15);
    expect(await outputMessageCount(protocol)).toBe(5);
  });

  it("does not double-apply after a crash immediately following output commit", async () => {
    const { protocol } = await newProtocol();
    await seedSource(protocol, [1, 2, 3, 4]);

    // Crash right after event #2 (seq 2) commits, before the ack is recorded.
    let thrown = false;
    const crashing = new ProjectionRuntime({
      protocol,
      adapter: counterAdapter(),
      faults: {
        afterAppend: ({ sourceSeq }) => {
          if (sourceSeq === 2) {
            thrown = true;
            throw new Error("crash after output commit");
          }
        },
      },
    });
    await expect(crashing.catchUp()).rejects.toThrow("crash after output commit");
    expect(thrown).toBe(true);
    // Events 0,1,2 committed exactly once each despite the crash.
    expect(await outputMessageCount(protocol)).toBe(3);

    // A fresh runtime resumes from the durable watermark and finishes cleanly.
    const recovered = new ProjectionRuntime({ protocol, adapter: counterAdapter() });
    const result = await recovered.catchUp();
    expect(result.applied).toBe(1); // only event #3 remained
    expect(recovered.currentState()).toBe(10);
    expect(await outputMessageCount(protocol)).toBe(4); // never double-applied
  });

  it("classifies an ambiguous re-append of a committed transition as duplicate", async () => {
    // Directly exercises the replay-safe producer identity the runtime relies on.
    const { protocol } = await newProtocol();
    const created = await protocol.create(OUTPUT, { contentType: "application/json" });
    const stream =
      created.status === "created" ? created.stream : await getStream(protocol, OUTPUT);
    const producer = { producerId: "counter::v1::events", producerEpoch: 1, producerSeq: 0 };
    const data = encoder.encode(JSON.stringify([{ kind: "meta", sum: 1 }]));

    const first = await stream.append({
      contentType: "application/json",
      data,
      producer,
      expectedOffset: ZERO_OFFSET,
    });
    expect(first.status).toBe("appended");
    if (first.status !== "appended") throw new Error("expected appended");

    // Same producer + seq, correct tail: an ambiguous retry is deduplicated.
    const retry = await stream.append({
      contentType: "application/json",
      data,
      producer,
      expectedOffset: first.offset,
    });
    expect(retry.status).toBe("duplicate");
  });

  it("prevents two racing materializers from double-applying a transition", async () => {
    const { protocol } = await newProtocol();
    await seedSource(protocol, [1, 2, 3]);

    const a = new ProjectionRuntime({ protocol, adapter: counterAdapter() });
    const b = new ProjectionRuntime({ protocol, adapter: counterAdapter() });
    await a.load();
    await b.load(); // both start from the empty projection

    await a.catchUp();
    expect(a.currentState()).toBe(6);
    expect(await outputMessageCount(protocol)).toBe(3);

    // B still believes the projection is empty and re-attempts every transition.
    // Replay-safe producer identity classifies each as already committed, so no
    // transition is written twice; B converges to the same state.
    await b.catchUp();
    expect(b.currentState()).toBe(6);
    expect(await outputMessageCount(protocol)).toBe(3); // never double-applied
  });

  it("lets a distinct-identity writer lose the output CAS and converge on reload", async () => {
    const { protocol } = await newProtocol();
    await seedSource(protocol, [1, 2, 3]);

    // Distinct processor ids => distinct producer identities, so the loser is
    // arbitrated by expectedOffset CAS rather than producer dedup.
    const a = new ProjectionRuntime({ protocol, adapter: counterAdapter({ processorId: "a" }) });
    const b = new ProjectionRuntime({ protocol, adapter: counterAdapter({ processorId: "b" }) });
    await a.load();
    await b.load();

    await a.catchUp();
    await b.catchUp(); // loses CAS on first append, reloads from A's committed tail

    expect(b.currentState()).toBe(6);
    expect(await outputMessageCount(protocol)).toBe(3); // CAS prevented duplicates
  });

  it("halts at the prior watermark on a poison event and exposes the failure", async () => {
    const { protocol } = await newProtocol();
    const offsets = await seedSource(protocol, [1, 2, POISON, 4]);

    const runtime = new ProjectionRuntime({ protocol, adapter: counterAdapter() });
    const { status } = await runtime.catchUp();

    expect(status.stopped).toBe(true);
    expect(status.lastError?.sourceSeq).toBe(2);
    expect(status.sourceThroughOffset).toBe(offsets[1]); // stopped before the poison
    expect(await outputMessageCount(protocol)).toBe(2);

    // Re-running halts again at the same offset; the event is never skipped.
    const rerun = new ProjectionRuntime({ protocol, adapter: counterAdapter() });
    const again = await rerun.catchUp();
    expect(again.applied).toBe(0);
    expect(again.status.lastError?.sourceSeq).toBe(2);
    expect(await outputMessageCount(protocol)).toBe(2);
  });
});
