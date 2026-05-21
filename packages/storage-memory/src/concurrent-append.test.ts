/**
 * Concurrency coverage for StreamProtocol.append.
 *
 * Asserts that concurrent appends from multiple producers serialize at the
 * stream level so offset/currentOffset/closed/lastSeq mutations cannot race,
 * while producer idempotency semantics (duplicate/stale/gap) remain intact.
 */

import { describe, it, expect } from "vitest";
import { StreamProtocol } from "@streamsy/core";
import { createMemoryStreamStore } from "@streamsy/storage-memory";

const CONTENT_TYPE = "application/octet-stream";

function bytes(value: string): Uint8Array {
  return new TextEncoder().encode(value);
}

describe("StreamProtocol concurrent append", () => {
  it("serializes offset allocation across concurrent producers", async () => {
    const store = createMemoryStreamStore();
    const protocol = new StreamProtocol(store);
    const streamId = "concurrent-multi-producer";

    const created = await protocol.create(streamId, { contentType: CONTENT_TYPE });
    expect(created.status).toBe("created");

    const producerCount = 8;
    const appendsPerProducer = 25;
    const producers = Array.from({ length: producerCount }, (_, i) => `p${i}`);

    const tasks: Promise<unknown>[] = [];
    for (const producerId of producers) {
      for (let seq = 0; seq < appendsPerProducer; seq++) {
        tasks.push(
          protocol.append(streamId, {
            data: bytes(`${producerId}:${seq}`),
            contentType: CONTENT_TYPE,
            producer: { producerId, producerEpoch: 0, producerSeq: seq },
          }),
        );
      }
    }

    const results = await Promise.all(tasks);
    for (const result of results) {
      expect(result).toMatchObject({ status: "appended" });
    }

    const read = await protocol.read(streamId, {});
    expect(read.status).toBe("ok");
    expect(read.messages).toHaveLength(producerCount * appendsPerProducer);

    // Offsets must be strictly increasing and unique — the symptom of the bug
    // is duplicate offsets allocated from the same counter snapshot.
    const offsets = read.messages.map((m) => m.offset);
    const unique = new Set(offsets);
    expect(unique.size).toBe(offsets.length);
    for (let i = 1; i < offsets.length; i++) {
      expect(offsets[i]! > offsets[i - 1]!).toBe(true);
    }

    // Each producer's payloads must appear in seq order with no missing seqs.
    const seenByProducer = new Map<string, number[]>();
    for (const message of read.messages) {
      const [producerId, seqText] = new TextDecoder().decode(message.data).split(":");
      const list = seenByProducer.get(producerId!) ?? [];
      list.push(parseInt(seqText!, 10));
      seenByProducer.set(producerId!, list);
    }
    for (const producerId of producers) {
      const seqs = seenByProducer.get(producerId);
      expect(seqs).toBeDefined();
      expect(seqs!).toEqual(Array.from({ length: appendsPerProducer }, (_, i) => i));
    }

    const meta = await protocol.metadata(streamId);
    expect(meta.status).toBe("ok");
    expect(meta.nextOffset).toBe(read.nextOffset);
  });

  it("preserves producer idempotency under concurrent retries", async () => {
    const store = createMemoryStreamStore();
    const protocol = new StreamProtocol(store);
    const streamId = "producer-idempotency";

    await protocol.create(streamId, { contentType: CONTENT_TYPE });

    // Concurrent retries of the same (producer, epoch, seq=0) tuple: exactly
    // one must persist and all duplicates must be reported as such.
    const retryAttempts = 6;
    const retryResults = await Promise.all(
      Array.from({ length: retryAttempts }, () =>
        protocol.append(streamId, {
          data: bytes("payload-0"),
          contentType: CONTENT_TYPE,
          producer: { producerId: "p1", producerEpoch: 0, producerSeq: 0 },
        }),
      ),
    );
    const appendedRetries = retryResults.filter((r) => r.status === "appended");
    const duplicates = retryResults.filter((r) => r.status === "duplicate");
    expect(appendedRetries).toHaveLength(1);
    expect(duplicates).toHaveLength(retryAttempts - 1);

    // Gap detection: jump from seq 0 straight to seq 5 without 1..4.
    const gap = await protocol.append(streamId, {
      data: bytes("payload-5"),
      contentType: CONTENT_TYPE,
      producer: { producerId: "p1", producerEpoch: 0, producerSeq: 5 },
    });
    expect(gap).toMatchObject({ status: "producer-gap", expectedSeq: 1, receivedSeq: 5 });

    // Stale epoch: epoch 0 after producer has advanced to epoch 1.
    const newEpoch = await protocol.append(streamId, {
      data: bytes("payload-epoch-1-seq-0"),
      contentType: CONTENT_TYPE,
      producer: { producerId: "p1", producerEpoch: 1, producerSeq: 0 },
    });
    expect(newEpoch.status).toBe("appended");

    const stale = await protocol.append(streamId, {
      data: bytes("payload-stale"),
      contentType: CONTENT_TYPE,
      producer: { producerId: "p1", producerEpoch: 0, producerSeq: 1 },
    });
    expect(stale).toMatchObject({ status: "stale-epoch", currentEpoch: 1 });

    const read = await protocol.read(streamId, {});
    expect(read.status).toBe("ok");
    // Exactly two messages persisted: the first accepted seq=0 and the
    // epoch-1 reset. Duplicates, gap, and stale-epoch must not have written.
    expect(read.messages).toHaveLength(2);
  });
});
