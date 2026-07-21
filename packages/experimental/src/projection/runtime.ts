/**
 * Replay-safe projection runtime.
 *
 * A materializer that consumes a canonical source stream and maintains a
 * *separate* projection stream in which every output transition atomically
 * carries the source offset it is valid through. The canonical stream stays the
 * source of truth; the projection is a rebuildable, causally-watermarked
 * materialization of it.
 *
 * The runtime owns everything replay-safety needs — reading, the durable
 * watermark, atomic output append, replay-safe producer identity, output CAS,
 * duplicate/conflict classification, poison-event halting, status, and fault
 * injection. The domain owns only the pure pieces through a {@link ProjectionAdapter}:
 * decoding source messages, the pure reducer, encoding a transition into the
 * atomic output batch, and recovering the latest checkpoint from output bytes.
 *
 * ## Atomicity
 * A transition's output is encoded as a JSON array and appended in ONE
 * `ProtocolStream.append` call. For `application/json`, Streamsy frames a JSON
 * array body into one message per item committed in a single storage
 * transaction (one `AppendPlan`). The adapter puts the watermark (and a resume
 * snapshot) in that same array, so board changes and their source-through offset
 * can never commit apart.
 *
 * ## Replay-safe identity
 * Each transition appends under producer identity
 * `producerId = <processorId>::<generation>::<sourceStreamId>` with
 * `producerSeq` = the 0-based source ordinal. Re-appending an already-committed
 * transition returns `duplicate` instead of writing twice. Concurrency is
 * additionally guarded by `expectedOffset` CAS on the projection tail: a racing
 * writer with a stale tail gets `conflict`/`expected-offset`, reloads, and
 * converges.
 */

import type {
  AppendResult,
  ProtocolStream,
  StreamId,
  StreamOffset,
  StreamProtocolFactory,
} from "@streamsy/core";
import { ZERO_OFFSET, compareOffsets } from "@streamsy/core";

const encoder = new TextEncoder();

/** Metadata every transition embeds; the durable causal watermark. */
export interface ProjectionMeta {
  sourceStreamId: StreamId;
  /** Source offset this transition is valid through (write-ack token). */
  sourceThroughOffset: StreamOffset;
  /** 0-based ordinal of the applied source event (also the producer seq). */
  sourceSeq: number;
  generation: string;
  reducerVersion: string;
}

export interface ProjectionTransition<State, Event> {
  prev: State;
  next: State;
  event: Event;
  meta: ProjectionMeta;
}

/** Recovered from the projection stream on resume. */
export interface ProjectionCheckpoint<State> {
  state: State;
  sourceThroughOffset: StreamOffset;
  sourceSeq: number;
}

/**
 * The domain-specific, mostly-pure surface. Everything here is deterministic
 * except that {@link ProjectionAdapter.reduce} MAY throw to mark a poison event.
 */
export interface ProjectionAdapter<State, Event> {
  readonly processorId: string;
  readonly generation: string;
  readonly reducerVersion: string;
  readonly sourceStreamId: StreamId;
  readonly outputStreamId: StreamId;
  /** Output content type; defaults to `application/json`. */
  readonly outputContentType?: string;

  initial(): State;
  /** Decode one source stream message into a domain event. */
  decodeSourceMessage(data: Uint8Array): Event;
  /** Pure reducer. Throw to halt the projection at the prior watermark. */
  reduce(state: State, event: Event, meta: ProjectionMeta): State;
  /**
   * Encode a transition into the JSON items appended atomically as one batch.
   * The watermark ({@link ProjectionMeta}) and enough to resume MUST be inside.
   */
  encodeTransition(transition: ProjectionTransition<State, Event>): unknown[];
  /**
   * Recover the latest checkpoint from every projection message's bytes (in
   * stream order), or `null` when the projection is empty.
   */
  decodeCheckpoint(outputMessages: readonly Uint8Array[]): ProjectionCheckpoint<State> | null;
}

/**
 * Deterministic fault-injection seam. Either hook may throw to simulate a crash.
 * `afterAppend` fires *after* the output has committed but before the runtime
 * records the acknowledgement — the critical crash-after-output window.
 */
export interface FaultHooks {
  beforeAppend?(ctx: { sourceSeq: number }): void | Promise<void>;
  afterAppend?(ctx: { sourceSeq: number; result: AppendResult }): void | Promise<void>;
}

export interface ProjectionRuntimeOptions<State, Event> {
  protocol: StreamProtocolFactory;
  adapter: ProjectionAdapter<State, Event>;
  /** Producer epoch; bump to fence a superseded writer. Defaults to `1`. */
  producerEpoch?: number;
  faults?: FaultHooks;
}

export interface ProjectionRuntimeStatus {
  running: boolean;
  stopped: boolean;
  /** True once `sourceThroughOffset` has reached `sourceHead`. */
  caughtUp: boolean;
  sourceStreamId: StreamId;
  sourceHead: StreamOffset | null;
  sourceThroughOffset: StreamOffset | null;
  /** 0-based ordinal of the last applied source event, or -1 if none. */
  sourceSeq: number;
  outputTail: StreamOffset;
  /** Set when a poison event or fencing halted the projection. */
  lastError: { sourceSeq: number; sourceOffset: StreamOffset; message: string } | null;
}

export interface CatchUpResult {
  applied: number;
  status: ProjectionRuntimeStatus;
}

type ApplyOutcome = "applied" | "reload" | "halt";

const CONTENT_TYPE = "application/json";

function describe(result: { status: string }): string {
  return result.status;
}

/**
 * Incremental catch-up processor for one source→projection pair. Construct one,
 * then call {@link ProjectionRuntime.catchUp} (bounded) or
 * {@link ProjectionRuntime.follow} (live). Safe to discard and reconstruct at any
 * time — durable state lives entirely in the projection stream.
 */
export class ProjectionRuntime<State, Event> {
  private readonly protocol: StreamProtocolFactory;
  private readonly adapter: ProjectionAdapter<State, Event>;
  private readonly producerEpoch: number;
  private readonly faults: FaultHooks;
  private readonly contentType: string;
  private readonly producerId: string;

  private loaded = false;
  private running = false;
  private stopped = false;
  private state: State;
  /** 0-based producer seq for the NEXT event to apply. */
  private nextSeq = 0;
  private sourceThroughOffset: StreamOffset | null = null;
  private outputTail: StreamOffset = ZERO_OFFSET;
  private lastError: ProjectionRuntimeStatus["lastError"] = null;

  constructor(options: ProjectionRuntimeOptions<State, Event>) {
    this.protocol = options.protocol;
    this.adapter = options.adapter;
    this.producerEpoch = options.producerEpoch ?? 1;
    this.faults = options.faults ?? {};
    this.contentType = options.adapter.outputContentType ?? CONTENT_TYPE;
    this.producerId = `${this.adapter.processorId}::${this.adapter.generation}::${this.adapter.sourceStreamId}`;
    this.state = options.adapter.initial();
  }

  /** Load durable state (state + watermark + output tail) from the projection stream. */
  async load(): Promise<void> {
    const output = await this.ensureOutputStream();
    const messages = await readAll(output, undefined);

    this.outputTail = messages.length > 0 ? messages[messages.length - 1]!.offset : ZERO_OFFSET;
    const checkpoint = this.adapter.decodeCheckpoint(messages.map((m) => m.data));
    if (checkpoint) {
      this.state = checkpoint.state;
      this.sourceThroughOffset = checkpoint.sourceThroughOffset;
      this.nextSeq = checkpoint.sourceSeq + 1;
    } else {
      this.state = this.adapter.initial();
      this.sourceThroughOffset = null;
      this.nextSeq = 0;
    }
    this.lastError = null;
    this.stopped = false;
    this.loaded = true;
  }

  /**
   * Process every currently-available source event, then return. Idempotent and
   * gap-free: a re-run after more source events appends resumes exactly at the
   * durable watermark. Halts (without advancing) if the reducer throws.
   */
  async catchUp(): Promise<CatchUpResult> {
    if (!this.loaded) await this.load();
    if (this.stopped) return { applied: 0, status: await this.status() };

    this.running = true;
    let applied = 0;
    try {
      outer: while (true) {
        const page = await this.readSourcePage(this.sourceThroughOffset);
        if (page.messages.length === 0) break;
        for (const message of page.messages) {
          const outcome = await this.applyOne(message);
          if (outcome === "applied") {
            applied += 1;
            continue;
          }
          if (outcome === "reload") {
            await this.load();
            continue outer;
          }
          break outer; // halt
        }
        if (page.upToDate) break;
      }
    } finally {
      this.running = false;
    }
    return { applied, status: await this.status() };
  }

  /**
   * Catch up, then follow the live source tail, re-catching-up on each wake.
   * Wakes are hints; the durable watermark remains the source of truth, so a
   * duplicate or spurious wake is harmless. Returns when `signal` aborts or the
   * projection halts on a poison event.
   */
  async follow(options: { signal: AbortSignal; pollTimeoutMs?: number }): Promise<CatchUpResult> {
    let applied = (await this.catchUp()).applied;
    const source = await this.protocol.get(this.adapter.sourceStreamId);
    if (source.status !== "ok") return { applied, status: await this.status() };

    while (!options.signal.aborted && !this.stopped) {
      const live = await source.stream.readLive({
        offset: this.sourceThroughOffset ?? ZERO_OFFSET,
        mode: "long-poll",
        signal: options.signal,
      });
      if (live.status === "not-supported") break;
      if (options.signal.aborted) break;
      if (live.status === "ok" && live.messages.length > 0) {
        applied += (await this.catchUp()).applied;
      }
    }
    return { applied, status: await this.status() };
  }

  async status(): Promise<ProjectionRuntimeStatus> {
    const sourceHead = await this.readSourceHead();
    const caughtUp =
      sourceHead === null ||
      (this.sourceThroughOffset !== null &&
        compareOffsets(this.sourceThroughOffset, sourceHead) >= 0);
    return {
      running: this.running,
      stopped: this.stopped,
      caughtUp,
      sourceStreamId: this.adapter.sourceStreamId,
      sourceHead,
      sourceThroughOffset: this.sourceThroughOffset,
      sourceSeq: this.nextSeq - 1,
      outputTail: this.outputTail,
      lastError: this.lastError,
    };
  }

  /** Current in-memory projection state (as last loaded/applied). */
  currentState(): State {
    return this.state;
  }

  private async applyOne(message: {
    data: Uint8Array;
    offset: StreamOffset;
  }): Promise<ApplyOutcome> {
    const event = this.adapter.decodeSourceMessage(message.data);
    const meta: ProjectionMeta = {
      sourceStreamId: this.adapter.sourceStreamId,
      sourceThroughOffset: message.offset,
      sourceSeq: this.nextSeq,
      generation: this.adapter.generation,
      reducerVersion: this.adapter.reducerVersion,
    };

    let next: State;
    try {
      next = this.adapter.reduce(this.state, event, meta);
    } catch (error) {
      this.stopped = true;
      this.lastError = {
        sourceSeq: meta.sourceSeq,
        sourceOffset: message.offset,
        message: error instanceof Error ? error.message : String(error),
      };
      return "halt";
    }

    const items = this.adapter.encodeTransition({ prev: this.state, next, event, meta });
    const data = encoder.encode(JSON.stringify(items));
    const output = await this.ensureOutputStream();

    await this.faults.beforeAppend?.({ sourceSeq: meta.sourceSeq });
    const result = await output.append({
      data,
      contentType: this.contentType,
      producer: {
        producerId: this.producerId,
        producerEpoch: this.producerEpoch,
        producerSeq: meta.sourceSeq,
      },
      expectedOffset: this.outputTail,
    });
    await this.faults.afterAppend?.({ sourceSeq: meta.sourceSeq, result });

    switch (result.status) {
      case "appended":
      case "duplicate": {
        // `duplicate` means this exact transition already committed (an ambiguous
        // retry). Either way the transition is applied exactly once in the log.
        this.state = next;
        this.nextSeq = meta.sourceSeq + 1;
        this.sourceThroughOffset = message.offset;
        this.outputTail = result.offset;
        return "applied";
      }
      case "conflict": {
        if (result.conflictReason === "expected-offset") return "reload";
        throw new Error(`projection append conflict: ${result.conflictReason}`);
      }
      case "stale-epoch": {
        this.stopped = true;
        this.lastError = {
          sourceSeq: meta.sourceSeq,
          sourceOffset: message.offset,
          message: `fenced by newer epoch ${result.currentEpoch}`,
        };
        return "halt";
      }
      default:
        throw new Error(`unexpected projection append status: ${describe(result)}`);
    }
  }

  private async ensureOutputStream(): Promise<ProtocolStream> {
    const existing = await this.protocol.get(this.adapter.outputStreamId);
    if (existing.status === "ok") return existing.stream;
    const created = await this.protocol.create(this.adapter.outputStreamId, {
      contentType: this.contentType,
    });
    if (created.status === "created" || created.status === "exists") return created.stream;
    throw new Error(`cannot open projection stream: ${describe(created)}`);
  }

  private async readSourcePage(
    afterOffset: StreamOffset | null,
  ): Promise<{ messages: { data: Uint8Array; offset: StreamOffset }[]; upToDate: boolean }> {
    const source = await this.protocol.get(this.adapter.sourceStreamId);
    if (source.status !== "ok") return { messages: [], upToDate: true };
    const read = await source.stream.read({ offset: afterOffset ?? undefined });
    if (read.status !== "ok") return { messages: [], upToDate: true };
    return {
      messages: read.messages.map((m) => ({ data: m.data, offset: m.offset })),
      upToDate: read.upToDate,
    };
  }

  private async readSourceHead(): Promise<StreamOffset | null> {
    const source = await this.protocol.get(this.adapter.sourceStreamId);
    if (source.status !== "ok") return null;
    const metadata = await source.stream.metadata();
    if (metadata.status !== "ok") return null;
    return metadata.nextOffset;
  }
}

/** Read every message strictly after `afterOffset` (or from the start). */
async function readAll(
  stream: ProtocolStream,
  afterOffset: StreamOffset | undefined,
): Promise<{ data: Uint8Array; offset: StreamOffset }[]> {
  const out: { data: Uint8Array; offset: StreamOffset }[] = [];
  let offset = afterOffset;
  for (;;) {
    const read = await stream.read({ offset });
    if (read.status !== "ok") break;
    for (const message of read.messages) out.push({ data: message.data, offset: message.offset });
    if (read.upToDate || read.messages.length === 0) break;
    offset = read.nextOffset;
  }
  return out;
}
