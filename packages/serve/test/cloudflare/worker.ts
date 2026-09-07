/* oxlint-disable effecttsgo/async-function -- This fixture owns the real workerd boundary. */
import type { AlarmInvocationInfo, DurableObjectNamespace } from "@cloudflare/workers-types";
import { Context, Effect, Layer } from "effect";
import { Storage, StorageFault, StreamsReader, StreamsWriter, type StreamId } from "@streamsy/core";
import { StreamsyObject, router, type ObjectOptions } from "@streamsy/serve/cloudflare";
import { layerProtocol } from "@streamsy/storage/durable-object";
import { byKeyOptions, byStreamOptions } from "./fixture-options.ts";

interface Env {
  readonly STREAMS: DurableObjectNamespace;
}

interface AlarmObservation {
  readonly isRetry: boolean;
  readonly retryCount: number;
}

class ProbeObject extends StreamsyObject<Env> {
  #layerAcquisitions = 0;
  #migrationAttempts = 0;
  #alarmInvocations = 0;
  #activeReads = 0;
  #alarmInfo: Array<AlarmObservation> = [];
  #failLayerOnce = false;
  #failNextExpiry = false;
  #failExpiryWhile = false;
  #copyLimit: number | undefined;
  #longPollTimeoutMs = 1_000;
  #alarmAfterMutation: number | null = null;

  override options(): ObjectOptions<Env> {
    const options: ObjectOptions<Env> = {
      ...byStreamOptions,
      namespace: (env: Env) => env.STREAMS,
    };
    return this.#copyLimit === undefined
      ? options
      : { ...options, copyOnForkMaxBytes: this.#copyLimit };
  }

  protected copyLimit(): number | undefined {
    return this.#copyLimit;
  }

  override layer(): Layer.Layer<StreamsReader | StreamsWriter | Storage, StorageFault> {
    this.#layerAcquisitions += 1;
    this.#migrationAttempts += 1;
    if (this.#failLayerOnce) {
      this.#failLayerOnce = false;
      return Layer.effectContext(
        Effect.fail(
          new StorageFault({
            operation: "fixture.layer",
            message: "fixture layer failure",
            retryable: true,
          }),
        ),
      );
    }

    const protocol = layerProtocol({
      client: { storage: this.ctx.storage },
      longPollTimeoutMs: this.#longPollTimeoutMs,
    });
    const failNextExpiry = () => {
      if (!this.#failNextExpiry) return false;
      this.#failNextExpiry = false;
      return true;
    };
    const failExpiryWhile = () => this.#failExpiryWhile;
    const incrementActiveReads = () => {
      this.#activeReads += 1;
    };
    const decrementActiveReads = () => {
      this.#activeReads -= 1;
    };
    const observed = Layer.effectContext(
      Effect.gen(function* () {
        const storage = yield* Storage;
        const reader = yield* StreamsReader;
        const writer = yield* StreamsWriter;
        const observedStorage = Storage.of({
          ...storage,
          nextExpiry: Effect.suspend(() => {
            if (failExpiryWhile() || failNextExpiry()) {
              return Effect.fail(
                new StorageFault({
                  operation: "fixture.nextExpiry",
                  message: "fixture expiry failure",
                  retryable: true,
                }),
              );
            }
            return storage.nextExpiry;
          }),
        });
        const observedReader = StreamsReader.of({
          head: reader.head,
          read: reader.read,
          readNext: (id: StreamId, options) =>
            Effect.sync(incrementActiveReads).pipe(
              Effect.andThen(reader.readNext(id, options)),
              Effect.ensuring(Effect.sync(decrementActiveReads)),
            ),
        });
        return Context.make(StreamsReader, observedReader).pipe(
          Context.add(StreamsWriter, writer),
          Context.add(Storage, observedStorage),
        );
      }),
    );
    return observed.pipe(Layer.provideMerge(protocol));
  }

  override async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    if (url.host === "streamsy.internal" && url.pathname === "/fork-source")
      this.#exportRequests += 1;
    if (url.pathname === "/__probe") {
      if (url.searchParams.has("fail-layer-once")) {
        this.#failLayerOnce = true;
        return Response.json({ ok: true });
      }
      if (url.searchParams.has("fail-next-expiry")) {
        this.#failNextExpiry = true;
        return Response.json({ ok: true });
      }
      if (url.searchParams.has("fail-expiry-while")) {
        this.#failExpiryWhile = true;
        return Response.json({ ok: true });
      }
      if (url.searchParams.has("clear-fail-expiry")) {
        this.#failExpiryWhile = false;
        return Response.json({ ok: true });
      }
      const copyLimit = url.searchParams.get("copy-limit");
      if (copyLimit !== null) {
        const parsed = Number(copyLimit);
        if (Number.isSafeInteger(parsed) && parsed > 0) this.#copyLimit = parsed;
        return Response.json({ ok: true });
      }
      const longPollTimeoutMs = url.searchParams.get("long-poll-timeout");
      if (longPollTimeoutMs !== null) {
        const parsed = Number(longPollTimeoutMs);
        if (Number.isFinite(parsed) && parsed > 0) this.#longPollTimeoutMs = parsed;
        return Response.json({ ok: true });
      }
      return this.#probe();
    }
    const response = await super.fetch(request);
    if (request.method === "PUT" || request.method === "POST" || request.method === "DELETE")
      this.#alarmAfterMutation = await this.ctx.storage.getAlarm();
    return response;
  }

  override alarm(info?: AlarmInvocationInfo): Promise<void> {
    this.#alarmInvocations += 1;
    if (info !== undefined) this.#alarmInfo.push(info);
    return super.alarm(info);
  }

  async #probe(): Promise<Response> {
    const rows = Array.from(
      this.ctx.storage.sql
        .exec<{
          readonly stream_id: string;
          readonly expires_at_ms: number | null;
          readonly forked_from: string | null;
        }>("SELECT stream_id, expires_at_ms, forked_from FROM streamsy_streams ORDER BY stream_id")
        .raw(),
    );
    const messages = Array.from(
      this.ctx.storage.sql
        .exec<{
          readonly stream_id: string;
          readonly offset: string;
          readonly timestamp: number;
          readonly length: number;
        }>(
          "SELECT stream_id, offset, timestamp, length(data) FROM streamsy_messages " +
            "ORDER BY stream_id, offset",
        )
        .raw(),
    );
    return Response.json({
      layerAcquisitions: this.#layerAcquisitions,
      migrationAttempts: this.#migrationAttempts,
      alarmInvocations: this.#alarmInvocations,
      activeReads: this.#activeReads,
      alarmInfo: this.#alarmInfo,
      exportRequests: this.#exportRequests,
      alarmAfterMutation: this.#alarmAfterMutation,
      alarm: await this.ctx.storage.getAlarm(),
      rows,
      messages,
    });
  }

  #exportRequests = 0;
}

export class ProbeByKeyObject extends ProbeObject {
  override options(): ObjectOptions<Env> {
    const options: ObjectOptions<Env> = {
      ...byKeyOptions,
      namespace: (env: Env) => env.STREAMS,
    };
    const copyLimit = this.copyLimit();
    return copyLimit === undefined ? options : { ...options, copyOnForkMaxBytes: copyLimit };
  }
}

const app = router<Env>({ namespace: (env) => env.STREAMS, ...byStreamOptions });

export default {
  fetch: app.fetch,
};

export { ProbeObject };
