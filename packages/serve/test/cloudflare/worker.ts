/* oxlint-disable effecttsgo/async-function -- This fixture owns the real workerd boundary. */
import type { AlarmInvocationInfo, DurableObjectNamespace } from "@cloudflare/workers-types";
import { Context, Effect, Layer } from "effect";
import { Storage, StorageFault, StreamsReader, StreamsWriter, type StreamId } from "@streamsy/core";
import { StreamsyObject, router } from "@streamsy/serve/cloudflare";
import { layerProtocol } from "@streamsy/storage/durable-object";

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
  #hostCommandRuns = 0;
  #activeReads = 0;
  #alarmInfo: Array<AlarmObservation> = [];
  #failLayerOnce = false;
  #failNextExpiry = false;

  override options() {
    return { pathPrefix: "/streams" };
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

    const protocol = layerProtocol({ client: { storage: this.ctx.storage } });
    const failNextExpiry = () => {
      if (!this.#failNextExpiry) return false;
      this.#failNextExpiry = false;
      return true;
    };
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
            if (failNextExpiry()) {
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

  override fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname === "/__probe") {
      if (url.searchParams.has("fail-layer-once")) {
        this.#failLayerOnce = true;
        return Promise.resolve(Response.json({ ok: true }));
      }
      if (url.searchParams.has("fail-next-expiry")) {
        this.#failNextExpiry = true;
        return Promise.resolve(Response.json({ ok: true }));
      }
      return this.#probe();
    }
    return super.fetch(request);
  }

  override alarm(info?: AlarmInvocationInfo): Promise<void> {
    this.#hostCommandRuns += 1;
    if (info !== undefined) this.#alarmInfo.push(info);
    return super.alarm(info);
  }

  async #probe(): Promise<Response> {
    const rows = Array.from(
      this.ctx.storage.sql
        .exec<{ readonly stream_id: string; readonly expires_at_ms: number | null }>(
          "SELECT stream_id, expires_at_ms FROM streamsy_streams ORDER BY stream_id",
        )
        .raw(),
    );
    return Response.json({
      layerAcquisitions: this.#layerAcquisitions,
      migrationAttempts: this.#migrationAttempts,
      hostCommandRuns: this.#hostCommandRuns,
      activeReads: this.#activeReads,
      alarmInfo: this.#alarmInfo,
      alarm: await this.ctx.storage.getAlarm(),
      rows,
    });
  }
}

const app = router<Env>({ namespace: (env) => env.STREAMS, pathPrefix: "/streams" });

export default {
  fetch: app.fetch,
};

export { ProbeObject };
