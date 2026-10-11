import { Clock } from "effect";
import { routes as protocolRoutes } from "@streamsy/core/protocol";
/* oxlint-disable effecttsgo/async-function -- This fixture owns the real workerd boundary. */
import type {
  AlarmInvocationInfo,
  DurableObjectNamespace,
  DurableObjectState,
} from "@cloudflare/workers-types";
import { Context, Effect, Layer, Option, Schema } from "effect";
import { Storage, StorageFault, StreamId } from "@streamsy/core";
import { StreamsReader, StreamsWriter } from "@streamsy/core/protocol";
import { Serve } from "@streamsy/serve";
import { StreamsyObject, Wakeup, router } from "@streamsy/serve/cloudflare";
import { DurableObjectStorage } from "@streamsy/storage/durable-object";
import { byStreamOptions } from "./fixture-options.ts";

interface Env {
  readonly STREAMS: DurableObjectNamespace;
}

interface AlarmObservation {
  readonly observedAt: number;
  readonly isRetry: boolean;
  readonly retryCount: number;
}

interface ProbeState {
  layerAcquisitions: number;
  migrationAttempts: number;
  alarmInvocations: number;
  activeReads: number;
  alarmInfo: Array<AlarmObservation>;
  failLayerOnce: boolean;
  throwLayerOnce: boolean;
  failNextExpiry: boolean;
  failExpiryWhile: boolean;
  longPollTimeoutMs: number;
  alarmAfterMutation: number | null;
}

const probes = new WeakMap<DurableObjectState, ProbeState>();

const probeFor = (state: DurableObjectState): ProbeState => {
  let probe = probes.get(state);

  if (probe === undefined) {
    probe = {
      layerAcquisitions: 0,
      migrationAttempts: 0,
      alarmInvocations: 0,
      activeReads: 0,
      alarmInfo: [],
      failLayerOnce: false,
      throwLayerOnce: false,
      failNextExpiry: false,
      failExpiryWhile: false,
      longPollTimeoutMs: 1_000,
      alarmAfterMutation: null,
    };
    probes.set(state, probe);
  }

  return probe;
};

const probeLayer = (
  state: DurableObjectState,
): Layer.Layer<StreamsReader | StreamsWriter | Storage, StorageFault> => {
  const probe = probeFor(state);
  probe.layerAcquisitions += 1;
  probe.migrationAttempts += 1;

  if (probe.throwLayerOnce) {
    probe.throwLayerOnce = false;
    throw new Error("fixture layer factory defect");
  }

  if (probe.failLayerOnce) {
    probe.failLayerOnce = false;

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

  const protocol = DurableObjectStorage.layerProtocol({
    client: { storage: state.storage },
    longPollTimeoutMs: probe.longPollTimeoutMs,
  });

  const failNextExpiry = () => {
    if (!probe.failNextExpiry) return false;
    probe.failNextExpiry = false;

    return true;
  };

  const failExpiryWhile = () => probe.failExpiryWhile;

  const incrementActiveReads = () => {
    probe.activeReads += 1;
  };

  const decrementActiveReads = () => {
    probe.activeReads -= 1;
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
};

class ProbeObject extends StreamsyObject.make<Env, StreamsReader | StreamsWriter>({
  app: Layer.mergeAll(
    protocolRoutes({ prefix: "/streams" }),
    Serve.route(
      "/serve-value/:seat",
      Effect.flatMap(
        Serve.params({ paramSchema: Schema.Struct({ seat: Schema.FiniteFromString }) }),
        ({ seat }) => Serve.value(Schema.Struct({ seat: Schema.Finite }), Effect.succeed({ seat })),
      ),
    ),
  ),
  layer: probeLayer,
  wakeups: [
    Wakeup.make<Storage | StreamsWriter>({
      name: "test/app-work",
      next: Storage.use((storage) => storage.record(StreamId.make("app-work"))).pipe(
        Effect.map(Option.map(() => 0)),
      ),
      run: StreamsWriter.use((writer) => writer.remove(StreamId.make("app-work"))),
    }),
  ],
}) {
  readonly #state = probeFor(this.ctx);

  override async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname === "/__probe") {
      if (url.searchParams.has("throw-layer-once")) {
        this.#state.throwLayerOnce = true;

        return Response.json({ ok: true });
      }

      if (url.searchParams.has("fail-layer-once")) {
        this.#state.failLayerOnce = true;

        return Response.json({ ok: true });
      }

      if (url.searchParams.has("fail-next-expiry")) {
        this.#state.failNextExpiry = true;

        return Response.json({ ok: true });
      }

      if (url.searchParams.has("fail-expiry-while")) {
        this.#state.failExpiryWhile = true;

        return Response.json({ ok: true });
      }

      if (url.searchParams.has("clear-fail-expiry")) {
        this.#state.failExpiryWhile = false;

        return Response.json({ ok: true });
      }

      const longPollTimeoutMs = url.searchParams.get("long-poll-timeout");

      if (longPollTimeoutMs !== null) {
        const parsed = Number(longPollTimeoutMs);

        if (Number.isFinite(parsed) && parsed > 0) this.#state.longPollTimeoutMs = parsed;

        return Response.json({ ok: true });
      }

      return this.#probe();
    }

    const response = await super.fetch(request);

    if (request.method === "PUT" || request.method === "POST" || request.method === "DELETE")
      this.#state.alarmAfterMutation = await this.ctx.storage.getAlarm();

    return response;
  }

  override alarm(info?: AlarmInvocationInfo): Promise<void> {
    this.#state.alarmInvocations += 1;

    if (info !== undefined)
      this.#state.alarmInfo.push({ ...info, observedAt: Effect.runSync(Clock.currentTimeMillis) });

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
          "SELECT stream_id, msg_offset, timestamp, length(data) FROM streamsy_messages " +
            "ORDER BY stream_id, msg_offset",
        )
        .raw(),
    );

    return Response.json({
      layerAcquisitions: this.#state.layerAcquisitions,
      migrationAttempts: this.#state.migrationAttempts,
      alarmInvocations: this.#state.alarmInvocations,
      activeReads: this.#state.activeReads,
      alarmInfo: this.#state.alarmInfo,
      alarmAfterMutation: this.#state.alarmAfterMutation,
      alarm: await this.ctx.storage.getAlarm(),
      rows,
      messages,
    });
  }
}

export class ProbeByKeyObject extends ProbeObject {}

const app = router<Env>({ namespace: (env) => env.STREAMS, ...byStreamOptions });

export default {
  fetch: app.fetch,
};

export { ProbeObject };
