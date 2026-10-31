import { expectTypeOf } from "vitest";
import { Context, Effect, type Layer, Schema } from "effect";
import type { SqlClient } from "effect/sql";
import {
  Outbox,
  OutboxStore,
  type InvalidConfiguration,
  type StoreFailure,
} from "@streamsy/outbox";
import { layerSql } from "@streamsy/outbox/sql";
import { layerMemory } from "@streamsy/outbox/memory";

class Sender extends Context.Service<
  Sender,
  { readonly send: (count: number) => Effect.Effect<void> }
>()("test/Sender") {}

const definition = Outbox.define({
  name: "types",
  payload: Schema.Struct({ id: Schema.String, count: Schema.FiniteFromString }),
  idempotencyKey: (value) => value.id,
  retry: { maxAttempts: 1, initialDelayMs: 0, factor: 1, maxDelayMs: 0 },
});

const delivery = Outbox.drain(definition, ({ payload }) =>
  Sender.use((sender) => sender.send(payload.count)),
);

expectTypeOf<Effect.Services<typeof delivery>>().toEqualTypeOf<OutboxStore | Sender>();

expectTypeOf<Effect.Error<typeof delivery>>().toEqualTypeOf<StoreFailure | InvalidConfiguration>();

expectTypeOf<Outbox.PayloadOf<typeof definition>>().toEqualTypeOf<{
  readonly id: string;
  readonly count: number;
}>();

expectTypeOf(layerMemory).toEqualTypeOf<Layer.Layer<OutboxStore>>();

expectTypeOf(layerSql).toEqualTypeOf<Layer.Layer<OutboxStore, never, SqlClient.SqlClient>>();

// @ts-expect-error Payload values are decoded numbers, not their wire strings.
const invalidPayload = Outbox.enqueue(definition, [{ id: "a", count: "42" }]);

void invalidPayload;

export function serviceFreeCodec(codec: Schema.Codec<string, string, Sender>): void {
  // @ts-expect-error Declaration codecs cannot require services during decoding.
  Outbox.define({ ...definition, payload: codec, idempotencyKey: (value: string) => value });
}

export function encodingServiceFree(codec: Schema.Codec<string, string, never, Sender>): void {
  // @ts-expect-error Declaration codecs cannot require services during encoding.
  Outbox.define({ ...definition, payload: codec, idempotencyKey: (value: string) => value });
}

// @ts-expect-error Definitions must go through validation and freezing.
const unchecked: Outbox.Definition<string> = {
  name: "unchecked",
  payload: Schema.String,
  idempotencyKey: (value) => value,
  retry: definition.retry,
};

void unchecked;
