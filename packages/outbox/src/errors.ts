import { Schema } from "effect";

export class OutboxUnavailable extends Schema.TaggedError<
  OutboxUnavailable,
  { readonly brand: unique symbol }
>()("OutboxUnavailable", { operation: Schema.String, detail: Schema.String }) {}

export class InvalidStoredRecord extends Schema.TaggedError<
  InvalidStoredRecord,
  { readonly brand: unique symbol }
>()("InvalidStoredRecord", { operation: Schema.String, detail: Schema.String }) {}

export class InvalidConfiguration extends Schema.TaggedError<
  InvalidConfiguration,
  { readonly brand: unique symbol }
>()("InvalidConfiguration", { detail: Schema.String }) {}

export class PayloadEncodingFailure extends Schema.TaggedError<
  PayloadEncodingFailure,
  { readonly brand: unique symbol }
>()("PayloadEncodingFailure", { outbox: Schema.String, detail: Schema.String }) {}

/** Expected delivery outcomes are consumed by drain, never returned as operational errors. */
export class RetryableDeliveryFailure extends Schema.TaggedError<
  RetryableDeliveryFailure,
  { readonly brand: unique symbol }
>()("RetryableDeliveryFailure", { detail: Schema.String }) {}

export class PermanentDeliveryFailure extends Schema.TaggedError<
  PermanentDeliveryFailure,
  { readonly brand: unique symbol }
>()("PermanentDeliveryFailure", { detail: Schema.String }) {}

export type DeliveryFailure = RetryableDeliveryFailure | PermanentDeliveryFailure;

/** What an adapter can fail with. */
export type StoreFailure = OutboxUnavailable | InvalidStoredRecord;
