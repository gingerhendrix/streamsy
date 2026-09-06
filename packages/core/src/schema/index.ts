import { Schema } from "effect";

export const StreamId = Schema.String.pipe(Schema.brand("StreamId"));
export const Offset = Schema.String.pipe(
  Schema.check(Schema.isPattern(/^\d{16}_\d{16}$/)),
  Schema.brand("Offset"),
);
export const ProducerId = Schema.String.pipe(Schema.brand("ProducerId"));

export const StreamConfig = Schema.Struct({
  contentType: Schema.String,
  ttlSeconds: Schema.optionalKey(Schema.Finite),
  expiresAt: Schema.optionalKey(Schema.String),
  createdAt: Schema.Finite,
});

export const StreamLifecycle = Schema.Struct({
  lastSeq: Schema.optionalKey(Schema.String),
  closed: Schema.Boolean,
  closedAt: Schema.optionalKey(Schema.Finite),
  forkedFrom: Schema.optionalKey(StreamId),
  forkOffset: Schema.optionalKey(Offset),
  forkSubOffset: Schema.optionalKey(Schema.Finite),
  softDeleted: Schema.Boolean,
  expiresAtMs: Schema.optionalKey(Schema.Finite),
});

export const StreamRecord = Schema.Struct({
  id: StreamId,
  config: StreamConfig,
  lifecycle: StreamLifecycle,
  currentOffset: Offset,
});

export const StoredMessage = Schema.Struct({
  offset: Offset,
  timestamp: Schema.Finite,
  data: Schema.Uint8Array,
});

export const ProducerState = Schema.Struct({ epoch: Schema.Finite, lastSeq: Schema.Finite });

export const MessageWindow = Schema.Struct({
  after: Schema.optionalKey(Offset),
  until: Schema.optionalKey(Offset),
  limit: Schema.optionalKey(Schema.Finite),
});

export const ChangeSnapshot = Schema.Struct({
  present: Schema.Boolean,
  currentOffset: Offset,
  closed: Schema.Boolean,
  softDeleted: Schema.Boolean,
});

export type StreamId = typeof StreamId.Type;

export type Offset = typeof Offset.Type;

export type ProducerId = typeof ProducerId.Type;

export interface StreamConfig extends Schema.Schema.Type<typeof StreamConfig> {}

export interface StreamLifecycle extends Schema.Schema.Type<typeof StreamLifecycle> {}

export interface StreamRecord extends Schema.Schema.Type<typeof StreamRecord> {}

export interface StoredMessage extends Schema.Schema.Type<typeof StoredMessage> {}

export interface ProducerState extends Schema.Schema.Type<typeof ProducerState> {}

export interface MessageWindow extends Schema.Schema.Type<typeof MessageWindow> {}

export interface ChangeSnapshot extends Schema.Schema.Type<typeof ChangeSnapshot> {}

export const RecordPatch = Schema.Struct({
  currentOffset: Schema.optionalKey(Offset),
  config: Schema.optionalKey(
    Schema.Struct({
      ttlSeconds: Schema.optionalKey(Schema.Finite),
      expiresAt: Schema.optionalKey(Schema.String),
    }),
  ),
  lifecycle: Schema.optionalKey(
    Schema.Struct({
      ...StreamLifecycle.fields,
      closed: Schema.optionalKey(Schema.Boolean),
      softDeleted: Schema.optionalKey(Schema.Boolean),
    }),
  ),
  clear: Schema.optionalKey(
    Schema.Array(Schema.Literals(["ttlSeconds", "expiresAt", "expiresAtMs", "lastSeq"])),
  ),
});
export interface RecordPatch extends Schema.Schema.Type<typeof RecordPatch> {}
