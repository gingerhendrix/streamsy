import { Schema } from "effect";

export class ViewStoreUnavailable extends Schema.TaggedError<ViewStoreUnavailable>()(
  "ViewStoreUnavailable",
  { operation: Schema.String, detail: Schema.String },
) {}
export class ViewStateRestorePoison extends Schema.TaggedError<ViewStateRestorePoison>()(
  "ViewStateRestorePoison",
  { table: Schema.String, identity: Schema.String, key: Schema.String, detail: Schema.String },
) {}
export class ViewCursorConflict extends Schema.TaggedError<ViewCursorConflict>()(
  "ViewCursorConflict",
  {
    planName: Schema.String,
    partition: Schema.String,
    expected: Schema.NullOr(Schema.String),
    actual: Schema.NullOr(Schema.String),
  },
) {}
export class ViewHistoryExpired extends Schema.TaggedError<ViewHistoryExpired>()(
  "ViewHistoryExpired",
  { epoch: Schema.Finite, requested: Schema.Finite, first: Schema.Finite, latest: Schema.Finite },
) {}
export class ViewCheckpointIncompatible extends Schema.TaggedError<ViewCheckpointIncompatible>()(
  "ViewCheckpointIncompatible",
  { reducerId: Schema.String, reason: Schema.String },
) {}
