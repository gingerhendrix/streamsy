import { Schema } from "effect";
import { Streamsy } from "@streamsy/core";
import { Projection } from "@streamsy/projection";

/** The streams of the workerd object pair. The folder owns `Files`; the user owns `UserWake`. */
export class Files extends Streamsy.json<Files>()(
  "WakePair/Files",
  "users/:user/folders/:folderId/files",
  {
    params: { user: Schema.String, folderId: Schema.String },
    schema: Schema.String,
  },
) {}

export class UserWake extends Streamsy.wake<UserWake>()("WakePair/UserWake", "users/:user/wake", {
  params: { user: Schema.String },
}) {}

export const filesWake = Projection.wake({
  id: "files-wake",
  params: { user: Schema.String, folderId: Schema.String },
  input: Files,
  output: UserWake,
});
