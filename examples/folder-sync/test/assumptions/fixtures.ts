import { testDirectory } from "../../scripts/test-directory.ts";
import { Streamsy } from "@streamsy/core";
import { BunStorage } from "@streamsy/storage/bun";
import { CheckpointStore, Commit, Projection, Transaction } from "@streamsy/projection";
import { Layer, Schema } from "effect";

export class Files extends Streamsy.json<Files>()("Assumptions/Files", "folders/:folderId/files", {
  params: { folderId: Schema.String },
  schema: Schema.String,
}) {}

export class Shared extends Streamsy.json<Shared>()("Assumptions/Shared", "shared", {
  params: {},
  schema: Schema.Struct({ folderId: Schema.String, text: Schema.String }),
}) {}

export class Registry extends Streamsy.json<Registry>()("Assumptions/Registry", "registry", {
  params: {},
  schema: Schema.Struct({ folderId: Schema.String }),
}) {}

export const emitted = Projection.emit({
  id: "assumption-folder-emits",
  params: { folderId: Schema.String },
  inputs: { input: Files },
  outputs: { output: Shared },
  commit: Commit.producer,
  emit: ({ item }, unit) => ({ output: [{ folderId: unit.params.folderId, text: item }] }),
});

export const host = (filename: string) =>
  Layer.mergeAll(CheckpointStore.layer, Transaction.layer).pipe(
    Layer.provideMerge(
      BunStorage.layer(Streamsy.group(Files, Shared, Registry), {
        client: { filename },
      }),
    ),
  );

export const temp = testDirectory("assumptions-");
