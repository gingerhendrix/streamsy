import { Effect, Schema } from "effect";
import { HttpRouter } from "effect/http";
import { CreateFolder, FolderConflict, InvalidFile } from "./domain.ts";
import { authorize, json, jsonBody, requestFailures, storageFailure } from "./http.ts";
import type { Folder } from "./streams.ts";

/** A folder create failed in storage or in another object. The route answers 500. */
export class FolderCreateFailed extends Schema.TaggedError<FolderCreateFailed>()(
  "FolderCreateFailed",
  { cause: Schema.Unknown },
) {}

/** Keep the two answers a client can act on. Any other failure becomes `FolderCreateFailed`. */
export const createFailures = <A, E, R>(self: Effect.Effect<A, E, R>) =>
  self.pipe(
    Effect.mapError((error) =>
      Schema.is(FolderConflict)(error) || Schema.is(InvalidFile)(error)
        ? error
        : new FolderCreateFailed({ cause: error }),
    ),
  );

/**
 * `POST /api/folders`. Each host passes its own create: the Bun host commits the
 * registry and the stream together, and the Worker index registers the folder
 * and then asks the folder object for its stream.
 */
export const folderRoutes = <R>(
  create: (
    input: typeof CreateFolder.Type,
  ) => Effect.Effect<Folder, FolderConflict | InvalidFile | FolderCreateFailed, R>,
) => [
  HttpRouter.route(
    "POST",
    "/api/folders",
    Effect.gen(function* () {
      yield* authorize();

      return json(yield* create(yield* jsonBody(CreateFolder)));
    }).pipe(
      Effect.catchTags(
        {
          ...requestFailures,
          InvalidFile: ({ message }) => Effect.succeed(json({ error: message }, { status: 400 })),
          FolderConflict: ({ message }) =>
            Effect.succeed(json({ error: message }, { status: 409 })),
        },
        storageFailure,
      ),
    ),
  ),
];
