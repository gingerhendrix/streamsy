import { Serve } from "@streamsy/serve";
import { Effect, Schema } from "effect";
import { HttpRouter, HttpServerResponse } from "effect/http";
import { Files, FolderKey } from "./streams.ts";

/** Capture the complete key: a valid FolderKey can exceed the router's named-parameter limit. */
export const mountFiles = (path: `/${string}` = "/streams/*") =>
  Serve.route(
    path,
    Effect.gen(function* () {
      const { "*": rest } = yield* HttpRouter.schemaPathParams(
        Schema.Struct({ "*": Schema.String }),
      );

      const match = /^folders\/([^/]+)\/files$/.exec(rest);

      if (match?.[1] === undefined)
        return HttpServerResponse.jsonUnsafe({ error: "Not found" }, { status: 404 });

      const folderId = yield* Schema.decodeEffect(FolderKey)(match[1]).pipe(
        Effect.mapError(() => new InvalidFolderStream()),
      );

      return yield* Serve.state(Files.at({ folderId }));
    }).pipe(
      Effect.catchTags({
        SchemaError: () => Effect.succeed(HttpServerResponse.empty({ status: 404 })),
        InvalidFolderStream: () =>
          Effect.succeed(
            HttpServerResponse.jsonUnsafe({ error: "Invalid folder stream" }, { status: 400 }),
          ),
      }),
    ),
  );

class InvalidFolderStream extends Schema.TaggedError<InvalidFolderStream>()(
  "InvalidFolderStream",
  {},
) {}
