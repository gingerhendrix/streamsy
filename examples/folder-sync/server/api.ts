import { Effect, Layer } from "effect";
import { HttpRouter, HttpServerResponse } from "effect/http";
import { blobRoutes } from "./blob-routes.ts";
import { BlobStore } from "./blob-store.ts";
import { fallbackRoutes, fileRoutes } from "./file-routes.ts";
import { createFailures, folderRoutes } from "./folder-routes.ts";
import { FolderStore } from "./folder-store.ts";
import { WriterToken, WriterTokenLive } from "./http.ts";

/** The Bun create commits the registry entry and the folder stream in one transaction. */
const createFolder = (input: { folderId: string; title: string; txid: string }) =>
  Effect.flatMap(FolderStore, (store) =>
    createFailures(store.create(input.folderId, input.title, input.txid)),
  );

/**
 * The routes of the Bun write API. The handlers read their services per request,
 * so a Worker object can serve the same routes from its own request context.
 */
export const ApiRequestLive = HttpRouter.addAll([
  ...folderRoutes(createFolder),
  ...blobRoutes,
  ...fileRoutes,
  ...fallbackRoutes,
]);

/** The Bun write API, with its services taken from the layer that builds it. */
export const ApiLive = ApiRequestLive.pipe(
  HttpRouter.provideRequest(
    Layer.effectContext(Effect.context<FolderStore | BlobStore | WriterToken>()),
  ),
  Layer.provide(WriterTokenLive),
);

/**
 * The file routes of one Worker folder object. The Worker checks the token at
 * its edge, and the object checks it again, so a request that reaches the object
 * by another path still needs the token.
 */
export const FolderApiRequestLive = HttpRouter.addAll([...fileRoutes, ...fallbackRoutes]);

export const HealthLive = HttpRouter.add("GET", "/health", HttpServerResponse.text("Folder sync"));
