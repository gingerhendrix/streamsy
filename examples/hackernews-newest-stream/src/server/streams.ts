import {
  createHttpHandler,
  createMemoryStorageAdapter,
  directProtocolClient,
  StreamProtocol,
  type JsonValue,
  type StorageAdapter,
  type StreamProtocolClient,
} from "@streamsy/core";
import { Effect } from "effect";
import { streamContentType, streamPrefix } from "./config.ts";
import { pollFailure, type PollFailure } from "./poller/contract.ts";
import { hackerNewsResources, hackerNewsSource } from "./stream-resources.ts";
import type { HackerNewsSourceChange } from "./story-index-projection.ts";

export { hackerNewsResources, hackerNewsSource, hackerNewsTarget } from "./stream-resources.ts";

export class DemoStreams {
  private readonly protocol: StreamProtocol;
  readonly client: StreamProtocolClient;
  private readonly handler: ReturnType<typeof createHttpHandler>;

  constructor(adapter?: StorageAdapter) {
    this.protocol = new StreamProtocol({
      storage: { adapter: adapter ?? createMemoryStorageAdapter() },
    });
    this.handler = createHttpHandler({ protocol: this.protocol, pathPrefix: streamPrefix });
    this.client = directProtocolClient(this.protocol);
  }

  async start(): Promise<void> {
    for (const resource of hackerNewsResources) {
      const result = await this.client
        .stream(resource.streamId)
        .create({ contentType: streamContentType });
      if (result.status !== "created" && result.status !== "conflict") {
        throw new Error(
          `Unable to create Streamsy demo stream ${resource.streamId}: ${result.status}`,
        );
      }
    }
  }

  async appendSourceBatch(items: readonly JsonValue[]): Promise<string> {
    const result = await this.client.stream(hackerNewsSource.streamId).appendJsonBatch(items);
    if (result.status !== "appended" && result.status !== "duplicate") {
      throw new Error(`Unable to append Hacker News source batch: ${result.status}`);
    }
    return result.offset;
  }

  async fetch(request: Request): Promise<Response> {
    return this.handler.fetch(request);
  }

  async close(): Promise<void> {
    await this.client.close();
  }
}

/** Adapt a Promise-based source append (such as DemoStreams) to the sink contract. */
export const appendSourceBatchFromPromise =
  (append: (changes: readonly HackerNewsSourceChange[]) => Promise<string>) =>
  (changes: readonly HackerNewsSourceChange[]): Effect.Effect<string, PollFailure> =>
    Effect.tryPromise({
      try: () => append(changes),
      catch: pollFailure("appendSourceBatch"),
    });
