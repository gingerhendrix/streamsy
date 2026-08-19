import {
  createHttpHandler,
  createMemoryStorageAdapter,
  directProtocolClient,
  StreamProtocol,
  type JsonValue,
  type StorageAdapter,
  type StreamProtocolClient,
} from "@streamsy/core";
import { streamIdentity } from "@streamsy/experimental/causal";
import { StateProjection } from "@streamsy/experimental/effect/state-projection";
import { contentType, sourceStreamPath, streamPath } from "./config.ts";

const streamPrefix = "/streams";
const streamIdFromPath = (path: string) => path.replace(/^\/streams\/?/, "");

export const hackerNewsSource = StateProjection.resource({
  identity: streamIdentity("hacker-news-newest-source"),
  streamId: streamIdFromPath(sourceStreamPath),
});

export const hackerNewsTarget = StateProjection.resource({
  identity: streamIdentity("hacker-news-newest-state"),
  streamId: streamIdFromPath(streamPath),
});

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
    for (const resource of [hackerNewsSource, hackerNewsTarget]) {
      const result = await this.client.stream(resource.streamId).create({ contentType });
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
