import { HttpHandler, StreamProtocol } from "@streamsy/core";
import { createMemoryStorageFactory } from "@streamsy/storage-memory";
import { contentType, streamPath } from "./config.ts";

const encoder = new TextEncoder();
const streamPrefix = "/streams";
const streamId = streamPath.replace(/^\/streams\/?/, "");

export class DemoStreams {
  private readonly protocol = new StreamProtocol(createMemoryStorageFactory());
  private readonly handler = new HttpHandler({
    protocol: this.protocol,
    pathPrefix: streamPrefix,
  });

  async start(): Promise<void> {
    const result = await this.protocol.create(streamId, { contentType });
    if (result.status === "conflict") return;
    if (result.status === "not-found" || result.status === "bad-request") {
      throw new Error(`Unable to create Streamsy demo stream: ${result.status}`);
    }
  }

  async appendJson(value: unknown): Promise<string> {
    const result = await this.protocol.append(streamId, {
      data: encoder.encode(JSON.stringify(value)),
      contentType,
    });

    if (result.status !== "appended" && result.status !== "duplicate") {
      throw new Error(`Unable to append state event to Streamsy stream: ${result.status}`);
    }

    return result.nextOffset;
  }

  async proxy(request: Request): Promise<Response> {
    return this.handler.fetch(request);
  }
}
