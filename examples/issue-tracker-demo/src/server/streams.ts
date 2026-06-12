import {
  createHttpHandler,
  createMemoryStreamFactory,
  createStreamProtocol,
  type ProtocolStream,
} from "@streamsy/core";
import { contentType, streamPath } from "./config.ts";

const encoder = new TextEncoder();
const streamPrefix = "/streams";
const streamId = streamPath.replace(/^\/streams\/?/, "");

export class DemoStreams {
  private readonly protocol = createStreamProtocol({
    storage: { factory: createMemoryStreamFactory() },
  });
  private readonly handler = createHttpHandler({
    protocol: this.protocol,
    pathPrefix: streamPrefix,
  });
  private stream: ProtocolStream | undefined;

  async start(): Promise<void> {
    const result = await this.protocol.create(streamId, { contentType });
    if (result.status !== "created" && result.status !== "exists") {
      throw new Error(`Unable to create Streamsy demo stream: ${result.status}`);
    }

    this.stream = result.stream;
  }

  async appendJson(value: unknown): Promise<string> {
    if (!this.stream) {
      const result = await this.protocol.get(streamId);
      if (result.status !== "ok") {
        throw new Error(`Unable to resolve Streamsy demo stream: ${result.status}`);
      }
      this.stream = result.stream;
    }

    const result = await this.stream.append({
      data: encoder.encode(JSON.stringify(value)),
      contentType,
    });

    if (result.status !== "appended" && result.status !== "duplicate") {
      throw new Error(`Unable to append state event to Streamsy stream: ${result.status}`);
    }

    return result.nextOffset;
  }

  async proxy(request: Request): Promise<Response> {
    console.log(`Proxying stream request: ${request.method} ${request.url}`);
    return this.handler.fetch(request);
  }
}
