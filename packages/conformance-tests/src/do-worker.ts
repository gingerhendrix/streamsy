import { HttpHandler, StreamProtocol } from "@streamsy/core";
import {
  createDurableObjectStorageAdapter,
  DurableObjectStreamStorage as StreamStorage,
} from "@streamsy/storage-durable-object";

export { StreamStorage };

interface Env {
  STREAM_DO: DurableObjectNamespace<StreamStorage>;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const adapter = createDurableObjectStorageAdapter({ namespace: env.STREAM_DO });
    const protocol = new StreamProtocol({ storage: { adapter }, longPollTimeoutMs: 1_500 });
    const handler = new HttpHandler({ protocol });
    return handler.fetch(request);
  },
} satisfies ExportedHandler<Env>;
