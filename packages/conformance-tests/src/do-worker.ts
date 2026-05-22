import { HttpHandler, StreamProtocol } from "@streamsy/core";
import {
  DurableObjectStreamStorage as StreamStorage,
  DurableObjectStreamStoreAdapter,
} from "@streamsy/storage-durable-object";

export { StreamStorage };

interface Env {
  STREAM_DO: DurableObjectNamespace<StreamStorage>;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const store = new DurableObjectStreamStoreAdapter(env.STREAM_DO);
    const protocol = new StreamProtocol(store);
    const handler = new HttpHandler({ protocol });
    return handler.fetch(request);
  },
} satisfies ExportedHandler<Env>;
