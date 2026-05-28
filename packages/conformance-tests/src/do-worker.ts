import { HttpHandler, StreamProtocol } from "@streamsy/core";
import {
  createDurableObjectStreamFactory,
  DurableObjectStreamStorage as StreamStorage,
} from "@streamsy/storage-durable-object";

export { StreamStorage };

interface Env {
  STREAM_DO: DurableObjectNamespace<StreamStorage>;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const factory = createDurableObjectStreamFactory({ namespace: env.STREAM_DO });
    const protocol = new StreamProtocol({ storage: { factory } });
    const handler = new HttpHandler({ protocol });
    return handler.fetch(request);
  },
} satisfies ExportedHandler<Env>;
