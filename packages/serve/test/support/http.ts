import { Context, Effect, Layer } from "effect";
import { FetchHttpClient, HttpClient, HttpClientRequest } from "effect/http";

/** Buffer a finite HTTP response for parity assertions at the Promise test boundary. */
export const request = (input: string | URL, init?: RequestInit): Promise<Response> =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const client = Context.get(
          yield* Layer.build(FetchHttpClient.layer),
          HttpClient.HttpClient,
        );

        const web = new Request(input, init);
        // fromWeb normalizes an empty body by removing Content-Type. Fork parity
        // cases deliberately send that header without a body, so restore the wire headers.
        const response = yield* client.execute(
          HttpClientRequest.fromWeb(web).pipe(HttpClientRequest.setHeaders(web.headers)),
        );

        const body =
          web.method === "HEAD" || [204, 205, 304].includes(response.status)
            ? null
            : yield* response.arrayBuffer;

        return new Response(body, { status: response.status, headers: response.headers });
      }),
    ),
  );
