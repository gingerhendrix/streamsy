/**
 * A Node host for the listener start-gap test. It runs under `node`, not Bun.
 * The listener binds the port first. The routes serve only after the parent
 * writes a line to stdin, so the test can send a request inside the gap.
 */
import { NodeRuntime } from "@effect/platform-node";
import { Effect, Layer } from "effect";
import { HttpServer, HttpServerResponse } from "effect/http";
import { NetAddress } from "effect/net";
import { listener } from "../../src/node.ts";

const waitForLine = Effect.callback<void>((resume) => {
  process.stdin.once("data", () => resume(Effect.void));
});

const Gap = Layer.effectDiscard(
  Effect.gen(function* () {
    const server = yield* HttpServer.HttpServer;
    const port = NetAddress.isInetAddress(server.address) ? server.address.port : 0;

    process.stdout.write(`listening ${port}\n`);
    yield* waitForLine;
    yield* server.serve(Effect.succeed(HttpServerResponse.text("served")));
    process.stdout.write("serving\n");
  }),
).pipe(Layer.provide(listener({ port: 0 })));

NodeRuntime.runMain(Layer.launch(Gap));
