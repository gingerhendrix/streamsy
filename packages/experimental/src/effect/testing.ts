import { Context, Effect, Layer } from "effect";
import {
  AppendStreams,
  ReadStreams,
  type AppendStreamsShape,
  type ReadStreamsShape,
} from "./streams.ts";

export interface TestStreamsShape {
  readonly read: ReadStreamsShape;
  readonly append: AppendStreamsShape;
}

export class TestStreams extends Context.Service<TestStreams, TestStreamsShape>()(
  "@streamsy/experimental/TestStreams",
) {}

/** Supply deterministic read/append handlers without constructing a client or transport. */
export const TestStreamsLayer = (handlers: TestStreamsShape) =>
  Layer.effectContext(
    Effect.succeed(
      Context.empty().pipe(
        Context.add(ReadStreams, ReadStreams.of(handlers.read)),
        Context.add(AppendStreams, AppendStreams.of(handlers.append)),
        Context.add(TestStreams, TestStreams.of(handlers)),
      ),
    ),
  );
