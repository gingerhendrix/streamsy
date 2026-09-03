import { Context, Effect, Layer } from "effect";
import {
  AppendStreams,
  CreateStreams,
  ReadStreams,
  type AppendStreamsService,
  type CreateStreamsService,
  type ReadStreamsService,
} from "./streams.ts";

export interface TestStreamsHandlers {
  readonly create: CreateStreamsService;
  readonly read: ReadStreamsService;
  readonly append: AppendStreamsService;
}

export class TestStreams extends Context.Service<TestStreams, TestStreamsHandlers>()(
  "@streamsy/streams/TestStreams",
) {}

/** Supply deterministic create/read/append handlers without constructing a client or transport. */
export const TestStreamsLayer = (handlers: TestStreamsHandlers) =>
  Layer.effectContext(
    Effect.succeed(
      Context.empty().pipe(
        Context.add(CreateStreams, CreateStreams.of(handlers.create)),
        Context.add(ReadStreams, ReadStreams.of(handlers.read)),
        Context.add(AppendStreams, AppendStreams.of(handlers.append)),
        Context.add(TestStreams, TestStreams.of(handlers)),
      ),
    ),
  );
