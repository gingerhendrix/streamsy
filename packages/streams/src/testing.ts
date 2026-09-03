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

/** Provide one fully composed layer at the Vitest or Bun executable test boundary. */
export function provideTestLayers<A, E, R, ROut, E2, RIn>(
  program: Effect.Effect<A, E, R>,
  layer: Layer.Layer<ROut, E2, RIn>,
): Effect.Effect<A, E | E2, RIn | Exclude<R, ROut>> {
  // @effect-diagnostics-next-line strictEffectProvide:off -- Vitest and Bun execute the returned Effect at this shared test boundary.
  return Effect.provide(program, layer);
}
