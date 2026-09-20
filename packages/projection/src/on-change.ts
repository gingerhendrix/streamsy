/* oxlint-disable typescript/no-explicit-any, typescript/no-unsafe-type-assertion, anti-slop/no-unsafe-dictionary-type, anti-slop/require-safety-comment-for-type-assertion -- The overload implementation erases member parameter, success, error, and service types after the public overloads have checked them; runtime dispatch is by the Family tag. */
import { Effect, Fiber, Option, Queue, Stream, type Scope } from "effect";
import { Storage, StreamsReader, type StreamRef, type StreamRoute } from "@streamsy/core";
import type { InputMap } from "./batch.ts";
import type { Family } from "./family.ts";
import { ProjectionFault } from "./fault.ts";
import type { Fused, Pinned } from "./projection.ts";
import { validateOptions, type RunOptions } from "./read.ts";
import { serialized } from "./serialized.ts";
import type { Host, Progress } from "./run.ts";

export interface OnChangeOptions extends RunOptions {}

type AnyProjection =
  | Fused<InputMap, unknown, unknown>
  | Pinned<InputMap, unknown, unknown, unknown>;
type RuntimeProjection =
  | Fused<InputMap, ProjectionFault, Host>
  | Pinned<InputMap, unknown, ProjectionFault, Host>;
type FamilyParams<F> = F extends Family<infer Codecs, any> ? StreamRoute.Params<Codecs> : never;
type FamilyMember<F> = F extends Family<any, infer Member> ? Member : never;
type MemberError<Member> =
  Member extends Fused<infer _Inputs, infer E, infer _R>
    ? E
    : Member extends Pinned<infer _Inputs, infer _Output, infer E, infer _R>
      ? E
      : never;
type MemberRequirements<Member> =
  Member extends Fused<infer _Inputs, infer _E, infer R>
    ? R
    : Member extends Pinned<infer _Inputs, infer _Output, infer _E, infer R>
      ? R
      : never;

const unsupported = (input: string, ref: StreamRef.StreamRef<unknown>) =>
  new ProjectionFault({
    phase: "read",
    reason: "unsupported-composition",
    input,
    message: `${ref.id} has no same-owner change feed`,
  });

const watch = <Inputs extends InputMap, O, E, R>(
  projection: Fused<Inputs, E, R> | Pinned<Inputs, O, E, R>,
  storage: typeof Storage.Service,
  options: OnChangeOptions,
): Effect.Effect<Progress, E | ProjectionFault, R | Host> =>
  Effect.scoped(
    Effect.gen(function* () {
      const wake = yield* Queue.make<void>({
        capacity: 1,
        strategy: "dropping",
      });
      const closed = new Set<string>();
      const inputs = Object.entries(projection.inputs);
      const reader = yield* StreamsReader;
      yield* Effect.forEach(inputs, ([name, ref]) =>
        Effect.gen(function* () {
          const readable = yield* reader.head(ref.id).pipe(
            Effect.as(true),
            Effect.catchTag("StreamNotFound", () => Effect.succeed(false)),
            Effect.mapError((cause) =>
              cause._tag === "StreamGone"
                ? new ProjectionFault({
                    phase: "read",
                    reason: "history-unavailable",
                    input: name,
                    message: `Required history of ${ref.id} is unavailable`,
                    cause,
                  })
                : new ProjectionFault({
                    phase: "read",
                    reason: "storage-failure",
                    input: name,
                    message: `Cannot inspect ${ref.id}`,
                    cause,
                  }),
            ),
          );
          if (readable) {
            const owned = yield* storage.record(ref.id).pipe(
              Effect.mapError(
                (cause) =>
                  new ProjectionFault({
                    phase: "read",
                    reason: "storage-failure",
                    input: name,
                    message: `Cannot inspect ${ref.id}`,
                    cause,
                  }),
              ),
            );
            if (Option.isNone(owned)) return yield* unsupported(name, ref);
          }
          return undefined;
        }),
      );
      const subscriptions = inputs.map(([name, ref]) =>
        storage.changes(ref.id).pipe(
          Stream.takeUntil((snapshot) => snapshot.closed),
          Stream.runForEach((snapshot) =>
            Effect.gen(function* () {
              if (snapshot.closed) closed.add(name);
              if (snapshot.present) yield* Queue.offer(wake, undefined);
            }),
          ),
          Effect.mapError(
            (cause) =>
              new ProjectionFault({
                phase: "read",
                reason: "storage-failure",
                input: name,
                message: `Cannot watch ${ref.id}`,
                cause,
              }),
          ),
          Effect.andThen(Effect.never),
        ),
      );
      const runner = Effect.gen(function* () {
        while (true) {
          yield* Queue.take(wake);
          let result = yield* serialized(projection, options);
          while (result.status === "limit-reached") result = yield* serialized(projection, options);
          if (result.status === "source-closed" && closed.size === inputs.length) return result;
        }
      });
      return yield* Effect.raceAllFirst([runner, ...subscriptions]);
    }),
  );

/**
 * Watches same-owner inputs. A missing input cannot be ownership-checked until it is created;
 * all other head failures surface as `read / storage-failure`.
 */
export function onChange<Inputs extends InputMap, O, E, R>(
  projection: Fused<Inputs, E, R> | Pinned<Inputs, O, E, R>,
  options?: OnChangeOptions,
): Effect.Effect<
  Fiber.Fiber<Progress, E | ProjectionFault>,
  ProjectionFault,
  R | Host | Scope.Scope
>;
export function onChange<F extends Family<any, any>>(
  family: F,
  members: ReadonlyArray<FamilyParams<F>>,
  options?: OnChangeOptions,
): Effect.Effect<
  ReadonlyArray<Fiber.Fiber<Progress, MemberError<FamilyMember<F>> | ProjectionFault>>,
  ProjectionFault,
  MemberRequirements<FamilyMember<F>> | Host | Scope.Scope
>;
export function onChange(
  target: AnyProjection | Family<any, any>,
  membersOrOptions: ReadonlyArray<Record<string, unknown>> | OnChangeOptions = {},
  maybeOptions: OnChangeOptions = {},
): Effect.Effect<
  Fiber.Fiber<any, any> | ReadonlyArray<Fiber.Fiber<any, any>>,
  ProjectionFault,
  any
> {
  return Effect.gen(function* () {
    const options = Array.isArray(membersOrOptions)
      ? maybeOptions
      : (membersOrOptions as OnChangeOptions);
    yield* validateOptions(options);
    const available = yield* Effect.serviceOption(Storage);
    if (Option.isNone(available)) {
      if (
        target._tag === "Family" &&
        Array.isArray(membersOrOptions) &&
        membersOrOptions.length === 0
      )
        return yield* new ProjectionFault({
          phase: "read",
          reason: "unsupported-composition",
          message: "The projection family has no same-owner change feed",
        });
      const firstParams = Array.isArray(membersOrOptions) ? (membersOrOptions[0] ?? {}) : {};
      const member = target._tag === "Family" ? target.member(firstParams) : target;
      const [name, ref] = Object.entries(member.inputs as InputMap)[0] ?? ["input", undefined];
      if (ref === undefined)
        return yield* new ProjectionFault({
          phase: "read",
          reason: "unsupported-composition",
          message: "The projection has no same-owner change feed",
        });
      return yield* unsupported(name, ref);
    }
    if (target._tag === "Family") {
      const members = membersOrOptions as ReadonlyArray<Record<string, unknown>>;
      return yield* Effect.forEach(members, (params) => {
        const projection = target.member(params) as RuntimeProjection;
        return watch(projection, available.value, maybeOptions).pipe(Effect.forkScoped);
      });
    }
    const erased: unknown = target;
    const projection = erased as RuntimeProjection;
    return yield* watch(projection, available.value, membersOrOptions as OnChangeOptions).pipe(
      Effect.forkScoped,
    );
  });
}
