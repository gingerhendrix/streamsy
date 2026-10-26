/**
 * Wake streams: one Durable State stream that indexes the writes of a family of
 * source streams.
 *
 * A wake declaration is a State stream with one `stream` collection. Each row
 * is the latest known state of one source stream. A projection writes the rows:
 * `Projection.wake` in `@streamsy/projection` reads one source stream and
 * appends its row with a producer tuple, on the same owner or another one. A
 * wake stream on another owner must exist first: its owner creates it at start.
 * The typed service is the normal State service.
 */
import { Data, Effect, Option, Schema, Stream, type Context } from "effect";
import { compare } from "../offset/index.ts";
import { Offset, StreamId } from "../schema/index.ts";
import * as Route from "./route.ts";
import * as StreamAddress from "./address.ts";
import type * as Streams from "./streams.ts";
import { state, type Api } from "./streamsy.ts";

/** One row of a wake stream: the latest known state of one source stream. */
export const WakeRow = Schema.Struct({
  /** Canonical id of the source stream. It is also the State key. */
  stream: StreamId,
  /** Source tail after the write. The same value as a reader's saved cursor. */
  offset: Offset,
  /** Present when the source stream is closed. */
  closed: Schema.optionalKey(Schema.Literal(true)),
});

export type WakeRow = typeof WakeRow.Type;

/** The one collection of every wake stream. Its State `type` is `stream`. */
export const wakeCollections = { stream: { schema: WakeRow, key: "stream" } } as const;

export type WakeCollections = typeof wakeCollections;

/** One item of a wake stream. */
export type WakeEvent = StreamAddress.CollectionsEvent<WakeCollections>;

/**
 * The codec of one wake stream message: a `stream` change or a State control.
 * A client that decodes raw JSON pages uses it in place of a typed service.
 */
export const WakeMessage: Schema.Codec<
  WakeEvent,
  StreamAddress.CollectionsEncodedEvent<WakeCollections>
> = Schema.Union([
  StreamAddress.stateChange({ type: "stream", schema: WakeRow }),
  StreamAddress.stateControl,
]);

/**
 * Declare a wake stream: a State stream of `WakeRow`s at a template.
 *
 * ```ts
 * class UserWake extends Streamsy.wake<UserWake>()("App/UserWake", "users/:user/wake", {
 *   params: { user: UserKey },
 * }) {}
 * ```
 *
 * The declaration names no sources. A `Projection.wake` binding links one
 * source stream to one wake stream, with params of its own.
 */
export const wake =
  <Self>() =>
  <const Name extends string, const Template extends string, C extends Route.ParamCodecs>(
    name: Name,
    template: Template,
    options: { readonly params: C & Route.ExactTemplateParams<Template, C> },
  ) =>
    state<Self>()<Name, Template, C, WakeCollections>(name, template, {
      params: options.params,
      collections: wakeCollections,
    });

/** One action for a consumer of a wake stream. */
export type WakeItem = Data.TaggedEnum<{
  /** Read `stream` from `from` up to at least `offset`, then save its cursor. */
  Dirty: {
    readonly stream: StreamId;
    readonly from: string;
    readonly offset: string;
    readonly closed: boolean;
  };
  /** The stream was removed. Drop its cursor and its local data. */
  Removed: { readonly stream: StreamId };
  /** Drop every cursor. The items after this one start each stream from `-1`. */
  Reset: {};
  /** A complete snapshot ended. Drop each stream that `streams` does not hold. */
  Snapshot: { readonly streams: ReadonlySet<StreamId> };
}>;

/** Constructors and matchers for `WakeItem`. */
export const WakeItem = Data.taggedEnum<WakeItem>();

/** One batch of wake actions. Save `offset` as the wake cursor after every item is handled. */
export interface WakeBatch {
  readonly items: ReadonlyArray<WakeItem>;
  readonly offset: string;
  readonly upToDate: boolean;
}

export interface FollowWakeOptions extends Omit<Streams.FollowOptions, "offset"> {
  /** The saved wake cursor. Default: `-1`, a cold start. */
  readonly from?: string;
  /** Saved source cursors. Read again for each batch, so the consumer may update the map. */
  readonly cursors: ReadonlyMap<StreamId, string>;
}

export interface WakeActionsOptions {
  /** Saved source cursors. A missing entry starts from `-1`. */
  readonly cursors: ReadonlyMap<StreamId, string>;
  /** The streams of a snapshot that an earlier batch left open. */
  readonly snapshot?: ReadonlySet<StreamId> | undefined;
}

/** The actions of one wake batch, and the snapshot state for the next batch. */
export interface WakeActions {
  readonly items: ReadonlyArray<WakeItem>;
  /** The streams of a snapshot that is still open. Pass it to the next call. */
  readonly snapshot: ReadonlySet<StreamId> | undefined;
}

interface Pending {
  removed: boolean;
  dirty: Option.Option<{
    readonly from: string;
    readonly offset: string;
    readonly closed: boolean;
  }>;
}

/**
 * The actions for one batch of wake messages. The function is pure: it reads
 * no stream and changes none of its inputs. Rows for one stream collapse to one
 * item; a delete then an upsert of one stream gives `Removed`, then `Dirty` from
 * `-1`. Pass the returned `snapshot` to the call for the next batch.
 *
 * `followWake` uses it over a typed service. A client with its own HTTP reader
 * decodes each page with `WakeMessage` and calls it directly.
 */
export function wakeActions(
  events: ReadonlyArray<WakeEvent>,
  options: WakeActionsOptions,
): WakeActions {
  const { cursors } = options;
  const items: Array<WakeItem> = [];
  let pending = new Map<StreamId, Pending>();
  let reset = false;
  let open = options.snapshot === undefined ? undefined : new Set(options.snapshot);

  const flush = () => {
    for (const [stream, entry] of pending) {
      if (entry.removed) items.push(WakeItem.Removed({ stream }));

      if (Option.isSome(entry.dirty)) items.push(WakeItem.Dirty({ stream, ...entry.dirty.value }));
    }

    pending = new Map();
  };

  for (const event of events) {
    if (StreamAddress.isControl(event)) {
      const control = event.headers.control;

      if (control === "reset") {
        pending = new Map();
        items.length = 0;
        items.push(WakeItem.Reset());
        reset = true;
        open = undefined;
      } else if (control === "snapshot-start") open = new Set();
      else if (open !== undefined) {
        flush();
        items.push(WakeItem.Snapshot({ streams: open }));
        open = undefined;
      }

      continue;
    }

    const stream = StreamId.make(event.key);
    const prior = pending.get(stream);
    // Re-inserting moves the stream to the end, so items keep write order.
    pending.delete(stream);

    if (!StreamAddress.isUpsert(event)) {
      pending.set(stream, { removed: true, dirty: Option.none() });
      open?.delete(stream);
      continue;
    }

    const row: WakeRow = event.value;
    const removed = prior?.removed ?? false;
    const base = removed || reset ? "-1" : (cursors.get(stream) ?? "-1");
    const dirty = compare(row.offset, base) > 0 || row.closed === true;

    pending.set(stream, {
      removed,
      dirty: dirty
        ? Option.some({ from: base, offset: row.offset, closed: row.closed === true })
        : Option.none(),
    });
    open?.add(stream);
  }

  flush();

  return { items, snapshot: open };
}

/**
 * Follow one wake stream and give the actions for each batch: which source
 * streams to read, from which cursor, and which to drop. The helper reads no
 * source stream. Save each source cursor first and the wake cursor (`offset`)
 * last; a stop between the two reads the same rows again, and the offset check
 * skips the streams that were already read.
 */
export function followWake<Self, P>(
  declaration: Context.Key<Self, Api<P, WakeEvent>>,
  params: P,
  options: FollowWakeOptions,
): Stream.Stream<WakeBatch, Streams.FollowFailure, Self> {
  const { from, cursors, ...follow } = options;

  return Stream.unwrap(
    Effect.map(Effect.service(declaration), (service) =>
      service.follow(params, { ...follow, offset: from }).pipe(
        Stream.mapAccum(
          (): ReadonlySet<StreamId> | undefined => undefined,
          (snapshot, batch) => {
            const next = wakeActions(batch.items, { cursors, snapshot });

            const out: WakeBatch = {
              items: next.items,
              offset: batch.nextOffset,
              upToDate: batch.upToDate,
            };

            return [next.snapshot, [out]] as const;
          },
        ),
      ),
    ),
  );
}
