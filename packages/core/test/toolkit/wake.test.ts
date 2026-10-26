import { expect, it as baseIt, layer } from "@effect/vitest";
import { Array, Context, Effect, Layer, Option, Record, Schema, Stream } from "effect";
import { HttpRouter } from "effect/http";
import { Protocol } from "@streamsy/core/protocol";
import {
  DurableState,
  Http,
  MemoryStorage,
  Offset,
  StreamAddress,
  StreamId,
  Streamsy,
} from "../../src/index.ts";
import { Streams, StreamsReader, StreamsWriter } from "../../src/protocol/index.ts";

const FolderKey = Schema.String;

class Files extends Streamsy.json<Files>()("Wake/Files", "users/:user/folders/:folder/files", {
  params: { user: Schema.String, folder: FolderKey },
  schema: Schema.Struct({ path: Schema.String }),
}) {}

class UserWake extends Streamsy.wake<UserWake>()("Wake/UserWake", "users/:user/wake", {
  params: { user: Schema.String },
}) {}

const streams = Streamsy.group(Files, UserWake);

const ann = { user: "ann", folder: "docs" };

const annId = Files.at(ann).id;

/**
 * Write wake changes through the wake service's wire, as `Projection.wake`
 * does with its producer append. The wake stream is created on first use.
 */
const writeWake = (
  user: string,
  changes: Array.NonEmptyReadonlyArray<DurableState.Change<Streamsy.WakeCollections>>,
) =>
  Effect.gen(function* () {
    const wake = yield* UserWake;
    const address = UserWake.at({ user });

    const context = Context.make(StreamsReader, wake.wire).pipe(
      Context.add(StreamsWriter, wake.wire),
    );

    yield* Streams.create(address).pipe(Effect.provide(context));
    yield* Streams.append(address, DurableState.changes(address, {}, changes)).pipe(
      Effect.provide(context),
    );
  });

const upsert = (stream: string, offset: string) =>
  DurableState.upsert("stream", { stream: StreamId.make(stream), offset: Offset.make(offset) });

layer(MemoryStorage.layer(streams))((it) => {
  it.effect("a wake declaration has the normal State service", () =>
    Effect.gen(function* () {
      const wake = yield* UserWake;
      expect(wake).toHaveProperty("append");
      expect(wake).toHaveProperty("remove");
      expect(UserWake.collections).toBe(Streamsy.wakeCollections);
      expect(wake.route.template).toBe(UserWake.template);

      const files = yield* Files;
      yield* files.create(ann);
      const tail = yield* files.append(ann, [{ path: "a.txt" }]);
      yield* wake.create({ user: "ann" });
      yield* wake.append(
        { user: "ann" },
        DurableState.changes(UserWake.at({ user: "ann" }), {}, [upsert(annId, tail.offset)]),
      );

      const { items } = yield* wake.collect({ user: "ann" });
      const state = DurableState.materialize(DurableState.materialized(UserWake), items);
      expect(Record.fromEntries(state.rows.stream)).toEqual({
        [annId]: { stream: annId, offset: tail.offset },
      });
      // A source write alone writes no row: only a projection writes the wake.
      yield* files.append(ann, [{ path: "b.txt" }]);
      expect(
        DurableState.materialize(
          DurableState.materialized(UserWake),
          (yield* wake.collect({ user: "ann" })).items,
        ).rows.stream.get(annId),
      ).toEqual({
        stream: annId,
        offset: tail.offset,
      });
      yield* wake.remove({ user: "ann" });
      expect((yield* Effect.flip(wake.head({ user: "ann" })))._tag).toBe("StreamNotFound");
    }),
  );
});

layer(MemoryStorage.layer(streams))((it) => {
  it.effect("the typed wake API creates an empty stream; a repeated create answers Exists", () =>
    Effect.gen(function* () {
      const wake = yield* UserWake;
      const before = yield* Effect.flip(wake.head({ user: "cy" }));

      expect(before._tag).toBe("StreamNotFound");

      expect((yield* wake.create({ user: "cy" }))._tag).toBe("Created");
      expect((yield* wake.create({ user: "cy" }))._tag).toBe("Exists");

      const { items, offset } = yield* wake.collect({ user: "cy" });
      const state = DurableState.materialize(DurableState.materialized(UserWake), items);
      expect(state.rows.stream.size).toBe(0);
      expect(offset).toBe((yield* wake.head({ user: "cy" })).nextOffset);

      // A projection's create on the same stream later also answers Exists.
      const created = yield* Streams.create(UserWake.at({ user: "cy" })).pipe(
        Effect.provideService(StreamsWriter, wake.wire),
      );

      expect(created._tag).toBe("Exists");
    }),
  );
});

layer(MemoryStorage.layer(streams))((it) => {
  it.effect("followWake collapses rows, skips saved cursors, and reports removals", () =>
    Effect.gen(function* () {
      const files = yield* Files;
      const a = { user: "ann", folder: "a" };
      const b = { user: "ann", folder: "b" };
      yield* files.create(a);
      const aTail = yield* files.append(a, [{ path: "1" }]);
      yield* files.create(b);
      const bFirst = yield* files.append(b, [{ path: "1" }]);
      const bTail = yield* files.append(b, [{ path: "2" }]);
      yield* files.remove(a);
      yield* writeWake("ann", [
        upsert(Files.at(a).id, aTail.offset),
        upsert(Files.at(b).id, bFirst.offset),
        upsert(Files.at(b).id, bTail.offset),
        DurableState.remove("stream", Files.at(a).id),
      ]);

      const saved = new Map([[Files.at(b).id, bTail.offset]]);

      const [batch] = yield* Streamsy.followWake(
        UserWake,
        { user: "ann" },
        { cursors: saved },
      ).pipe(Stream.take(1), Stream.runCollect);

      expect(batch?.items).toEqual([Streamsy.WakeItem.Removed({ stream: Files.at(a).id })]);
      expect(aTail._tag).toBe("Appended");

      const [cold] = yield* Streamsy.followWake(
        UserWake,
        { user: "ann" },
        { cursors: new Map() },
      ).pipe(Stream.take(1), Stream.runCollect);

      expect(cold?.items).toEqual([
        Streamsy.WakeItem.Dirty({
          stream: Files.at(b).id,
          from: "-1",
          offset: bTail.offset,
          closed: false,
        }),
        Streamsy.WakeItem.Removed({ stream: Files.at(a).id }),
      ]);
      expect(cold?.offset).toBe((yield* (yield* UserWake).head({ user: "ann" })).nextOffset);
    }),
  );
});

const protocol = MemoryStorage.layerProtocol();

layer(Protocol.bind(protocol, streams).pipe(Layer.provideMerge(protocol)))((it) => {
  it.effect("followWake applies reset and snapshot controls from the wake stream", () =>
    Effect.gen(function* () {
      const files = yield* Files;
      yield* files.create(ann);
      const tail = yield* files.append(ann, [{ path: "1" }]);
      const wakeAddress = UserWake.at({ user: "ann" });
      // A future compactor writes controls with the raw protocol writer of the host.
      const writer = yield* StreamsWriter;
      const kept = Files.at({ user: "ann", folder: "kept" }).id;
      yield* writer.create(wakeAddress.id, { contentType: wakeAddress.contentType });

      const write = (events: ReadonlyArray<unknown>) =>
        Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown))(events).pipe(
          Effect.flatMap((text) =>
            writer.append(wakeAddress.id, {
              data: new TextEncoder().encode(text),
              contentType: "application/json",
            }),
          ),
        );

      yield* write([
        StreamAddress.reset(),
        StreamAddress.snapshotStart(),
        {
          type: "stream",
          key: kept,
          value: { stream: kept, offset: tail.offset },
          headers: { operation: "upsert" },
        },
        StreamAddress.snapshotEnd(),
      ]);

      const batches = yield* Streamsy.followWake(
        UserWake,
        { user: "ann" },
        { cursors: new Map([[kept, tail.offset]]) },
      ).pipe(Stream.take(1), Stream.runCollect);

      const items = Array.flatMap(batches, (batch) => batch.items);

      expect(items).toEqual([
        Streamsy.WakeItem.Reset(),
        Streamsy.WakeItem.Dirty({ stream: kept, from: "-1", offset: tail.offset, closed: false }),
        Streamsy.WakeItem.Snapshot({ streams: new Set([kept]) }),
      ]);
    }),
  );
});

const offsetAt = (n: number) => `000000000000000${n}_0000000000000000`;

/** One raw JSON wake row, as a client gets it in an HTTP page. */
const rawRow = (key: string, at: number, closed?: true) => ({
  type: "stream",
  key,
  value: closed
    ? { stream: key, offset: offsetAt(at), closed }
    : { stream: key, offset: offsetAt(at) },
  headers: { operation: "upsert" },
});

const decodePage = Schema.decodeUnknownEffect(Schema.Array(Streamsy.WakeMessage));

baseIt.effect("wakeActions turns decoded wake pages into actions and carries a snapshot", () =>
  Effect.gen(function* () {
    const first = yield* decodePage([
      rawRow("a", 1),
      rawRow("b", 2),
      rawRow("a", 3),
      StreamAddress.snapshotStart(),
      rawRow("c", 1),
    ]);

    const saved = new Map([[StreamId.make("b"), offsetAt(2)]]);
    const one = Streamsy.wakeActions(first, { cursors: saved });

    expect(one.items).toEqual([
      Streamsy.WakeItem.Dirty({
        stream: StreamId.make("a"),
        from: "-1",
        offset: offsetAt(3),
        closed: false,
      }),
      Streamsy.WakeItem.Dirty({
        stream: StreamId.make("c"),
        from: "-1",
        offset: offsetAt(1),
        closed: false,
      }),
    ]);
    expect(one.snapshot).toEqual(new Set(["c"]));

    const second = yield* decodePage([rawRow("b", 2, true), StreamAddress.snapshotEnd()]);

    const two = Streamsy.wakeActions(second, {
      cursors: saved,
      snapshot: one.snapshot,
    });

    expect(two.items).toEqual([
      Streamsy.WakeItem.Dirty({
        stream: StreamId.make("b"),
        from: offsetAt(2),
        offset: offsetAt(2),
        closed: true,
      }),
      Streamsy.WakeItem.Snapshot({ streams: new Set([StreamId.make("c"), StreamId.make("b")]) }),
    ]);
    expect(two.snapshot).toBeUndefined();
    // The input snapshot is not changed.
    expect(one.snapshot).toEqual(new Set(["c"]));

    // A change of another State type does not decode.
    const other = yield* Effect.flip(decodePage([{ ...rawRow("a", 1), type: "file" }]));

    expect(other._tag).toBe("SchemaError");
  }),
);

baseIt.effect("a wake declares its own params and names no sources", () =>
  Effect.sync(() => {
    expect(UserWake.at({ user: "ann" }).id).toBe("users/ann/wake");
    expect(UserWake.parse("users/bob/wake")).toEqual(Option.some({ user: "bob" }));

    expect(() =>
      Streamsy.wake()("Wake/Bad", "teams/:team/wake", {
        // @ts-expect-error -- `:team` needs a codec.
        params: {},
      }),
    ).toThrow();

    class Overlapping extends Streamsy.json<Overlapping>()("Wake/Over", "users/:user/:any", {
      params: { user: Schema.String, any: Schema.String },
      schema: Schema.String,
    }) {}

    expect(() => Streamsy.group(Overlapping, UserWake)).toThrow(/overlap/);
  }),
);

baseIt.live("HTTP applies a literal access policy to wake and source streams", () =>
  Effect.gen(function* () {
    const edge = yield* Effect.acquireRelease(
      Effect.sync(() =>
        HttpRouter.toWebHandler(
          Http.routes(streams, { prefix: "/streams", access: "full" }).pipe(
            HttpRouter.provideRequest(MemoryStorage.layer(streams)),
          ),
          { disableLogger: true },
        ),
      ),
      (acquired) => Effect.promise(() => acquired.dispose()),
    );

    const request = (path: string, method: string, body?: string) =>
      Effect.promise(() =>
        edge.handler(
          new Request(`http://host/streams/${path}`, {
            method,
            // oxlint-disable-next-line unicorn/no-invalid-fetch-options -- Only writes pass a body.
            body,
            headers: { "content-type": "application/json" },
          }),
        ),
      );

    const body = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown))([row(annId)]);

    expect((yield* request(annId, "PUT")).status).toBe(201);
    expect((yield* request("users/ann/wake", "PUT")).status).toBe(201);
    expect((yield* request("users/ann/wake", "POST", body)).status).toBe(204);
    expect((yield* request("users/ann/wake", "DELETE")).status).toBe(204);
    expect((yield* request("users/ann/wake?offset=-1", "GET")).status).toBe(404);
  }),
);

baseIt.live("an internal mount opens wake writes; the public mount still refuses them", () =>
  Effect.gen(function* () {
    const storage = MemoryStorage.layer(streams);

    const routes = Layer.mergeAll(
      Http.routes(streams, { prefix: "/streams", access: "read" }),
      Http.routes(streams, {
        prefix: "/__wake",
        access: (stream) => (stream === UserWake ? "full" : "read"),
      }),
    );

    const edge = yield* Effect.acquireRelease(
      Effect.sync(() =>
        HttpRouter.toWebHandler(routes.pipe(HttpRouter.provideRequest(storage)), {
          disableLogger: true,
        }),
      ),
      (acquired) => Effect.promise(() => acquired.dispose()),
    );

    const row = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown))([
      {
        type: "stream",
        key: annId,
        value: { stream: annId, offset: "0000000000000001_0000000000000000" },
        headers: { operation: "upsert" },
      },
    ]);

    const producer = { "producer-id": "files-wake", "producer-epoch": "1", "producer-seq": "0" };

    const request = (path: string, method: string, headers: Record<string, string> = {}) =>
      Effect.promise(() =>
        edge.handler(
          new Request(`http://host${path}`, {
            method,
            // oxlint-disable-next-line unicorn/no-invalid-fetch-options -- Only writes pass a body.
            body: method === "POST" ? row : undefined,
            headers: { "content-type": "application/json", ...headers },
          }),
        ),
      );

    expect((yield* request("/streams/users/ann/wake", "PUT")).status).toBe(405);
    expect((yield* request("/__wake/users/ann/wake", "PUT")).status).toBe(201);
    expect((yield* request("/streams/users/ann/wake", "POST", producer)).status).toBe(405);
    expect((yield* request("/__wake/users/ann/wake", "POST", producer)).status).toBe(200);
    // A retry of the same producer tuple is a duplicate and writes nothing.
    expect((yield* request("/__wake/users/ann/wake", "POST", producer)).status).toBe(204);
    expect((yield* request(`/__wake/${annId}`, "PUT")).status).toBe(405);

    const read = yield* request("/streams/users/ann/wake?offset=-1", "GET");
    expect(read.status).toBe(200);

    const events = yield* Schema.decodeEffect(
      Schema.fromJsonString(Schema.Array(Schema.Struct({ key: Schema.String }))),
    )(yield* Effect.promise(() => read.text()));

    expect(Array.map(events, (event) => event.key)).toEqual([annId]);
  }),
);

const row = (key: string) => ({
  type: "stream" as const,
  key,
  value: { stream: StreamId.make(key), offset: Offset.make("0000000000000001_0000000000000000") },
  headers: { operation: "upsert" as const },
});

const drop = (key: string) => ({
  type: "stream" as const,
  key,
  headers: { operation: "delete" as const },
});

baseIt.effect("materialize folds upserts, deletes, reset and a snapshot over two calls", () =>
  Effect.sync(() => {
    const empty = DurableState.materialized(UserWake.at({ user: "ann" }));

    const one = DurableState.materialize(empty, [row("a"), row("b"), drop("a")]);
    expect([...one.rows.stream.keys()]).toEqual(["b"]);
    expect(empty.rows.stream.size).toBe(0);

    const open = DurableState.materialize(one, [StreamAddress.snapshotStart(), row("c")]);
    expect(open.snapshot?.size).toBe(1);
    expect([...open.rows.stream.keys()]).toEqual(["b", "c"]);

    const closed = DurableState.materialize(open, [StreamAddress.snapshotEnd()]);
    expect([...closed.rows.stream.keys()]).toEqual(["c"]);
    expect(closed.snapshot).toBeUndefined();

    const cleared = DurableState.materialize(closed, [StreamAddress.reset(), row("d")]);
    expect([...cleared.rows.stream.keys()]).toEqual(["d"]);
    expect([...closed.rows.stream.keys()]).toEqual(["c"]);
  }),
);
