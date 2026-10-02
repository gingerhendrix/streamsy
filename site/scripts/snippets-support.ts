/**
 * Support for `check-snippets.ts`. A docs page often shows a fragment that uses names
 * from an earlier block or from the prose. These entries give the compiler those names.
 * Blocks are numbered from 1 in page order, counting every `ts` fence on the page.
 * When a page gains or loses a `ts` fence, move its entries to the new numbers.
 */
export interface PageSupport {
  /** Save block N under this module name, so later blocks can import it. */
  readonly renames?: Readonly<Record<number, string>>;
  /** Blocks that are not compiled, with the reason in a comment beside the entry. */
  readonly skip?: ReadonlyArray<number>;
  /** Lines put before block N: imports and declarations that the page leaves out. */
  readonly preludes?: Readonly<Record<number, string>>;
  /** Extra modules in the page folder, by file name. */
  readonly modules?: Readonly<Record<string, string>>;
}

const shopStreams = `import { Schema } from "effect";
import { Streamsy } from "@streamsy/core";
export const OrderEvent = Schema.Struct({ orderId: Schema.String, amount: Schema.Finite });
export const OrderRow = Schema.Struct({ orderId: Schema.String, total: Schema.Finite });
export class Orders extends Streamsy.json<Orders>()("Shop/Orders", "orders", { params: {}, schema: OrderEvent }) {}
export class OrderTotals extends Streamsy.json<OrderTotals>()("Shop/OrderTotals", "order-totals", { params: {}, schema: OrderRow }) {}
export class OrderEvents extends Streamsy.json<OrderEvents>()("Shop/OrderEvents", "orders/:orderId/events", { params: { orderId: Schema.String }, schema: OrderEvent }) {}
export class Drafts extends Streamsy.json<Drafts>()("Shop/Drafts", "drafts/:draftId", { params: { draftId: Schema.String }, schema: Schema.String }) {}
export class Journal extends Streamsy.json<Journal>()("Shop/Journal", "journal", { params: {}, schema: Schema.String }) {}
export class Events extends Streamsy.json<Events>()("Game/Events", "events/:seat", { params: { seat: Schema.String }, schema: Schema.String }) {}
export const shop = Streamsy.group(Orders, OrderTotals, OrderEvents);
export const game = Streamsy.group(Events);
`;
const notesStreams = `import { Schema } from "effect";
import { Streamsy } from "@streamsy/core";
export const Note = Schema.Struct({ id: Schema.String, text: Schema.String });
export class Notes extends Streamsy.json<Notes>()("Notes/Notes", "notes", { params: {}, schema: Note }) {}
export class Drafts extends Streamsy.json<Drafts>()("Notes/Drafts", "drafts/:draftId", { params: { draftId: Schema.String }, schema: Note }) {}
const Project = Schema.Struct({ id: Schema.String, name: Schema.String });
const Issue = Schema.Struct({ id: Schema.String, projectId: Schema.String, title: Schema.String });
export class Workspace extends Streamsy.state<Workspace>()("Tracker/Workspace", "workspaces/:workspaceId", {
  params: { workspaceId: Schema.String },
  collections: { project: { schema: Project, key: "id" }, issue: { schema: Issue, key: "id" } },
}) {}
`;
const orderWrite = `import { Effect } from "effect";
import { Orders } from "./streams.ts";
export const write = Effect.gen(function* () {
  const orders = yield* Orders;
  yield* orders.create({});
  return (yield* orders.append({}, [{ orderId: "o-17", amount: 40 }])).offset;
});
`;

/** Support by page path under `content/docs`, without `.mdx`. Pages with no entry compile as they are. */
export const snippetSupport = new Map<string, PageSupport>([
  [
    "hosting/storage-adapter",
    {
      renames: { 1: "adapter.ts" },
      modules: {
        "streams.ts": shopStreams,
      },
    },
  ],
  [
    "tutorial",
    {
      renames: { 1: "streams.ts", 2: "write.ts", 4: "projection.ts", 7: "storage.ts" },
    },
  ],
  [
    "projections/index",
    {
      renames: { 1: "minimal.ts", 2: "tripled.ts" },
      modules: {
        "zz-provided.ts": `import { Effect, Layer } from "effect";
import { Streamsy } from "@streamsy/core";
import { CheckpointStore, Projection, Transaction } from "@streamsy/projection";
import { BunStorage } from "@streamsy/storage/bun";
import { minimal, Numbers, Doubled } from "./minimal.ts";
import { tripled } from "./tripled.ts";
import { host } from "./block03.ts";
export const memory: Effect.Effect<unknown, unknown, never> = Projection.run(tripled.at({})).pipe(Effect.provide(host));
const sqlHost = Layer.mergeAll(CheckpointStore.layer, Transaction.layer).pipe(Layer.provideMerge(BunStorage.layer(Streamsy.group(Numbers, Doubled), { client: { filename: ":memory:" } })));
export const sql: Effect.Effect<unknown, unknown, never> = minimal.pipe(Effect.provide(sqlHost));
`,
      },
    },
  ],
  [
    "projections/actions",
    {
      renames: { 1: "sink.ts" },
    },
  ],
  [
    "projections/define",
    {
      renames: { 1: "order-totals.ts" },
      preludes: {
        6: `import { Effect, Layer, Schema } from "effect";
import { MemoryStorage } from "@streamsy/core";
import { CheckpointStore, Commit, Projection } from "@streamsy/projection";
import { orderTotalsProjection } from "./order-totals.ts";
import { Orders, shop } from "./streams.ts";
`,
        7: `import { Effect, Layer, Schema } from "effect";
import { MemoryStorage } from "@streamsy/core";
import { CheckpointStore, Commit, Projection } from "@streamsy/projection";
import { orderTotalsProjection } from "./order-totals.ts";
import { Orders, shop } from "./streams.ts";
declare const readOrderTotals: () => Effect.Effect<number>;
`,
        8: `import { Effect, Layer, Schema } from "effect";
import { MemoryStorage } from "@streamsy/core";
import { CheckpointStore, Commit, Projection } from "@streamsy/projection";
import { orderTotalsProjection } from "./order-totals.ts";
import { Orders, shop } from "./streams.ts";
import { Streamsy } from "@streamsy/core";
class Facts extends Streamsy.json<Facts>()("Shop/Facts", "facts/:workspaceId", { params: { workspaceId: Schema.String }, schema: Schema.Finite }) {}
class Totals extends Streamsy.json<Totals>()("Shop/Totals", "totals/:workspaceId", { params: { workspaceId: Schema.String }, schema: Schema.Finite }) {}
const ordersByWorkspace = Projection.emit({ id: "order-totals", params: { workspaceId: Schema.String }, inputs: { facts: Facts }, outputs: { output: Totals }, commit: Commit.producer, emit: ({ item }) => ({ output: [item] }) });
`,
        9: `import { Effect, Layer, Schema } from "effect";
import { MemoryStorage } from "@streamsy/core";
import { CheckpointStore, Commit, Projection } from "@streamsy/projection";
import { orderTotalsProjection } from "./order-totals.ts";
import { Orders, shop } from "./streams.ts";
`,
        10: `import { Effect, Layer, Schema } from "effect";
import { MemoryStorage } from "@streamsy/core";
import { CheckpointStore, Commit, Projection } from "@streamsy/projection";
import { orderTotalsProjection } from "./order-totals.ts";
import { Orders, shop } from "./streams.ts";
const host = CheckpointStore.layer.pipe(Layer.provideMerge(MemoryStorage.layer(shop)));
`,
        11: `import { Schema as S } from "effect";
const orderSchema = S.Struct({ orderId: S.String, amount: S.Finite });
const totalSchema = S.Struct({ orderId: S.String, total: S.Finite });
declare const toTotals: (items: ReadonlyArray<typeof orderSchema.Type>) => ReadonlyArray<typeof totalSchema.Type>;
`,
      },
      modules: {
        "streams.ts": shopStreams,
      },
    },
  ],
  [
    "projections/state",
    {
      preludes: {
        2: `import { Effect, Schema } from "effect";
import { Commit, Projection } from "@streamsy/projection";
import { Orders } from "./streams.ts";
`,
      },
      modules: {
        "order-totals.ts": `import { Effect } from "effect";
import { Commit, Projection } from "@streamsy/projection";
import { Orders, OrderTotals } from "./streams.ts";
export const orderTotalsProjection = Projection.emit({
  id: "order-totals",
  params: {},
  inputs: { input: Orders },
  outputs: { totals: OrderTotals },
  commit: Commit.producer,
  emit: ({ item }) => ({ totals: [{ orderId: item.orderId, total: item.amount }] }),
}).at({});
`,
        "run.ts": `import { Commit, Projection } from "@streamsy/projection";
import { orderTotalsProjection } from "./order-totals.ts";
export const program = Projection.run(orderTotalsProjection, { limit: 10 });
`,
        "streams.ts": shopStreams,
      },
    },
  ],
  [
    "http/serve",
    {
      renames: { 1: "app.ts" },
      preludes: {
        3: `import { Context, Effect, Layer, Schema } from "effect";
import { HttpRouter, HttpServerResponse } from "effect/unstable/http";
import { Streamsy } from "@streamsy/core";
import { Serve } from "@streamsy/serve";
class Events extends Streamsy.json<Events>()("Game/Events", "events/:seat", { params: { seat: Schema.String }, schema: Schema.String }) {}
class CurrentSeat extends Context.Service<CurrentSeat, { readonly id: string }>()("CurrentSeat") {}
const SeatAuth = HttpRouter.middleware<{ provides: CurrentSeat }>()((handler) => Effect.provideService(handler, CurrentSeat, { id: "1" }));
export {};
`,
        4: `import { Serve } from "@streamsy/serve";
import { Streamsy } from "@streamsy/core";
class Events extends Streamsy.json<Events>()("Game/ApiEvents", "events/:seat", { params: { seat: Schema.String }, schema: Schema.String }) {}
`,
        5: `import { Layer } from "effect";
import { HttpRouter } from "effect/unstable/http";
import { App } from "./app.ts";
`,
      },
      modules: {
        "zz-provided.ts": `import { Layer, Schema } from "effect";
import { HttpRouter } from "effect/unstable/http";
import { MemoryStorage, Streamsy } from "@streamsy/core";
import { App, game } from "./app.ts";
const served = HttpRouter.serve(App);
type Needs = Layer.Services<typeof served>;
class Events extends Streamsy.json<Events>()("Game/Events", "events/:seat", { params: { seat: Schema.String }, schema: Schema.String }) {}
// The served app requires the Events stream service.
export const requiresEvents: Events extends Needs ? true : false = true;
export const provided = served.pipe(Layer.provide(MemoryStorage.layer(game)));
type Left = Exclude<Layer.Services<typeof provided>, import("effect/unstable/http").HttpServer.HttpServer>;
export const nothingElse: [Left] extends [never] ? true : false = true;
`,
      },
    },
  ],
  [
    "hosting/cloudflare",
    {
      preludes: {
        2: `import type { DurableObjectState } from "@cloudflare/workers-types";
declare const ctx: DurableObjectState;
`,
      },
      modules: {
        "streams.ts": shopStreams,
      },
    },
  ],
  [
    "http/routes",
    {
      renames: { 1: "app.ts" },
    },
  ],
  [
    "http/remote",
    {
      modules: {
        "streams.ts": shopStreams,
      },
    },
  ],
  [
    "hosting/index",
    {
      modules: {
        "streams.ts": shopStreams,
        "write.ts": orderWrite,
        "zz-provided.ts": `import { Effect, Stream } from "effect";
import { layer } from "./block02.ts";
import { Drafts, Journal } from "./streams.ts";
const program = Effect.gen(function* () {
  const drafts = yield* Drafts;
  const journal = yield* Journal;
  yield* drafts.create({ draftId: "a" });
  yield* journal.append({}, ["x"]);
  return yield* journal.read({}).pipe(Stream.runCollect);
});
export const provided: Effect.Effect<unknown, unknown, never> = program.pipe(Effect.provide(layer));
`,
      },
    },
  ],
  [
    "streams/create",
    {
      modules: {
        "streams.ts": notesStreams,
      },
    },
  ],
  [
    "streams/declare",
    {
      renames: { 1: "streams.ts" },
    },
  ],
  [
    "streams/read",
    {
      modules: {
        "streams.ts": notesStreams,
        "zz-provided.ts": `import { Effect } from "effect";
import { MemoryStorage, Streamsy } from "@streamsy/core";
import { Notes } from "./streams.ts";
import * as B2 from "./block02.ts";
declare const all: Effect.Effect<unknown, unknown, Notes>;
const provided: Effect.Effect<unknown, unknown, never> = all.pipe(Effect.provide(MemoryStorage.layer(Streamsy.group(Notes))));
export { B2, provided };
`,
      },
    },
  ],
  [
    "streams/write",
    {
      modules: {
        "orders.ts": `import { Schema } from "effect";
import { Streamsy } from "@streamsy/core";
export const OrderEvent = Schema.Struct({ type: Schema.Literal("reserved"), sku: Schema.String });
export class Orders extends Streamsy.json<Orders>()("Shop/Orders", "orders", { params: {}, schema: OrderEvent }) {}
export interface OrdersState { readonly reserved: ReadonlySet<string> }
export const initial: OrdersState = { reserved: new Set() };
export const apply = (state: OrdersState, event: typeof OrderEvent.Type): OrdersState => ({ reserved: new Set([...state.reserved, event.sku]) });
`,
        "streams.ts": notesStreams,
      },
    },
  ],
]);
