import { Array, Option } from "effect";
import { ArrowRight, Check, ChevronLeft, ChevronRight, Copy } from "lucide-react";
import { useState } from "react";
import { withBasePath } from "#/lib/base-path";

interface CodeTab {
  readonly id: string;
  readonly label: string;
  readonly filename: string;
  readonly caption: string;
  readonly href: `/${string}`;
  readonly linkLabel: string;
  readonly code: string;
}

const tabs: ReadonlyArray<CodeTab> = [
  {
    id: "streams",
    label: "Streams",
    filename: "streams.ts",
    caption: "Declare a JSON event log and keyed Durable State streams in one typed group.",
    href: "/docs/streams/declare",
    linkLabel: "Streams guide",
    code: `import { Schema } from "effect";
import { Streamsy } from "@streamsy/core";

export const OrderEvent = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("placed"),
    orderId: Schema.String,
    customerId: Schema.String,
    total: Schema.Finite,
  }),
  Schema.Struct({
    type: Schema.Literal("cancelled"),
    orderId: Schema.String,
  }),
]);

// Append-only JSON event log
export class OrderEvents extends Streamsy.json<OrderEvents>()(
  "Shop/OrderEvents",
  "shop/:shopId/events",
  { params: { shopId: Schema.String }, schema: OrderEvent },
) {}

// Keyed Durable State streams: materialized orders, customers, and derived views
export class Orders extends Streamsy.state<Orders>()("Shop/Orders", "shop/:shopId/orders", {
  params: { shopId: Schema.String },
  collections: {
    order: {
      schema: Schema.Struct({
        orderId: Schema.String,
        customerId: Schema.String,
        total: Schema.Finite,
      }),
      key: "orderId",
    },
  },
}) {}

export class Customers extends Streamsy.state<Customers>()(
  "Shop/Customers",
  "shop/:shopId/customers",
  {
    params: { shopId: Schema.String },
    collections: {
      customer: {
        schema: Schema.Struct({
          customerId: Schema.String,
          tier: Schema.Literals(["standard", "gold"]),
        }),
        key: "customerId",
      },
    },
  },
) {}

export class ShopViews extends Streamsy.state<ShopViews>()(
  "Shop/Views",
  "shop/:shopId/views/g/:generation",
  {
    params: { shopId: Schema.String, generation: Schema.FiniteFromString },
    collections: {
      goldTotals: {
        schema: Schema.Struct({
          customerId: Schema.String,
          orders: Schema.Finite,
          revenue: Schema.Finite,
        }),
        key: "customerId",
      },
    },
  },
) {}

export const shop = Streamsy.group(OrderEvents, Orders, Customers, ShopViews);`,
  },
  {
    id: "cas",
    label: "Write & CAS",
    filename: "write.ts",
    caption: "Append facts with compare-and-swap via decide() or explicit expectedOffset.",
    href: "/docs/streams/write#conditional-writes",
    linkLabel: "Conditional writes",
    code: `import { Effect } from "effect";
import { MemoryStorage } from "@streamsy/core";
import { OrderEvents, shop } from "./streams.ts";

// Read-fold-append CAS loop: decides from current state, appends with expectedOffset,
// and retries automatically on OffsetMismatch if another writer appends first.
export const placeOrder = (
  shopId: string,
  order: { readonly orderId: string; readonly customerId: string; readonly total: number },
) =>
  Effect.gen(function* () {
    const events = yield* OrderEvents;
    return yield* events.decide(
      { shopId },
      {
        initial: () => new Set<string>(),
        fold: (seen, event) =>
          event.type === "placed" ? new Set([...seen, event.orderId]) : seen,
        decide: (seen) =>
          Effect.succeed(
            seen.has(order.orderId)
              ? { value: "duplicate" as const, items: [] }
              : { value: "placed" as const, items: [{ type: "placed" as const, ...order }] },
          ),
      },
    );
  });

const program = Effect.gen(function* () {
  const events = yield* OrderEvents;
  yield* events.create({ shopId: "main" });

  const placed = yield* placeOrder("main", { orderId: "o-1", customerId: "c-1", total: 42 });

  // Or pass expectedOffset directly to fence a single append against concurrent writers.
  return yield* events.append({ shopId: "main" }, [{ type: "cancelled", orderId: "o-1" }], {
    expectedOffset: placed.offset,
  });
});

// Run in memory for tests—swap the Layer for SQLite, Postgres, MySQL, or remote HTTP.
await Effect.runPromise(program.pipe(Effect.provide(MemoryStorage.layer(shop))));`,
  },
  {
    id: "project",
    label: "Project",
    filename: "projection.ts",
    caption: "Materialize OrderEvents into the keyed Orders state stream with Commit.producer.",
    href: "/docs/projections",
    linkLabel: "Projections guide",
    code: `import { Schema } from "effect";
import { DurableState } from "@streamsy/core";
import { Commit, Projection } from "@streamsy/projection";
import { OrderEvents, Orders } from "./streams.ts";

// Project append-only facts into keyed Durable State rows.
// Commit.producer pins producer positions so retries never write duplicate changes.
export const ordersProjection = Projection.emit({
  id: "shop-orders",
  params: { shopId: Schema.String },
  inputs: { events: OrderEvents },
  outputs: { orders: Orders },
  commit: Commit.producer,
  emit: ({ item }) => ({
    orders: [
      item.type === "placed"
        ? DurableState.upsert("order", {
            orderId: item.orderId,
            customerId: item.customerId,
            total: item.total,
          })
        : DurableState.remove("order", item.orderId),
    ],
  }),
});

// Run on demand in tests or jobs, or attach it to a server or Durable Object (steps 5 & 7).
export const catchUpOrders = (shopId: string) => Projection.run(ordersProjection.at({ shopId }));`,
  },
  {
    id: "view",
    label: "View",
    filename: "views.ts",
    caption: "Incrementally join projected Orders with Customers into ShopViews.",
    href: "/docs/views/relations",
    linkLabel: "Views guide",
    code: `import { Projection } from "@streamsy/projection";
import { View } from "@streamsy/views";
import { Customers, Orders, ShopViews } from "./streams.ts";

const orders = View.from(Orders, "order");
const customers = View.from(Customers, "customer");

export const shopViews = View.circuit({
  id: "shop-views",
  params: Orders.paramCodecs,
  output: Projection.perGeneration(ShopViews, "generation"),
  views: {
    goldTotals: orders
      .join(customers, {
        as: "customer",
        on: (order, customer) => order.customerId.eq(customer.customerId),
      })
      .where((row) => row.customer.tier.eq("gold"))
      .groupBy((row) => ({ customerId: row.customerId }))
      .aggregate((row) => ({
        orders: View.count(),
        revenue: View.sum(row.total),
      })),
  },
});`,
  },
  {
    id: "serve",
    label: "Serve",
    filename: "server.ts",
    caption: "Attach both the projection and view circuit on Bun SQLite with HTTP routes.",
    href: "/docs/servers/process",
    linkLabel: "Process server guide",
    code: `import { Effect, Layer } from "effect";
import { SqlClient } from "effect/sql";
import { BunRuntime } from "@effect/platform-bun";
import { Http } from "@streamsy/core";
import { CheckpointStore, Projection, Transaction } from "@streamsy/projection";
import { Serve } from "@streamsy/serve";
import { listener } from "@streamsy/serve/bun";
import { BunStorage } from "@streamsy/storage/bun";
import { View, ViewBackend } from "@streamsy/views";
import { ordersProjection } from "./projection.ts";
import { ShopViews, shop } from "./streams.ts";
import { shopViews } from "./views.ts";

const App = Layer.mergeAll(
  Http.routes(shop, { prefix: "/streams", access: "append" }),
  Serve.mount(ShopViews, "/shops/:shopId/views/:generation"),
);

const migrate = Layer.effectDiscard(Effect.flatMap(SqlClient.SqlClient, View.migrate));
const storage = BunStorage.layer(shop, { client: { filename: "./shop.sqlite" } });

const Server = Layer.mergeAll(
  Serve.layer(App),
  Projection.attach(ordersProjection, { durable: true }),
  Projection.attach(shopViews, { durable: true }),
).pipe(
  Layer.provide(migrate),
  Layer.provide(Layer.mergeAll(CheckpointStore.layer, Transaction.layer, ViewBackend.layerSql)),
  Layer.provide(Projection.Registry.layer),
  Layer.provide(storage),
  Layer.provide(listener({ port: 3000 })),
);

BunRuntime.runMain(Layer.launch(Server), { teardown: Serve.teardown });`,
  },
  {
    id: "live",
    label: "Live",
    filename: "client.ts",
    caption: "Follow remote streams in an Effect client or browser with View.live.",
    href: "/docs/views/live",
    linkLabel: "Live snapshots guide",
    code: `import { Effect, Layer, Stream } from "effect";
import { FetchHttpClient } from "effect/http";
import { Streamsy } from "@streamsy/core";
import { RemoteStreams } from "@streamsy/core/remote";
import { View } from "@streamsy/views";
import { Customers, Orders } from "./streams.ts";

const shopPage = View.live({
  id: "shop-page",
  params: Orders.paramCodecs,
  views: {
    goldOrders: View.from(Orders, "order")
      .join(View.from(Customers, "customer"), {
        as: "customer",
        on: (order, customer) => order.customerId.eq(customer.customerId),
      })
      .where((row) => row.customer.tier.eq("gold")),
  },
});

const remote = RemoteStreams.layer(Streamsy.group(Orders, Customers), {
  baseUrl: "https://shop.example.com/streams",
}).pipe(Layer.provide(FetchHttpClient.layer));

export const watchGoldOrders = (shopId: string) =>
  Effect.gen(function* () {
    const live = yield* shopPage.at({ shopId });
    yield* live.caughtUp;
    yield* live.snapshots.pipe(Stream.runForEach(({ rows }) => Effect.log(rows.goldOrders)));
  }).pipe(Effect.scoped, Effect.provide(remote));`,
  },
  {
    id: "cloudflare",
    label: "Cloudflare",
    filename: "worker.ts",
    caption: "Deploy the same streams, projection, and view on Durable Objects with WebSockets.",
    href: "/docs/servers/cloudflare",
    linkLabel: "Cloudflare guide",
    code: `import type { DurableObjectNamespace } from "@cloudflare/workers-types";
import { Effect, Layer } from "effect";
import { SqlClient } from "effect/sql";
import { Http, Streamsy } from "@streamsy/core";
import { CheckpointStore, Projection, ProjectionWakeup, Transaction } from "@streamsy/projection";
import { Placement, StreamsyObject, router, rule } from "@streamsy/serve/cloudflare";
import { DurableObjectStorage } from "@streamsy/storage/durable-object";
import { View, ViewBackend } from "@streamsy/views";
import { ordersProjection } from "./projection.ts";
import { Customers, OrderEvents, Orders, ShopViews, shop } from "./streams.ts";
import { shopViews } from "./views.ts";

interface Env {
  readonly SHOP: DurableObjectNamespace;
}

export class ShopObject extends StreamsyObject.make<
  Env,
  Streamsy.Services<typeof shop> | CheckpointStore | Transaction | ViewBackend
>({
  app: Http.routes(shop, { prefix: "/streams", access: "append" }),
  layer: (state) => {
    const storage = DurableObjectStorage.layer(shop, { client: { storage: state.storage } });
    const migrate = Layer.effectDiscard(Effect.flatMap(SqlClient.SqlClient, View.migrate));
    return Layer.mergeAll(
      migrate.pipe(Layer.orDie),
      CheckpointStore.layer,
      Transaction.layer,
      ViewBackend.layerSql.pipe(Layer.orDie),
      Projection.Registry.layer,
    ).pipe(Layer.provideMerge(storage));
  },
  wakeups: [ProjectionWakeup.attach(ordersProjection), ProjectionWakeup.attach(shopViews)],
  live: { websocket: { prefix: "/streams" } },
}) {}

export default router<Env>({
  prefix: "/streams",
  namespace: (env) => env.SHOP,
  placement: Placement.byRoute([
    rule({ stream: OrderEvents, owner: ({ shopId }) => shopId }),
    rule({ stream: Orders, owner: ({ shopId }) => shopId }),
    rule({ stream: Customers, owner: ({ shopId }) => shopId }),
    rule({ stream: ShopViews, owner: ({ shopId }) => shopId }),
  ]),
});`,
  },
];

type TokenKind = "plain" | "comment" | "string" | "keyword" | "number";

interface Token {
  readonly kind: TokenKind;
  readonly text: string;
}

const tokenPattern =
  /(\/\/[^\n]*|"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*'|\b(?:import|export|default|from|const|class|extends|return|yield|await|function|new|as|readonly|type|interface)\b|\b\d+\b)/g;

const keywords = new Set([
  "import",
  "export",
  "default",
  "from",
  "const",
  "class",
  "extends",
  "return",
  "yield",
  "await",
  "function",
  "new",
  "as",
  "readonly",
  "type",
  "interface",
]);

function classify(text: string): TokenKind {
  if (text.startsWith("//")) return "comment";

  if (text.startsWith('"') || text.startsWith("'")) return "string";

  if (keywords.has(text)) return "keyword";

  if (/^\d+$/.test(text)) return "number";

  return "plain";
}

function tokenizeLine(line: string): ReadonlyArray<Token> {
  const result: Array<Token> = [];
  let index = 0;

  for (const match of line.matchAll(tokenPattern)) {
    const start = match.index;
    const text = match[0];

    if (start > index) result.push({ kind: "plain", text: line.slice(index, start) });

    result.push({ kind: classify(text), text });
    index = start + text.length;
  }

  if (index < line.length) result.push({ kind: "plain", text: line.slice(index) });

  return result;
}

const tokenClasses: Readonly<Record<TokenKind, string>> = {
  plain: "text-fd-foreground",
  comment: "text-fd-muted-foreground italic",
  string: "text-emerald-700 dark:text-emerald-400",
  keyword: "text-sky-700 dark:text-sky-400",
  number: "text-amber-700 dark:text-amber-400",
};

export function InstallCommand({ command }: { readonly command: string }) {
  const [copied, setCopied] = useState(false);

  const handleCopy = () => {
    void navigator.clipboard?.writeText(command);
    setCopied(true);
    window.setTimeout(() => setCopied(false), 1800);
  };

  return (
    <div className="inline-flex items-center gap-3 rounded-lg border bg-fd-card px-3.5 py-2 font-mono text-sm">
      <span className="select-none text-fd-muted-foreground">$</span>
      <code className="text-fd-foreground">{command}</code>
      <button
        type="button"
        onClick={handleCopy}
        aria-label="Copy install command"
        className="rounded p-1 text-fd-muted-foreground transition-colors hover:bg-fd-accent hover:text-fd-foreground"
      >
        {copied ? <Check className="size-3.5" /> : <Copy className="size-3.5" />}
      </button>
    </div>
  );
}

export function HomeCodePreview() {
  const [activeIndex, setActiveIndex] = useState(0);
  const [copied, setCopied] = useState(false);

  const active = Option.getOrElse(Array.get(tabs, activeIndex), () => tabs[0]);

  const selectIndex = (index: number) => {
    setActiveIndex(index);
    setCopied(false);
  };

  const handleCopy = () => {
    void navigator.clipboard?.writeText(active.code);
    setCopied(true);
    window.setTimeout(() => setCopied(false), 1800);
  };

  const lines = active.code.split("\n");

  return (
    <div className="overflow-hidden rounded-xl border bg-fd-card shadow-xs">
      <div className="flex items-center justify-between gap-2 border-b bg-fd-secondary/40 px-3 py-2">
        <div
          className="flex flex-wrap items-center gap-1"
          role="tablist"
          aria-label="Shop walkthrough"
        >
          {Array.map(tabs, (tab, index) => {
            const selected = index === activeIndex;

            return (
              <button
                key={tab.id}
                type="button"
                role="tab"
                aria-selected={selected}
                onClick={() => selectIndex(index)}
                className={
                  selected
                    ? "inline-flex items-center gap-1.5 rounded-md bg-fd-background px-2.5 py-1 text-xs font-medium text-fd-foreground shadow-2xs ring-1 ring-fd-border"
                    : "inline-flex items-center gap-1.5 rounded-md px-2.5 py-1 text-xs font-medium text-fd-muted-foreground transition-colors hover:text-fd-foreground"
                }
              >
                <span className="font-mono text-[10px] opacity-60">{index + 1}</span>
                {tab.label}
              </button>
            );
          })}
        </div>
        <div className="flex items-center gap-2">
          <span className="hidden font-mono text-xs text-fd-muted-foreground sm:inline">
            {active.filename}
          </span>
          <button
            type="button"
            onClick={handleCopy}
            aria-label="Copy code snippet"
            className="rounded p-1 text-fd-muted-foreground transition-colors hover:bg-fd-accent hover:text-fd-foreground"
          >
            {copied ? <Check className="size-3.5" /> : <Copy className="size-3.5" />}
          </button>
        </div>
      </div>

      <pre className="min-h-[340px] overflow-x-auto p-5 text-[13px] leading-6">
        <code className="font-mono">
          {Array.map(lines, (line, lineIndex) => (
            <div key={lineIndex}>
              {line.length === 0
                ? "\n"
                : Array.map(tokenizeLine(line), (token, tokenIndex) => (
                    <span key={tokenIndex} className={tokenClasses[token.kind]}>
                      {token.text}
                    </span>
                  ))}
            </div>
          ))}
        </code>
      </pre>

      <div className="flex flex-wrap items-center justify-between gap-3 border-t bg-fd-secondary/30 px-4 py-2.5 text-xs text-fd-muted-foreground">
        <span>{active.caption}</span>
        <div className="flex items-center gap-3">
          <a
            href={withBasePath(active.href)}
            className="inline-flex items-center gap-1 font-medium text-fd-foreground hover:underline"
          >
            {active.linkLabel}
            <ArrowRight className="size-3.5" />
          </a>
          <div className="flex items-center gap-1 border-s ps-3">
            <button
              type="button"
              onClick={() => selectIndex((activeIndex - 1 + tabs.length) % tabs.length)}
              aria-label="Previous step"
              className="rounded p-1 text-fd-muted-foreground transition-colors hover:bg-fd-accent hover:text-fd-foreground"
            >
              <ChevronLeft className="size-3.5" />
            </button>
            <button
              type="button"
              onClick={() => selectIndex((activeIndex + 1) % tabs.length)}
              aria-label="Next step"
              className="rounded p-1 text-fd-muted-foreground transition-colors hover:bg-fd-accent hover:text-fd-foreground"
            >
              <ChevronRight className="size-3.5" />
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
