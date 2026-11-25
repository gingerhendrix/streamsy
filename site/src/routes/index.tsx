import { Array } from "effect";
import { createFileRoute } from "@tanstack/react-router";
import { createServerFn } from "@tanstack/react-start";
import { getRequestUrl } from "@tanstack/react-start/server";
import { HomeLayout } from "fumadocs-ui/layouts/home";
import { ArrowRight, ArrowUpRight, Database, Layers, Radio, Server } from "lucide-react";
import doomCoverUrl from "../../assets/docs/examples/streamsy-doom-cover.webp";
import issueTrackerScreenshotUrl from "../../assets/docs/examples/issue-tracker-foldkit-screenshot.png";
import yjsConceptUrl from "../../assets/docs/examples/yjs-editor-concept.svg";
import { HomeCodePreview, InstallCommand } from "#/components/home-code-preview";
import { withBasePath } from "#/lib/base-path";
import { baseOptions } from "#/lib/layout.shared";

const heroHeading = "Build Effect applications with Durable Streams";

const heroParagraph =
  "Streamsy is an Effect 4 toolkit and server for Durable Streams—typed stream services, swappable storage Layers, checkpointed projections, incremental relational views, and multi-runtime hosts.";

const loadImage = createServerFn({ method: "GET" }).handler(
  () => new URL(withBasePath("/og.webp"), getRequestUrl().origin).href,
);

export const Route = createFileRoute("/")({
  component: Home,
  loader: () => loadImage(),
  head: ({ loaderData }) => ({
    meta: [
      { title: `${heroHeading} — Streamsy` },
      { name: "description", content: heroParagraph },
      { property: "og:type", content: "website" },
      { property: "og:title", content: heroHeading },
      { property: "og:description", content: heroParagraph },
      { property: "og:image", content: loaderData ?? withBasePath("/og.webp") },
      { property: "og:image:width", content: "1200" },
      { property: "og:image:height", content: "630" },
      { property: "og:image:type", content: "image/webp" },
      { name: "twitter:card", content: "summary_large_image" },
      { name: "twitter:title", content: heroHeading },
      { name: "twitter:description", content: heroParagraph },
      { name: "twitter:image", content: loaderData ?? withBasePath("/og.webp") },
    ],
  }),
});

const pillars = [
  {
    icon: Radio,
    title: "Typed Streams",
    description:
      "Declare byte, JSON, Durable State, and wake streams with Effect Schema, conditional appends, and native forks.",
    href: "/docs/streams/declare",
  },
  {
    icon: Layers,
    title: "Projections & Views",
    description:
      "Derive SQL tables and output streams with checkpointed projections, incremental relational circuits, and in-memory View.live snapshots.",
    href: "/docs/views",
  },
  {
    icon: Database,
    title: "Swappable Storage",
    description:
      "Run the same program on Memory, SQLite, Postgres, MySQL, Cloudflare Durable Objects, or over HTTP via RemoteStreams.",
    href: "/docs/storage",
  },
  {
    icon: Server,
    title: "Servers & Clients",
    description:
      "Serve the Durable Streams HTTP protocol and read-only app routes on Bun, Node, Deno, or Cloudflare with alarms and hibernatable WebSockets.",
    href: "/docs/servers",
  },
] as const;

const featuredExamples = [
  {
    title: "Doom",
    badge: "Cloudflare DO · Live demo",
    description:
      "Browser Doom recorded as binary streams with rewind snapshots, native forks, live HUD, and lobby.",
    image: doomCoverUrl,
    imageAlt: "Streamsy Doom live demo screenshot",
    imageClass: "object-cover",
    href: "/docs/examples/doom",
    liveUrl: "https://doom.streamsy.dev/",
  },
  {
    title: "Issue Tracker",
    badge: "Durable State · Views",
    description:
      "Collaborative sync with optimistic writes, txid confirmation, and swappable TanStack DB, Foldkit + Views, and event-sourced variants.",
    image: issueTrackerScreenshotUrl,
    imageAlt: "Issue tracker demo screenshot",
    imageClass: "object-cover object-top",
    href: "/docs/examples/issue-tracker",
    liveUrl: undefined,
  },
  {
    title: "Yjs Editor",
    badge: "Cloudflare DO · Live demo",
    description:
      "Collaborative ProseMirror editing over binary update streams, expiring awareness, and alarm-driven snapshot compaction.",
    image: yjsConceptUrl,
    imageAlt: "Yjs editor stream compaction diagram",
    imageClass: "object-contain bg-fd-secondary/40 p-4",
    href: "/docs/examples/yjs-editor",
    liveUrl: "https://yjs-editor.streamsy.dev",
  },
] as const;

function Home() {
  return (
    <HomeLayout {...baseOptions()}>
      <main className="mx-auto w-full max-w-6xl px-6 py-14 sm:py-20">
        {/* Hero */}
        <section className="max-w-3xl">
          <p className="font-mono text-xs font-medium tracking-wider text-fd-muted-foreground uppercase">
            Effect 4 · Durable Streams
          </p>
          <h1 className="mt-3 text-4xl font-semibold tracking-tight sm:text-5xl">{heroHeading}</h1>
          <p className="mt-5 text-base leading-7 text-fd-muted-foreground sm:text-lg sm:leading-8">
            {heroParagraph}
          </p>

          <div className="mt-7 flex flex-wrap items-center gap-3">
            <a
              className="inline-flex items-center gap-2 rounded-lg bg-fd-primary px-5 py-2.5 text-sm font-medium text-fd-primary-foreground transition-opacity hover:opacity-90"
              href={withBasePath("/docs")}
            >
              Get Started
              <ArrowRight className="size-4" />
            </a>
            <a
              className="inline-flex items-center gap-2 rounded-lg border bg-fd-card px-5 py-2.5 text-sm font-medium transition-colors hover:bg-fd-accent"
              href={withBasePath("/docs/examples")}
            >
              Explore Examples
            </a>
            <InstallCommand command="bun add @streamsy/core effect" />
          </div>
        </section>

        {/* Full-width interactive walkthrough */}
        <section className="mt-12">
          <HomeCodePreview />
        </section>

        {/* Four core pillars */}
        <section className="mt-16 border-t pt-12 sm:mt-20">
          <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
            {Array.map(pillars, (pillar) => {
              const Icon = pillar.icon;

              return (
                <a
                  key={pillar.title}
                  href={withBasePath(pillar.href)}
                  className="group flex flex-col rounded-xl border bg-fd-card p-5 transition-colors hover:bg-fd-accent/50"
                >
                  <Icon className="size-5 text-fd-muted-foreground transition-colors group-hover:text-fd-foreground" />
                  <h2 className="mt-3.5 text-base font-semibold tracking-tight">{pillar.title}</h2>
                  <p className="mt-2 text-sm leading-6 text-fd-muted-foreground">
                    {pillar.description}
                  </p>
                </a>
              );
            })}
          </div>
        </section>

        {/* Featured examples */}
        <section className="mt-16 border-t pt-12 sm:mt-20">
          <div className="flex flex-wrap items-end justify-between gap-4">
            <div>
              <h2 className="text-2xl font-semibold tracking-tight">Featured examples</h2>
              <p className="mt-1.5 text-sm text-fd-muted-foreground">
                Complete, runnable applications built with Streamsy.
              </p>
            </div>
            <a
              href={withBasePath("/docs/examples")}
              className="inline-flex items-center gap-1.5 text-sm font-medium text-fd-foreground hover:underline"
            >
              Browse all 13 examples
              <ArrowRight className="size-4" />
            </a>
          </div>

          <div className="mt-6 grid gap-5 md:grid-cols-3">
            {Array.map(featuredExamples, (example) => (
              <article
                key={example.title}
                className="flex flex-col overflow-hidden rounded-xl border bg-fd-card"
              >
                <a href={withBasePath(example.href)} className="block overflow-hidden border-b">
                  <img
                    src={example.image}
                    alt={example.imageAlt}
                    className={`aspect-16/9 w-full ${example.imageClass}`}
                  />
                </a>
                <div className="flex flex-1 flex-col p-5">
                  <span className="font-mono text-xs text-fd-muted-foreground">
                    {example.badge}
                  </span>
                  <h3 className="mt-1.5 text-lg font-semibold tracking-tight">
                    <a href={withBasePath(example.href)} className="hover:underline">
                      {example.title}
                    </a>
                  </h3>
                  <p className="mt-2 flex-1 text-sm leading-6 text-fd-muted-foreground">
                    {example.description}
                  </p>
                  <div className="mt-4 flex flex-wrap items-center gap-4 pt-2 text-sm font-medium">
                    <a
                      href={withBasePath(example.href)}
                      className="inline-flex items-center gap-1 text-fd-foreground hover:underline"
                    >
                      Walkthrough
                      <ArrowRight className="size-3.5" />
                    </a>
                    {example.liveUrl ? (
                      <a
                        href={example.liveUrl}
                        target="_blank"
                        rel="noreferrer"
                        className="inline-flex items-center gap-1 text-fd-muted-foreground hover:text-fd-foreground hover:underline"
                      >
                        Live demo
                        <ArrowUpRight className="size-3.5" />
                      </a>
                    ) : null}
                  </div>
                </div>
              </article>
            ))}
          </div>
        </section>
      </main>
    </HomeLayout>
  );
}
