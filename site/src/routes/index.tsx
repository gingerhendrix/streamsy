import { createFileRoute } from "@tanstack/react-router";
import { createServerFn } from "@tanstack/react-start";
import { getRequestUrl } from "@tanstack/react-start/server";
import { HomeLayout } from "fumadocs-ui/layouts/home";
import { baseOptions } from "#/lib/layout.shared";
import { withBasePath } from "#/lib/base-path";

const title = "Streamsy — Effect streams, from server to browser";
const description =
  "An Effect server and toolkit for Durable Streams. Typed streams, a Bun memory host, and official Durable Streams packages in the browser.";

const loadImage = createServerFn({ method: "GET" }).handler(
  () => new URL(withBasePath("/og.webp"), getRequestUrl().origin).href,
);

export const Route = createFileRoute("/")({
  component: Home,
  loader: () => loadImage(),
  head: ({ loaderData }) => ({
    meta: [
      { title },
      { name: "description", content: description },
      { property: "og:type", content: "website" },
      { property: "og:title", content: title },
      { property: "og:description", content: description },
      { property: "og:image", content: loaderData ?? withBasePath("/og.webp") },
      { property: "og:image:width", content: "1200" },
      { property: "og:image:height", content: "630" },
      { property: "og:image:type", content: "image/webp" },
      { name: "twitter:card", content: "summary_large_image" },
      { name: "twitter:title", content: title },
      { name: "twitter:description", content: description },
      { name: "twitter:image", content: loaderData ?? withBasePath("/og.webp") },
    ],
  }),
});

function Home() {
  return (
    <HomeLayout {...baseOptions()}>
      <main className="mx-auto w-full max-w-5xl px-6 py-16 sm:py-24">
        <p className="font-mono text-sm text-fd-muted-foreground">First release · 0.4.0</p>
        <h1 className="mt-5 max-w-3xl text-4xl font-semibold tracking-tight sm:text-6xl">
          Effect streams, from server to browser.
        </h1>
        <p className="mt-6 max-w-2xl text-lg leading-8 text-fd-muted-foreground">
          Streamsy is an Effect server and toolkit for Durable Streams. Create typed event streams,
          follow their changes, and serve them to official Durable Streams browser clients.
        </p>
        <div className="mt-8 flex flex-wrap gap-4">
          <a
            className="rounded-lg bg-fd-primary px-5 py-3 text-fd-primary-foreground"
            href={withBasePath("/docs/introduction")}
          >
            Start with typed streams
          </a>
          <a
            className="rounded-lg border px-5 py-3"
            href={withBasePath("/docs/demos/hackernews-newest-stream")}
          >
            Explore the Hacker News demo
          </a>
        </div>
        <section className="mt-16 grid gap-6 sm:grid-cols-3" aria-label="Packages">
          {[
            [
              "@streamsy/core",
              "Protocol services, typed streams, storage contract and memory Layer.",
            ],
            ["@streamsy/serve", "Checked serving contracts and an owned Bun HTTP host."],
            ["@streamsy/views", "Incremental views and view-store contracts."],
          ].map(([name, detail]) => (
            <div key={name} className="rounded-xl border p-6">
              <h2 className="font-mono text-lg font-semibold">{name}</h2>
              <p className="mt-3 leading-7 text-fd-muted-foreground">{detail}</p>
            </div>
          ))}
        </section>
        <section className="mt-12 max-w-3xl border-t pt-8">
          <h2 className="text-2xl font-semibold">Start locally, with explicit lifetimes.</h2>
          <p className="mt-4 leading-7 text-fd-muted-foreground">
            The Hacker News demo shares one SQLite Layer across its streams and projection
            checkpoint, and resumes after a process restart. Cloudflare hosting is future work.
            Effect 4.0.0-rc.112 powers the server and toolkit. Browsers use official Durable Streams
            client and State packages.
          </p>
          <p className="mt-4 leading-7">
            Read the{" "}
            <a className="underline" href={withBasePath("/docs/streams/introduction")}>
              Streams guide
            </a>{" "}
            or the{" "}
            <a className="underline" href={withBasePath("/docs/runtime/introduction")}>
              Runtime guide
            </a>
            .
          </p>
        </section>
      </main>
    </HomeLayout>
  );
}
