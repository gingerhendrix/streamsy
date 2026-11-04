import { createFileRoute } from "@tanstack/react-router";
import { createServerFn } from "@tanstack/react-start";
import { getRequestUrl } from "@tanstack/react-start/server";
import { HomeLayout } from "fumadocs-ui/layouts/home";
import { baseOptions } from "#/lib/layout.shared";
import { withBasePath } from "#/lib/base-path";

const heroHeading = "Build Effect applications with Durable Streams";

const heroParagraph =
  "Streamsy is an Effect server and toolkit for Durable Streams. Create typed streams, append data, follow their changes, build projections and serve them over the official HTTP protocol.";

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

function Home() {
  return (
    <HomeLayout {...baseOptions()}>
      <main className="mx-auto w-full max-w-5xl px-6 py-16 sm:py-24">
        <h1 className="mt-5 max-w-3xl text-4xl font-semibold tracking-tight sm:text-6xl">
          {heroHeading}
        </h1>
        <p className="mt-6 max-w-2xl text-lg leading-8 text-fd-muted-foreground">{heroParagraph}</p>
        <div className="mt-8 flex flex-wrap gap-4">
          <a
            className="rounded-lg bg-fd-primary px-5 py-3 text-fd-primary-foreground"
            href={withBasePath("/docs")}
          >
            Get Started
          </a>
          <a className="rounded-lg border px-5 py-3" href={withBasePath("/articles")}>
            Read the blog
          </a>
        </div>
      </main>
    </HomeLayout>
  );
}
