import { createFileRoute, notFound } from "@tanstack/react-router";
import { createServerFn } from "@tanstack/react-start";
import { getRequestUrl } from "@tanstack/react-start/server";
import { HomeLayout } from "fumadocs-ui/layouts/home";
import { DocsBody } from "fumadocs-ui/layouts/docs/page";
import { useFumadocsLoader } from "fumadocs-core/source/client";
import browserCollections from "collections/browser";
import { Suspense } from "react";
import { articlesSource } from "#/lib/source";
import { baseOptions } from "#/lib/layout.shared";
import { useMDXComponents } from "#/components/mdx";

export const Route = createFileRoute("/articles/$slug")({
  component: ArticlePage,
  loader: async ({ params }) => {
    const data = await serverLoader({ data: params.slug });
    await clientLoader.preload(data.path);
    return data;
  },
  head: ({ loaderData }) => ({
    meta: loaderData
      ? [
          { title: `${loaderData.title} — Streamsy` },
          { name: "description", content: loaderData.description ?? "" },
          { property: "og:type", content: "article" },
          { property: "og:title", content: loaderData.title },
          { property: "og:description", content: loaderData.description ?? "" },
          { property: "og:image", content: loaderData.image },
          { property: "og:image:width", content: "1200" },
          { property: "og:image:height", content: "630" },
          { property: "og:image:type", content: "image/webp" },
          { name: "twitter:card", content: "summary_large_image" },
          { name: "twitter:title", content: loaderData.title },
          { name: "twitter:description", content: loaderData.description ?? "" },
          { name: "twitter:image", content: loaderData.image },
        ]
      : [],
  }),
});

const serverLoader = createServerFn({ method: "GET" })
  .inputValidator((slug: string) => slug)
  .handler(async ({ data: slug }) => {
    const page = articlesSource.getPage([slug]);
    if (!page) throw notFound();

    let image = "/og.webp";
    try {
      image = new URL(image, getRequestUrl().origin).href;
    } catch {
      // Fall back to the relative path if request context is unavailable.
    }

    return {
      path: page.path,
      title: page.data.title,
      description: page.data.description,
      image,
    };
  });

const clientLoader = browserCollections.articles.createClientLoader({
  component({ default: MDX }, _props: undefined) {
    return <MDX components={useMDXComponents()} />;
  },
});

function ArticlePage() {
  const data = useFumadocsLoader(Route.useLoaderData());
  return (
    <HomeLayout {...baseOptions()}>
      <article className="mx-auto w-full max-w-3xl px-6 py-14 sm:py-20">
        <DocsBody>
          <Suspense>{clientLoader.useContent(data.path)}</Suspense>
        </DocsBody>
      </article>
    </HomeLayout>
  );
}
