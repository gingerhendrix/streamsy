import { Array } from "effect";
import { createFileRoute, notFound, redirect, useNavigate } from "@tanstack/react-router";
import { DocsLayout } from "fumadocs-ui/layouts/docs";
import { createServerFn } from "@tanstack/react-start";
import { getRequestUrl } from "@tanstack/react-start/server";
import { forwardedSection, movedPages } from "#/lib/moved-pages";
import { getPageImage, source } from "#/lib/source";
import browserCollections from "collections/browser";
import { DocsBody, DocsDescription, DocsPage, DocsTitle } from "fumadocs-ui/layouts/docs/page";
import { baseOptions } from "#/lib/layout.shared";
import { useFumadocsLoader } from "fumadocs-core/source/client";
import { Suspense, useEffect } from "react";
import { useMDXComponents } from "#/components/mdx";

/** The slug path of a docs URL, without empty segments. */
const slugPath = (splat: string | undefined): string =>
  splat === undefined ? "" : Array.join(Array.filter(splat.split("/"), Boolean), "/");

export const Route = createFileRoute("/docs/$")({
  component: Page,
  beforeLoad: ({ params }) => {
    const moved = movedPages.get(slugPath(params["_splat"]));

    if (moved !== undefined) {
      throw redirect({ to: "/docs/$", params: { _splat: moved }, statusCode: 308 });
    }
  },
  loader: async ({ params }) => {
    const splat = params["_splat"];
    const slugs = splat === undefined ? [] : Array.filter(splat.split("/"), Boolean);
    const data = await serverLoader({ data: slugs });
    await clientLoader.preload(data.path);

    return data;
  },
  head: ({ loaderData }) => ({
    meta: loaderData
      ? [
          { title: `${loaderData.title} — Streamsy Docs` },
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

const serverLoader = createServerFn({
  method: "GET",
})
  .validator((slugs: string[]) => slugs)
  .handler(async ({ data: slugs }) => {
    const page = source.getPage(slugs);

    if (!page) throw notFound();

    // Absolute og:image URL (crawlers like X/Twitter require absolute URLs).
    const imagePath = getPageImage(page).url;
    let image = imagePath;

    try {
      image = new URL(imagePath, getRequestUrl().origin).href;
    } catch {
      // Fall back to the relative path if request context is unavailable.
    }

    return {
      path: page.path,
      title: page.data.title,
      description: page.data.description,
      image,
      pageTree: await source.serializePageTree(source.getPageTree()),
    };
  });

const clientLoader = browserCollections.docs.createClientLoader({
  component({ toc, frontmatter, default: MDX }, _props: undefined) {
    return (
      <DocsPage toc={toc}>
        <DocsTitle>{frontmatter.title}</DocsTitle>
        <DocsDescription>{frontmatter.description}</DocsDescription>
        <DocsBody>
          <MDX components={useMDXComponents()} />
        </DocsBody>
      </DocsPage>
    );
  },
});

/**
 * Forwards a fragment that names a moved section to the page and heading that hold it
 * now. The server never sees a fragment, so this runs in the browser, on load and on
 * each hash change. Render it inside the `Suspense` boundary of the page content: its
 * effect then runs after that content hydrates, so the server HTML is hydrated under
 * the URL it was rendered for.
 */
function MovedSections({ splat }: { readonly splat: string | undefined }) {
  const navigate = useNavigate();

  useEffect(() => {
    const forward = () => {
      const target = forwardedSection(slugPath(splat), window.location.hash);

      if (target !== undefined)
        void navigate({
          to: "/docs/$",
          params: { _splat: target.slug },
          hash: target.hash,
          replace: true,
        });
    };

    forward();
    window.addEventListener("hashchange", forward);

    return () => window.removeEventListener("hashchange", forward);
  }, [splat, navigate]);

  return null;
}

function Page() {
  const data = useFumadocsLoader(Route.useLoaderData());
  const splat = Route.useParams()["_splat"];

  return (
    <DocsLayout {...baseOptions()} tree={data.pageTree}>
      <Suspense>
        {clientLoader.useContent(data.path)}
        <MovedSections splat={splat} />
      </Suspense>
    </DocsLayout>
  );
}
