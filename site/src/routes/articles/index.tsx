import { createFileRoute, Link } from "@tanstack/react-router";
import { createServerFn } from "@tanstack/react-start";
import { HomeLayout } from "fumadocs-ui/layouts/home";
import { ArrowRight } from "lucide-react";
import { articlesSource } from "#/lib/source";
import { baseOptions } from "#/lib/layout.shared";

export const Route = createFileRoute("/articles/")({
  component: ArticlesIndex,
  loader: () => loadArticles(),
  head: () => ({
    meta: [
      { title: "Articles — Streamsy" },
      {
        name: "description",
        content: "Long-form writing about streams, sync, and the ideas behind Streamsy.",
      },
    ],
  }),
});

const loadArticles = createServerFn({ method: "GET" }).handler(() =>
  articlesSource
    .getPages()
    .map((page) => ({
      slug: page.slugs[0],
      title: page.data.title,
      description: page.data.description,
      date:
        page.data.date instanceof Date ? page.data.date.toISOString().slice(0, 10) : page.data.date,
    }))
    .filter((article): article is typeof article & { slug: string } => article.slug !== undefined)
    .sort((a, b) => b.date.localeCompare(a.date)),
);

const articleDateFormatter = new Intl.DateTimeFormat("en-GB", {
  day: "numeric",
  month: "long",
  year: "numeric",
  timeZone: "UTC",
});

function formatArticleDate(date: string) {
  return articleDateFormatter.format(new Date(`${date}T00:00:00Z`));
}

function ArticlesIndex() {
  const articles = Route.useLoaderData();

  return (
    <HomeLayout {...baseOptions()}>
      <div className="mx-auto w-full max-w-4xl px-6 py-16 sm:py-24">
        <header className="max-w-2xl">
          <p className="mb-3 font-mono text-sm font-medium text-fd-muted-foreground">
            Streamsy Articles
          </p>
          <h1 className="text-4xl font-semibold tracking-tight sm:text-5xl">
            Ideas for durable, local-first software
          </h1>
          <p className="mt-5 text-lg leading-8 text-fd-muted-foreground">
            Long-form writing about streams, sync, and the building blocks behind Streamsy.
          </p>
        </header>

        <section className="mt-14 grid gap-4 border-t pt-8">
          {articles.map((article) => (
            <Link
              key={article.slug}
              to="/articles/$slug"
              params={{ slug: article.slug }}
              className="group block rounded-xl border bg-fd-card p-6 transition-colors hover:bg-fd-accent/50 sm:p-8"
            >
              <time dateTime={article.date} className="font-mono text-sm text-fd-muted-foreground">
                {formatArticleDate(article.date)}
              </time>
              <h2 className="mt-3 text-2xl font-semibold tracking-tight">{article.title}</h2>
              {article.description ? (
                <p className="mt-3 max-w-2xl leading-7 text-fd-muted-foreground">
                  {article.description}
                </p>
              ) : null}
              <span className="mt-5 inline-flex items-center gap-2 font-medium">
                Read article{" "}
                <ArrowRight className="size-4 transition-transform group-hover:translate-x-1" />
              </span>
            </Link>
          ))}
        </section>
      </div>
    </HomeLayout>
  );
}
