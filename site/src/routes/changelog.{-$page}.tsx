import { createFileRoute, notFound } from "@tanstack/react-router";
import { createServerFn } from "@tanstack/react-start";
import { HomeLayout } from "fumadocs-ui/layouts/home";
import { ChangelogPagination, ChangelogTimeline } from "#/components/changelog";
import { withBasePath } from "#/lib/base-path";
import { baseOptions } from "#/lib/layout.shared";
import { getCachedReleases } from "#/lib/releases.server";

const getReleases = createServerFn({ method: "GET" })
  .validator((page: number) => page)
  .handler(async ({ data: page }) => {
    if (!Number.isSafeInteger(page) || page < 1) throw notFound();

    let releases;

    try {
      releases = await getCachedReleases(page);
    } catch {
      return { unavailable: true as const, entries: [], nextPage: undefined };
    }

    if (page > 1 && releases.entries.length === 0) throw notFound();

    return { unavailable: false as const, ...releases };
  });

export const Route = createFileRoute("/changelog/{-$page}")({
  loader: async ({ params }) => {
    const page = Number(params.page ?? 1);

    return { page, ...(await getReleases({ data: page })) };
  },
  head: () => ({ meta: [{ title: "Changelog — Streamsy" }] }),
  component: Page,
});

function Page() {
  const { page, entries, nextPage, unavailable } = Route.useLoaderData();

  return (
    <HomeLayout {...baseOptions()}>
      <div className="mx-auto flex w-full max-w-[900px] flex-col gap-10 px-4 py-12">
        <h1 className="text-3xl font-semibold">Changelog</h1>
        {unavailable ? (
          <p>
            Releases are temporarily unavailable. View the{" "}
            <a className="underline" href="https://github.com/gingerhendrix/streamsy/releases">
              GitHub releases
            </a>
            .
          </p>
        ) : entries.length === 0 ? (
          <p>No releases yet.</p>
        ) : (
          <ChangelogTimeline entries={entries} />
        )}
        <ChangelogPagination
          newer={
            page > 1
              ? withBasePath(page === 2 ? "/changelog" : `/changelog/${page - 1}`)
              : undefined
          }
          older={nextPage ? withBasePath(`/changelog/${nextPage}`) : undefined}
        />
      </div>
    </HomeLayout>
  );
}
