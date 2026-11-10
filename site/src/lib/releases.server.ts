import { githubReleases } from "@fumadocs/changelog/github";
import { getRequestUrl } from "@tanstack/react-start/server";
import { z } from "zod";

const releasesSchema = z.object({
  entries: z.array(
    z.object({
      id: z.string(),
      title: z.string(),
      date: z.coerce.date(),
      versions: z.array(z.object({ name: z.string(), version: z.string() })).optional(),
      url: z.string().optional(),
      content: z.string().optional(),
      prerelease: z.boolean(),
    }),
  ),
  nextPage: z.number().optional(),
});

/** Workers share an egress IP, so keep each GitHub page in the edge cache for an hour. */
export async function getCachedReleases(page: number) {
  // caches.default is a Workers extension and is absent in the Vite dev server.
  const storage: (CacheStorage & { default?: Cache }) | undefined = globalThis.caches;
  const cache = storage?.default;

  const key = new Request(
    new URL(`/__cache/github-releases/streamsy/${page}`, getRequestUrl().origin),
  );

  try {
    const cached = await cache?.match(key);

    if (cached) return releasesSchema.parse(await cached.json());
  } catch {
    // A cache failure must not prevent a fresh GitHub request.
  }

  const releases = await githubReleases({
    owner: "gingerhendrix",
    repo: "streamsy",
    token: process.env.GITHUB_TOKEN,
    page,
  });

  try {
    await cache?.put(
      key,
      Response.json(releases, {
        headers: { "Cache-Control": "public, max-age=3600" },
      }),
    );
  } catch {
    // Releases are still usable when the cache is unavailable.
  }

  return releases;
}
