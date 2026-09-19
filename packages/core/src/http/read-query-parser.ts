export type HttpLiveMode = "long-poll" | "sse";

export type ReadQueryResult =
  | { ok: true; offset?: string; live?: HttpLiveMode; cursor?: string }
  | { ok: false; response: Response };

export const readQueryParser = (isValidOffset: (offset: string) => boolean) => ({
  parse(url: URL): ReadQueryResult {
    const offset = url.searchParams.get("offset") ?? undefined;
    const liveParam = url.searchParams.get("live");
    const cursor = url.searchParams.get("cursor") ?? undefined;
    const live = liveParam === "long-poll" || liveParam === "sse" ? liveParam : undefined;

    if (offset !== undefined && offset !== "-1" && offset !== "now" && !isValidOffset(offset)) {
      return { ok: false, response: new Response("Invalid offset format", { status: 400 }) };
    }

    if (
      cursor !== undefined &&
      (!/^(0|[1-9]\d*)$/.test(cursor) ||
        !Number.isSafeInteger(Number(cursor)) ||
        Number(cursor) > Number.MAX_SAFE_INTEGER - 180)
    ) {
      return { ok: false, response: new Response("Invalid cursor", { status: 400 }) };
    }
    return { ok: true, offset, live, cursor };
  },
});
