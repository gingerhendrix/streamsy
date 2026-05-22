export type ReadLiveMode = "long-poll" | "sse";

export type ReadQueryResult =
  | { ok: true; offset?: string; live?: ReadLiveMode; cursor?: string }
  | { ok: false; response: Response };

export class ReadQueryParser {
  parse(url: URL): ReadQueryResult {
    const offset = url.searchParams.get("offset") ?? undefined;
    const liveParam = url.searchParams.get("live");
    const cursor = url.searchParams.get("cursor") ?? undefined;
    const live = liveParam === "long-poll" || liveParam === "sse" ? liveParam : undefined;

    if (offset !== undefined && offset !== "-1" && offset !== "now" && !/^\d+_\d+$/.test(offset)) {
      return { ok: false, response: new Response("Invalid offset format", { status: 400 }) };
    }

    return { ok: true, offset, live, cursor };
  }
}
