export type HttpLiveMode = "long-poll" | "sse";

export type ReadQueryResult =
  | { ok: true; offset?: string; live?: HttpLiveMode; cursor?: string; batchSize?: number }
  | { ok: false; response: Response };

export class ReadQueryParser {
  constructor(private isValidOffset: (offset: string) => boolean) {}

  parse(url: URL): ReadQueryResult {
    const offset = url.searchParams.get("offset") ?? undefined;
    const liveParam = url.searchParams.get("live");
    const cursor = url.searchParams.get("cursor") ?? undefined;
    const live = liveParam === "long-poll" || liveParam === "sse" ? liveParam : undefined;
    const batchSizeParam = url.searchParams.get("batch_size");
    const batchSize = batchSizeParam === null ? undefined : Number(batchSizeParam);

    if (
      offset !== undefined &&
      offset !== "-1" &&
      offset !== "now" &&
      !this.isValidOffset(offset)
    ) {
      return { ok: false, response: new Response("Invalid offset format", { status: 400 }) };
    }
    if (
      batchSize !== undefined &&
      (!Number.isInteger(batchSize) || batchSize < 1 || batchSize > 10_000)
    ) {
      return { ok: false, response: new Response("Invalid batch_size", { status: 400 }) };
    }

    if (
      cursor !== undefined &&
      (!/^(0|[1-9]\d*)$/.test(cursor) ||
        !Number.isSafeInteger(Number(cursor)) ||
        Number(cursor) > Number.MAX_SAFE_INTEGER - 180)
    ) {
      return { ok: false, response: new Response("Invalid cursor", { status: 400 }) };
    }
    return { ok: true, offset, live, cursor, batchSize };
  }
}
