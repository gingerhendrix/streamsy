/**
 * Just enough of RFC 9110 §12.5.1 to choose between two representations.
 *
 * This is not a general content negotiator: the actions resource offers exactly
 * two readings, and the questions worth asking are "did the caller accept this
 * one" and "did they prefer it". Media types are case-insensitive, `q=0` means
 * *unacceptable* rather than merely unpreferred, and wildcards apply only where
 * nothing more specific matched — those three rules are the whole of it, and
 * substring matching gets all three wrong.
 */

interface MediaRange {
  type: string;
  subtype: string;
  quality: number;
}

function parseAcceptHeader(header: string): MediaRange[] {
  const ranges: MediaRange[] = [];
  for (const entry of header.split(",")) {
    const [rawType, ...parameters] = entry.split(";");
    const media = (rawType ?? "").trim().toLowerCase();
    if (!media) continue;
    const slash = media.indexOf("/");
    if (slash === -1) continue;
    let quality = 1;
    for (const parameter of parameters) {
      const [name, value] = parameter.split("=");
      if ((name ?? "").trim().toLowerCase() !== "q") continue;
      const parsed = Number.parseFloat((value ?? "").trim());
      quality = Number.isFinite(parsed) ? Math.min(Math.max(parsed, 0), 1) : 1;
    }
    ranges.push({ type: media.slice(0, slash), subtype: media.slice(slash + 1), quality });
  }
  return ranges;
}

/**
 * The quality the caller assigned to one media type. The most specific matching
 * range wins, as precedence requires: an explicit `text/event-stream;q=0`
 * refuses the stream even alongside a generous wildcard.
 */
function qualityOf(ranges: MediaRange[], type: string, subtype: string): number {
  const bySpecificity = [
    (range: MediaRange) => range.type === type && range.subtype === subtype,
    (range: MediaRange) => range.type === type && range.subtype === "*",
    (range: MediaRange) => range.type === "*" && range.subtype === "*",
  ];
  for (const matches of bySpecificity) {
    const match = ranges.find(matches);
    if (match) return match.quality;
  }
  return 0;
}

/**
 * Does this request want the immediate JSON page rather than the stream?
 *
 * The stream is the resource's contract, so it wins an absent header, a
 * wildcard, and a tie. JSON is chosen only when the caller both accepts it and
 * prefers it to the stream — including the case where they explicitly refused
 * the stream with `q=0`.
 */
export function prefersJsonOverEventStream(request: Request): boolean {
  const header = request.headers.get("accept");
  if (!header) return false;
  const ranges = parseAcceptHeader(header);
  const json = qualityOf(ranges, "application", "json");
  const stream = qualityOf(ranges, "text", "event-stream");
  return json > 0 && json > stream;
}
