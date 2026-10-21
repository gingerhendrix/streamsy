import { Array } from "effect";
import type { SearchResult } from "../server/index-rows.ts";

/** React escapes every text segment. The server supplies only the match flags. */
export function Snippet({ parts }: { parts: SearchResult["snippet"] }) {
  return (
    <>
      {Array.map(parts, (part, index) =>
        part.match ? <mark key={index}>{part.text}</mark> : <span key={index}>{part.text}</span>,
      )}
    </>
  );
}
