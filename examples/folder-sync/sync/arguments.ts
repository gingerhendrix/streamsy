import { Schema } from "effect";
import type { SyncOptions } from "./decisions.ts";

export class ArgumentsError extends Schema.TaggedError<ArgumentsError>()("ArgumentsError", {
  message: Schema.String,
}) {}

export const usage =
  "bun run sync --server <url> --dir <path> [--folders a,b] [--write a,b] [--token <writer-token>]\n bun run sync rejected --dir <path>";

/** Argument parsing is pure; the executable edge owns argv and runtime execution. */
export const parseArguments = (args: ReadonlyArray<string>): SyncOptions => {
  const values = new Map<string, string>();

  for (let i = 0; i < args.length; i += 2) {
    const key = args[i];
    const value = args[i + 1];

    if (
      key === undefined ||
      !["--server", "--dir", "--folders", "--write", "--token"].includes(key) ||
      value === undefined ||
      value.startsWith("--") ||
      values.has(key)
    )
      throw new ArgumentsError({ message: usage });
    values.set(key, value);
  }

  const server = values.get("--server");
  const directory = values.get("--dir");

  if (server === undefined || directory === undefined || directory === "")
    throw new ArgumentsError({ message: usage });
  const url = new URL(server);

  if (!["http:", "https:"].includes(url.protocol) || url.search || url.hash)
    throw new ArgumentsError({
      message: "--server must be an HTTP URL without a query or fragment",
    });
  const folders = values.get("--folders")?.split(",");
  const writable = values.get("--write")?.split(",") ?? [];

  if (
    folders?.includes("") ||
    writable.includes("") ||
    writable.some((id) => folders !== undefined && !folders.includes(id))
  )
    throw new ArgumentsError({
      message: "--write must name selected folders; folder IDs cannot be empty",
    });

  const token = values.get("--token");
  const common: SyncOptions = { server: url.href.replace(/\/$/, ""), directory, writable };
  const authenticated = token === undefined ? common : { ...common, token };

  return folders === undefined ? authenticated : { ...authenticated, folders };
};
