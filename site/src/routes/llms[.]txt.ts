import { createFileRoute } from "@tanstack/react-router";
import { docsLlms } from "#/lib/source";

export const Route = createFileRoute("/llms.txt")({
  server: {
    handlers: {
      GET: () =>
        new Response(docsLlms.index(), {
          headers: { "Content-Type": "text/plain; charset=utf-8" },
        }),
    },
  },
});
