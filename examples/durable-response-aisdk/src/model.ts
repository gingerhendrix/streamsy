/* oxlint-disable effecttsgo/async-function, effecttsgo/new-promise, effecttsgo/global-timers -- Native AI SDK test provider implements its Promise/ReadableStream contract. */
import { MockLanguageModelV3 } from "ai/test";

export const chunks = [
  "Start with a question in one tab. ",
  "The server runs the model once ",
  "and records its output in a SQLite log. ",
  "The chat hook reads that log through the transport ",
  "and turns successive text parts into ",
  "the assistant message you see here.\n\n",
  "Reload before the answer finishes. ",
  "The page loses its in-memory chat state, ",
  "but the server keeps recording the answer. ",
  "The new chat hook replays the stored parts from the beginning ",
  "to rebuild the message, then follows new parts as they arrive. ",
  "You do not have to send your question again.\n\n",
  "A second tab uses that same replay path. ",
  "It can catch up with the first tab ",
  "even if it opens after the model has finished. ",
  "Look at the model call count to check ",
  "that neither reader started another generation.\n\n",
  "To end the generation early, press Stop in either tab. ",
  "The server interrupts the model ",
  "and records the end of the answer. ",
  "Both tabs keep the text already written, ",
  "and a reload shows the same partial message and stop reason.",
];

/** A real AI SDK model interface, paced at the provider boundary, with observable abort. */
export const scriptedModel = (paceMs: number) => {
  const stats = { calls: 0, interruptions: 0 };

  const model = new MockLanguageModelV3({
    doStream: async ({ abortSignal }) => {
      stats.calls++;
      let index = -1;
      let terminal = false;

      return {
        stream: new ReadableStream({
          async pull(controller) {
            if (terminal) return;
            // A cancelled read does not cancel this provider. Only the server's signal does.
            await new Promise<void>((resolve) => {
              const done = () => {
                clearTimeout(timer);
                abortSignal?.removeEventListener("abort", done);
                resolve();
              };

              const timer = setTimeout(done, paceMs);

              if (abortSignal?.aborted) done();
              else abortSignal?.addEventListener("abort", done, { once: true });
            });

            if (abortSignal?.aborted) {
              terminal = true;
              stats.interruptions++;
              controller.error(abortSignal.reason);

              return;
            }

            if (index === -1) controller.enqueue({ type: "text-start", id: "answer" });
            else if (index < chunks.length)
              controller.enqueue({ type: "text-delta", id: "answer", delta: chunks[index] ?? "" });
            else {
              terminal = true;
              controller.enqueue({ type: "text-end", id: "answer" });
              controller.enqueue({
                type: "finish",
                finishReason: { unified: "stop", raw: "stop" },
                usage: {
                  inputTokens: { total: 1, noCache: 1, cacheRead: 0, cacheWrite: 0 },
                  outputTokens: { total: 10, text: 10, reasoning: 0 },
                },
              });
              controller.close();
            }

            index++;
          },
        }),
      };
    },
  });

  return { model, stats };
};
