/* oxlint-disable effecttsgo/async-function, effecttsgo/new-promise, effecttsgo/global-timers -- Native AI SDK test provider implements its Promise/ReadableStream contract. */
import { MockLanguageModelV3 } from "ai/test";

export const chunks = [
  "A durable response ",
  "is written once ",
  "to a persistent log. ",
  "Each reader follows ",
  "the same recorded parts. ",
  "Reload this page ",
  "while the model continues. ",
  "Another reader can join ",
  "and any reader can stop ",
  "the generation for everyone.",
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
