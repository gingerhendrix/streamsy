/* oxlint-disable effecttsgo/async-function -- The example-local source wrapper implements the native framework's AsyncIterable contract. */
import { chat, EventType, type StreamChunk } from "@tanstack/ai";
import { fakeText } from "@tanstack/ai/testing";
import { STOP_EVENT, type Stopped } from "./contract.ts";

export const answer =
  "A durable response is recorded once. Reload to replay the same answer. Any reader can stop the native model, and every reader sees the recorded reason.";

export interface Running {
  controller: AbortController;
  task: Promise<void>;
  decision: "running" | "finished" | "stopped" | "error";
  stopped?: Stopped;
}

/** Native chat/fakeText owns generation and observes abort; this app owns the durable stop convention. */
export const scriptedModel = (paceMs: number) => {
  const adapter = fakeText({ tokensPerSecond: paceMs === 0 ? undefined : 1000 / paceMs });
  let interruptions = 0;
  const stats = () => ({ calls: adapter.state.callCount, interruptions });

  const source = async function* (
    id: string,
    prompt: string,
    run: Running,
  ): AsyncIterable<StreamChunk> {
    adapter.appendResponses([{ text: answer }]);

    const native = chat({
      threadId: id,
      runId: id,
      adapter,
      messages: [{ role: "user", content: prompt }],
      abortController: run.controller,
      middleware: [
        {
          name: "durable-response-observer",
          onAbort: () => {
            interruptions++;
          },
        },
      ],
    });

    let openText: string | undefined;

    try {
      for await (const chunk of native) {
        // The native iterator still drains after cancellation so its onAbort hook observes the provider exit.
        if (run.decision === "stopped") continue;

        if (chunk.type === EventType.RUN_FINISHED) run.decision = "finished";

        if (chunk.type === EventType.RUN_ERROR) run.decision = "error";

        if (chunk.type === EventType.TEXT_MESSAGE_START) openText = chunk.messageId;

        if (chunk.type === EventType.TEXT_MESSAGE_END) openText = undefined;

        if (chunk.type === EventType.RUN_STARTED) {
          yield { ...chunk, metadata: { ...chunk.metadata, durableResponse: { prompt } } };
        } else yield chunk;
      }
    } catch {
      if (run.decision === "running") {
        run.decision = "error";
        yield {
          type: EventType.RUN_ERROR,
          threadId: id,
          runId: id,
          message: "Native generation failed",
        };
      }
    }

    if (run.decision === "stopped" && run.stopped) {
      if (openText) yield { type: EventType.TEXT_MESSAGE_END, messageId: openText };
      // Explicit owner-authorized convention: native abort itself emits neither of these events.
      yield { type: EventType.CUSTOM, name: STOP_EVENT, value: run.stopped };
      yield {
        type: EventType.RUN_FINISHED,
        threadId: id,
        runId: id,
        outcome: { type: "cancelled" },
      };
    } else if (run.decision === "running") {
      run.decision = "error";
      yield {
        type: EventType.RUN_ERROR,
        threadId: id,
        runId: id,
        message: "Native generation ended without a terminal event",
      };
    }
  };

  return { source, stats };
};
