/* oxlint-disable effecttsgo/async-function -- The example-local source wrapper implements the native framework's AsyncIterable contract. */
import { chat, EventType, type StreamChunk } from "@tanstack/ai";
import { fakeText } from "@tanstack/ai/testing";
import { STOP_EVENT, type Stopped } from "./contract.ts";

export const answer = `A durable response starts with one model call. As the model writes, the server saves each text event in a SQLite log. The chat hook uses those events to build the assistant message you see here.

Open this response in a second tab while the answer is still arriving. The new tab replays the events it missed, then follows the live stream. Reloading works the same way: the chat hook rebuilds the message from recorded events without asking the model to repeat its answer.

Try pressing Stop in either tab. The server aborts the model and saves a separate event with the stop reason before it closes the stream. The other tab gets the same partial text and stop reason, even though it never sent a stop request.

Come back to the response later. The completed log still has everything the model wrote before it finished or was stopped. A late reader can reconstruct that answer without a running model.`;

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
