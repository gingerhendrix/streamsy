import type { Update } from "foldkit";
import { applyBatch } from "@streamsy/durable-response-contract";
import { Start, Stop, Save } from "./command.ts";
import { Message } from "./message.ts";
import type { Flags, Model } from "./model.ts";
import type { Resources } from "./resources.ts";

type Return = Update.Return<Model, Message, Resources>;

export const init = (flags: Flags): Return => ({
  model: {
    ...flags,
    prompt: "Explain durable streams",
    resumedAt: flags.snapshot.offset,
    mode: flags.snapshot.closed ? "ended" : "connected",
    calls: 0,
    notice: "",
    busy: false,
  },
});

export const update = (model: Model, message: Message): Return =>
  Message.match(message, {
    ReceivedBatch: ({ batch }): Return => {
      if (model.mode !== "connected") return { model };

      try {
        const snapshot = applyBatch(model.snapshot, batch);

        return {
          model: { ...model, snapshot, mode: snapshot.closed ? "ended" : "connected" },
          commands: [Save({ id: model.id, snapshot })],
        };
      } catch {
        return { model: { ...model, mode: "dropped", notice: "Invalid response part" } };
      }
    },
    FailedRead: ({ error }): Return => ({ model: { ...model, mode: "dropped", notice: error } }),
    ChangedPrompt: ({ value }) => ({ model: { ...model, prompt: value } }),
    SubmittedPrompt: () =>
      model.busy || !model.prompt.trim()
        ? { model }
        : { model: { ...model, busy: true }, commands: [Start({ prompt: model.prompt })] },
    ClickedDrop: (): Return => ({
      model: { ...model, mode: model.snapshot.closed ? "ended" : "dropped" },
    }),
    ClickedReconnect: (): Return => ({
      model: {
        ...model,
        mode: model.snapshot.closed ? "ended" : "connected",
        resumedAt: model.snapshot.offset,
        notice: "",
      },
    }),
    ClickedStop: () =>
      model.snapshot.view.terminal
        ? { model }
        : { model, commands: [Stop({ id: model.id, reader: model.reader })] },
    ReceivedStopReply: ({ alreadyClosed }) => ({
      model: { ...model, notice: alreadyClosed ? "Already finished" : "Stop requested" },
    }),
    FailedWrite: ({ error }) => ({ model: { ...model, notice: error, busy: false } }),
    ReceivedModelCalls: ({ calls }) => ({ model: { ...model, calls } }),
    SavedResume: () => ({ model }),
    Navigated: () => ({ model }),
  });
