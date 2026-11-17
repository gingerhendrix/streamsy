import { Result, Schema } from "effect";
import { Stopped, type ResponseEntry } from "./entry-schema.ts";

export const ResponseView = Schema.Struct({
  text: Schema.String,
  prompt: Schema.String,
  status: Schema.String,
  entries: Schema.Int,
  terminal: Schema.Boolean,
});

export type ResponseView = typeof ResponseView.Type;

export const emptyView: ResponseView = {
  text: "",
  prompt: "",
  status: "Generating",
  entries: 0,
  terminal: false,
};

export const foldResponse = (state: ResponseView, part: ResponseEntry): ResponseView => {
  if (state.terminal) return state;
  const next = { ...state, entries: state.entries + 1 };

  switch (part.type) {
    case "response-metadata": {
      const request = Schema.decodeUnknownResult(Schema.Struct({ prompt: Schema.String }))(
        part.metadata.request,
      );

      return Result.isSuccess(request) ? { ...next, prompt: request.success.prompt } : next;
    }

    case "text-delta":
      return { ...next, text: state.text + part.delta };
    case "error":
      return { ...next, terminal: true, status: `Error: ${String(part.error)}` };
    case "finish": {
      const stopped = Schema.decodeUnknownResult(Stopped)(part.metadata.stopped);

      return {
        ...next,
        terminal: true,
        status: Result.isSuccess(stopped)
          ? `Stopped by ${stopped.success.by} at ${stopped.success.at}: ${stopped.success.reason}`
          : "Finished",
      };
    }

    default:
      return next;
  }
};

export const ResumeSnapshot = Schema.Struct({
  offset: Schema.String,
  view: ResponseView,
  closed: Schema.Boolean,
});

export type ResumeSnapshot = typeof ResumeSnapshot.Type;

export const emptySnapshot: ResumeSnapshot = { offset: "-1", view: emptyView, closed: false };

export const ReadBatch = Schema.Struct({
  items: Schema.Array(Schema.Unknown),
  offset: Schema.String,
  closed: Schema.Boolean,
});

export type ReadBatch = typeof ReadBatch.Type;
