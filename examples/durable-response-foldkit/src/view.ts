import type { Document, HtmlBuilder } from "foldkit/html";
import { Message } from "./message.ts";
import type { Model } from "./model.ts";

export const view = (model: Model, h: HtmlBuilder<Message>): Document => {
  const { snapshot } = model;

  return {
    title: "Durable response · Foldkit",
    body: h.main(
      [h.Class("shell")],
      [
        h.header(
          [],
          [
            h.div(
              [],
              [h.div([h.Class("eyebrow")], ["Streamsy · Foldkit"]), h.h1([], ["Durable response"])],
            ),
            h.span([h.Class("reader")], [`Reader: ${model.reader}`]),
          ],
        ),
        h.p([h.Class("intro")], ["One generation. Resume anywhere. Stop for everyone."]),
        h.section(
          [h.Class("card")],
          [
            ...(!model.id
              ? [
                  h.form(
                    [h.OnSubmit(Message.SubmittedPrompt())],
                    [
                      h.label([h.For("prompt")], ["Prompt"]),
                      h.textarea([
                        h.Id("prompt"),
                        h.Value(model.prompt),
                        h.OnInput((value) => Message.ChangedPrompt({ value })),
                      ]),
                      h.button(
                        [
                          h.Class("primary"),
                          h.Type("submit"),
                          h.Disabled(model.busy || !model.prompt.trim()),
                        ],
                        [model.busy ? "Starting…" : "Start response"],
                      ),
                    ],
                  ),
                ]
              : [
                  h.div([h.Class("prompt")], [`Prompt: ${snapshot.view.prompt || "Loading…"}`]),
                  h.div([h.Class("answer")], [snapshot.view.text || "Waiting for the first part…"]),
                  h.div([h.Class("status"), h.Role("status")], [snapshot.view.status]),
                  h.div(
                    [h.Class("controls")],
                    [
                      h.button(
                        [
                          h.Class("stop"),
                          h.Disabled(snapshot.view.terminal),
                          h.OnClick(Message.ClickedStop()),
                        ],
                        ["Stop"],
                      ),
                      h.button(
                        [
                          h.Disabled(model.mode !== "connected" || snapshot.closed),
                          h.OnClick(Message.ClickedDrop()),
                        ],
                        ["Drop connection"],
                      ),
                      h.button(
                        [
                          h.Disabled(model.mode !== "dropped" || snapshot.closed),
                          h.OnClick(Message.ClickedReconnect()),
                        ],
                        ["Reconnect"],
                      ),
                    ],
                  ),
                  h.div(
                    [h.Class("debug")],
                    [
                      h.span([], [`Read: ${model.mode}`]),
                      h.span([], [`Offset: ${snapshot.offset}`]),
                      h.span([], [`Resumed at: ${model.resumedAt}`]),
                      h.span([], [`Entries: ${snapshot.view.entries}`]),
                      h.span([], [`Model calls: ${model.calls} (scripted)`]),
                    ],
                  ),
                ]),
            h.div([h.Class("notice"), h.Role("alert")], [model.notice]),
          ],
        ),
        h.footer(
          [],
          [
            "Scripted model · SQLite log · ",
            h.a([h.Href("/")], ["New response"]),
            h.br([]),
            "Drop disconnects this reader. Stop interrupts the provider and closes the response for all readers.",
          ],
        ),
      ],
    ),
  };
};
