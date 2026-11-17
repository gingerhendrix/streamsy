import { YjsProvider } from "@durable-streams/y-durable-streams";
import { baseKeymap, setBlockType, toggleMark } from "prosemirror-commands";
import { keymap } from "prosemirror-keymap";
import { schema } from "prosemirror-schema-basic";
import { EditorState, type Command } from "prosemirror-state";
import { EditorView } from "prosemirror-view";
import { Awareness } from "y-protocols/awareness";
import { redo, undo, yCursorPlugin, ySyncPlugin, yUndoPlugin } from "y-prosemirror";
import * as Y from "yjs";
import "prosemirror-view/style/prosemirror.css";
import "./style.css";

// oxlint-disable-next-line typescript/no-unnecessary-type-parameters -- Caller specifies the expected DOM element type for querySelector.
const element = <T extends HTMLElement>(selector: string) => {
  const found = document.querySelector<T>(selector);

  if (!found) throw new Error(`Missing ${selector}`);

  return found;
};

const rooms = [
  { id: "notes", title: "Notes", detail: "The everyday scratchpad" },
  { id: "ideas", title: "Ideas", detail: "Room for the next thing" },
  { id: "draft", title: "Draft", detail: "A work in progress" },
];

const colors = ["#276b64", "#aa4c32", "#7059a6", "#386b9c"];

const color = colors[crypto.getRandomValues(new Uint8Array(1))[0]! % colors.length]!;

const name = element<HTMLInputElement>("#name");

name.value = `Writer ${crypto.getRandomValues(new Uint16Array(1))[0]!.toString().slice(0, 3)}`;

let dispose = () => {};

let currentRoom = "";

const openRoom = (roomId: string) => {
  let room = rooms[0]!;

  for (const candidate of rooms) if (candidate.id === roomId) room = candidate;

  if (room.id === currentRoom) return;
  dispose();
  currentRoom = room.id;
  element("#room-title").textContent = room.title;

  for (const button of document.querySelectorAll<HTMLButtonElement>("#rooms button"))
    button.setAttribute("aria-current", String(button.dataset.room === room.id));
  const doc = new Y.Doc();
  const awareness = new Awareness(doc);
  awareness.setLocalStateField("user", { name: name.value, color });

  const provider = new YjsProvider({
    doc,
    awareness,
    baseUrl: `${location.origin}/v1/yjs/editor`,
    docId: room.id,
    connect: false,
  });

  const view = new EditorView(element("#editor"), {
    state: EditorState.create({
      schema,
      plugins: [
        ySyncPlugin(doc.getXmlFragment("prosemirror")),
        yCursorPlugin(awareness),
        yUndoPlugin(),
        keymap({ "Mod-z": undo, "Mod-y": redo, "Mod-Shift-z": redo }),
        keymap(baseKeymap),
      ],
    }),
    editable: () => provider.connected,
    attributes: {
      "aria-label": `${room.title} shared editor`,
      role: "textbox",
      "aria-multiline": "true",
      "data-placeholder": "Start a thought. Someone else can finish it.",
    },
  });

  const toolbar = element("#toolbar");
  toolbar.replaceChildren();

  const addCommand = (label: string, title: string, command: Command) => {
    const button = document.createElement("button");
    button.textContent = label;
    button.title = title;
    button.setAttribute("aria-label", title);
    button.addEventListener("mousedown", (event) => event.preventDefault());
    button.addEventListener("click", () => {
      if (provider.connected) command(view.state, view.dispatch, view);
      view.focus();
    });
    toolbar.append(button);
  };

  addCommand("B", "Bold", toggleMark(schema.marks.strong));
  addCommand("I", "Italic", toggleMark(schema.marks.em));
  addCommand("H2", "Heading", setBlockType(schema.nodes.heading, { level: 2 }));
  addCommand("¶", "Paragraph", setBlockType(schema.nodes.paragraph));
  addCommand("↶", "Undo", undo);
  addCommand("↷", "Redo", redo);

  const renderPeople = () => {
    const list = element("#people");
    list.replaceChildren();

    for (const [id, state] of awareness.getStates()) {
      const item = document.createElement("li");
      item.textContent = `${state.user?.name || "Anonymous"}${id === doc.clientID ? " (you)" : ""}`;
      item.style.setProperty("--person-color", state.user?.color || "#777");
      list.append(item);
    }
  };

  awareness.on("change", renderPeople);
  renderPeople();

  const rename = () =>
    awareness.setLocalStateField("user", { name: name.value.trim() || "Anonymous", color });

  name.addEventListener("input", rename);
  provider.on("status", (status) => {
    element("#status").textContent = {
      connected: "● Live",
      connecting: "Connecting…",
      disconnected: "Disconnected",
    }[status];
    element("#status").dataset.state = status;
    element("#editor-footer").textContent =
      status === "connected"
        ? "Changes sync while connected"
        : "Editing paused. Reload to reconnect.";
    view.setProps({ editable: () => provider.connected });
  });
  provider.on("error", (error) => {
    element("#editor-footer").textContent =
      `Connection error: ${error.message}. Reload to reconnect.`;
  });
  void provider.connect();
  dispose = () => {
    name.removeEventListener("input", rename);
    provider.destroy();
    view.destroy();
    awareness.destroy();
    doc.destroy();
  };
};

for (const room of rooms) {
  const button = document.createElement("button");
  button.dataset.room = room.id;
  const title = document.createElement("strong");
  title.textContent = room.title;
  const detail = document.createElement("span");
  detail.textContent = room.detail;
  button.append(title, detail);
  button.addEventListener("click", () => {
    location.hash = room.id;
  });
  element("#rooms").append(button);
}

window.addEventListener("hashchange", () => openRoom(location.hash.slice(1)));

window.addEventListener("pagehide", () => dispose());

openRoom(location.hash.slice(1));
