import { YjsProvider, type YjsProviderStatus } from "@streamsy/y-durable-streams";
import { nanoid } from "nanoid";
import { baseKeymap, setBlockType, toggleMark } from "prosemirror-commands";
import { keymap } from "prosemirror-keymap";
import { schema } from "prosemirror-schema-basic";
import { EditorState, type Command } from "prosemirror-state";
import { EditorView } from "prosemirror-view";
import { Awareness } from "y-protocols/awareness";
import { redo, undo, yCursorPlugin, ySyncPlugin, yUndoPlugin } from "y-prosemirror";
import * as Y from "yjs";
import "prosemirror-view/style/prosemirror.css";
import { roomIdFromHash } from "./room-link.ts";
import "./style.css";

// oxlint-disable-next-line typescript/no-unnecessary-type-parameters -- Caller specifies the expected DOM element type for querySelector.
const element = <T extends HTMLElement>(selector: string) => {
  const found = document.querySelector<T>(selector);

  if (!found) throw new Error(`Missing ${selector}`);

  return found;
};

const colors = ["#276b64", "#aa4c32", "#7059a6", "#386b9c"];

const color = colors[crypto.getRandomValues(new Uint8Array(1))[0]! % colors.length]!;

const name = element<HTMLInputElement>("#name");

name.value = `Writer ${crypto.getRandomValues(new Uint16Array(1))[0]!.toString().slice(0, 3)}`;

const statusBadge = element("#status");

const editorFooter = element("#editor-footer");

const renderError = (error: Error) => {
  editorFooter.textContent = `Connection error: ${error.message}. Reload to reconnect.`;
};

const openRoom = (roomId: string) => {
  const doc = new Y.Doc();
  const awareness = new Awareness(doc);
  awareness.setLocalStateField("user", { name: name.value, color });

  const provider = new YjsProvider({
    doc,
    awareness,
    baseUrl: `${location.origin}/v1/yjs/editor`,
    docId: roomId,
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
    dispatchTransaction(this: EditorView, transaction) {
      // y-prosemirror can dispatch a queued awareness update after the view is destroyed.
      if (!this.isDestroyed) this.updateState(this.state.apply(transaction));
    },
    editable: () => provider.connected,
    attributes: {
      "aria-label": "Shared editor",
      role: "textbox",
      "aria-multiline": "true",
      "data-placeholder": "Start a thought. Someone else can finish it.",
    },
  });

  const toolbar = element("#toolbar");

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

  const list = element("#people");

  const renderPeople = () => {
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

  const renderStatus = (status: YjsProviderStatus) => {
    statusBadge.textContent = {
      connected: "● Live",
      connecting: "Connecting…",
      disconnected: "Disconnected",
    }[status];
    statusBadge.dataset.state = status;
    editorFooter.textContent = {
      connected: "Changes sync while connected",
      connecting: "Editing starts when the room connects.",
      disconnected: "Editing paused. Reload to reconnect.",
    }[status];
    view.setProps({ editable: () => provider.connected });
  };

  provider.on("status", renderStatus);
  provider.on("error", renderError);
  void provider.connect();

  return () => {
    name.removeEventListener("input", rename);
    awareness.off("change", renderPeople);
    provider.off("status", renderStatus);
    provider.off("error", renderError);
    provider.destroy();
    view.destroy();
    awareness.destroy();
    doc.destroy();
    toolbar.replaceChildren();
    list.replaceChildren();
  };
};

const roomUi = {
  landing: element("#landing"),
  workspace: element(".workspace"),
  roomLinkError: element("#room-link-error"),
  rawStream: element<HTMLAnchorElement>("#raw-stream"),
  rawStreamLink: element("#raw-stream-link"),
  manualCopy: element("#manual-copy"),
  roomLink: element<HTMLInputElement>("#room-link"),
  copyStatus: element("#copy-status"),
};

let activeRoom: { id: string; dispose: () => void } | undefined;

const closeRoom = () => {
  activeRoom?.dispose();
  activeRoom = undefined;
};

const renderRoute = () => {
  const roomId = roomIdFromHash(location.hash);

  if (roomId !== null && roomId === activeRoom?.id) return;

  closeRoom();
  roomUi.landing.hidden = roomId !== null;
  roomUi.workspace.hidden = roomId === null;
  statusBadge.hidden = roomId === null;
  roomUi.rawStreamLink.hidden = roomId === null;
  roomUi.manualCopy.hidden = true;
  roomUi.roomLink.value = "";
  roomUi.copyStatus.textContent = "";
  roomUi.roomLinkError.textContent =
    roomId === null && location.hash
      ? "This room link is invalid or outdated. Create a new room."
      : "";

  if (roomId === null) {
    roomUi.rawStream.removeAttribute("href");

    return;
  }

  roomUi.rawStream.href = `/v1/stream/yjs/editor/docs/${roomId}/.updates?offset=-1`;
  activeRoom = { id: roomId, dispose: openRoom(roomId) };
};

element("#create-room").addEventListener("click", () => {
  location.hash = nanoid();
});

// oxlint-disable-next-line effecttsgo/async-function -- Clipboard is a browser API.
const copyRoomLink = async () => {
  const room = activeRoom;

  const hash = location.hash;

  if (!room || roomIdFromHash(hash) !== room.id) return;

  const url = location.href;

  let copied = false;

  try {
    await navigator.clipboard.writeText(url);
    copied = true;
  } catch {
    // Clipboard access can be blocked or unavailable.
  }

  if (room !== activeRoom || hash !== location.hash) return;

  roomUi.manualCopy.hidden = copied;
  roomUi.copyStatus.textContent = copied
    ? "Room link copied."
    : "Select and copy the room link below.";

  if (!copied) {
    roomUi.roomLink.value = url;
    roomUi.roomLink.focus();
    roomUi.roomLink.select();
  }
};

element("#copy-room-link").addEventListener("click", () => void copyRoomLink());

window.addEventListener("hashchange", renderRoute);

window.addEventListener("pagehide", closeRoom);

window.addEventListener("pageshow", (event) => {
  if (event.persisted) renderRoute();
});

renderRoute();
