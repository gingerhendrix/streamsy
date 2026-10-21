import { Array } from "effect";
import { useEffect, useRef, useState } from "react";
import ReactMarkdown from "react-markdown";
import { MarkdownLink } from "./MarkdownLink.tsx";
import { draftReads } from "./draft-reads.ts";
import { isLive, isUpsert } from "../server/messages.ts";
import { fileUrl } from "../server/routes.ts";
import { historyReader, readFile } from "./client.ts";
import type { History, OpenFile, Selection } from "./client.ts";
import { editFile, type FileDb } from "./db.ts";

export function FilePanel({
  selection,
  db,
  headVersion,
  open,
}: {
  selection: Selection;
  db: FileDb;
  headVersion: number | undefined;
  open: (selection: Selection) => void;
}) {
  const [file, setFile] = useState<OpenFile | null>(null);
  const [rows, setRows] = useState<History>([]);
  const [draft, setDraft] = useState("");
  const [editing, setEditing] = useState(false);
  const [author, setAuthor] = useState("browser writer");
  const [saving, setSaving] = useState(false);
  const [message, setMessage] = useState("");
  const [readError, setReadError] = useState("");
  const [reload, setReload] = useState(0);
  const dirty = file !== null && file.text !== null && file.text !== draft;
  const reads = useRef(draftReads());
  const historyReads = useRef(historyReader(setRows));
  const retry = useRef<{ text: string; base: number; author: string; txid: string } | null>(null);

  useEffect(() => {
    const reader = historyReads.current;
    void reader.refresh(selection).catch((error: Error) => setMessage(error.message));

    return () => reader.cancel();
  }, [selection, headVersion]);

  useEffect(() => {
    if (dirty || saving) return undefined;
    const applyRead = reads.current.begin();
    const controller = new AbortController();
    void readFile(selection, db.collections.files.get(selection.path), controller.signal)
      .then((loaded) => {
        if (controller.signal.aborted) return;
        applyRead(() => {
          setReadError("");
          setFile(loaded);
          setDraft(loaded.text ?? "");
        });
      })
      .catch((error: Error) => {
        if (!controller.signal.aborted) {
          applyRead(() => {
            setReadError(error.message);
            setFile(null);
          });
        }
      });

    return () => controller.abort();
    // A new file version refreshes a clean view. Draft edits retain the version they opened.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selection, headVersion, saving, reload]);

  const save = () => {
    if (!file || file.text === null || saving) return;
    let attempt = retry.current;

    if (
      !attempt ||
      attempt.text !== draft ||
      attempt.base !== file.version ||
      attempt.author !== author
    ) {
      // oxlint-disable-next-line effecttsgo/crypto-random-uuid -- Browser write boundary creates a transaction id for this attempted save.
      attempt = { text: draft, base: file.version, author, txid: crypto.randomUUID() };
      retry.current = attempt;
    }

    const sent = attempt;
    setSaving(true);
    setMessage("");
    void editFile(
      db,
      { path: selection.path, version: sent.base },
      sent.text,
      sent.author,
      sent.txid,
    )
      .then((accepted) => {
        setFile({
          text: sent.text,
          size: accepted.size,
          hash: accepted.hash,
          version: accepted.version,
        });
        retry.current = null;
        setMessage("Saved");

        return historyReads.current.refresh(selection);
      })
      .catch((error: Error) => setMessage(error.message))
      .finally(() => setSaving(false));
  };

  const download =
    selection.at === undefined || file === null
      ? fileUrl(selection.folderId, selection.path)
      : `/api/blobs/${file.hash}`;

  const markdown = /\.mdx?$/i.test(selection.path);

  return (
    <section className="file-panel">
      <div className="file-heading">
        <div>
          <p className="eyebrow">{selection.folderId}</p>
          <h2>{selection.path}</h2>
        </div>
        <a href={download} download={selection.path.split("/").at(-1)}>
          Download raw
        </a>
      </div>
      <div className="toolbar">
        <span className="version">
          {selection.at ? "Historical version" : "Current version"}{" "}
          {file?.version ?? selection.at ?? "…"}
        </span>
        {selection.at && (
          <button onClick={() => open({ folderId: selection.folderId, path: selection.path })}>
            Open current
          </button>
        )}
        {!selection.at && file?.text !== null && file && (
          <button onClick={() => setEditing(!editing)} disabled={saving}>
            {editing ? "Preview" : "Edit"}
          </button>
        )}
      </div>
      {message && (
        <p role="status" className="notice">
          {message}
        </p>
      )}
      {dirty && headVersion && file?.version !== headVersion && (
        <p className="notice">
          The server has a newer version. A save makes your draft the head, and the other version
          stays in history.
        </p>
      )}
      {!file ? (
        <p>{readError || "Loading file…"}</p>
      ) : file.text === null ? (
        <p className="empty">
          Binary file · {file.size.toLocaleString()} bytes. Use the raw download.
        </p>
      ) : editing ? (
        <div className="editor">
          <textarea
            aria-label="File text"
            value={draft}
            disabled={saving}
            onChange={(event) => {
              reads.current.edited();
              setDraft(event.target.value);
            }}
          />
          <div className="save-bar">
            <label>
              Author{" "}
              <input
                aria-label="Author"
                value={author}
                disabled={saving}
                onChange={(event) => setAuthor(event.target.value)}
              />
            </label>
            <button className="primary" disabled={saving || !dirty} onClick={save}>
              {saving ? "Saving…" : "Save"}
            </button>
            {dirty && (
              <button
                disabled={saving}
                onClick={() => {
                  reads.current.edited();
                  retry.current = null;
                  setFile(null);
                  setDraft("");
                  setMessage("");
                  setReload((value) => value + 1);
                }}
              >
                Discard draft and reload
              </button>
            )}
          </div>
        </div>
      ) : markdown ? (
        <article className="markdown">
          <ReactMarkdown skipHtml components={{ a: MarkdownLink }}>
            {draft}
          </ReactMarkdown>
        </article>
      ) : (
        <pre className="text-view">{draft}</pre>
      )}
      <aside className="history">
        <h3>
          File history <span>{rows.length} changes</span>
        </h3>
        {Array.map(Array.reverse(rows), (change, index) => (
          <div key={`${change.headers.txid ?? ""}:${index}`} className="history-entry">
            {isLive(change) ? (
              <>
                <button onClick={() => open({ ...selection, at: change.value.version })}>
                  {change.value.version}
                </button>
                <span>
                  Written by {change.value.author} · {parentLabel(change.value.parent)} ·{" "}
                  {change.headers.timestamp ?? "earlier log"}
                </span>
              </>
            ) : (
              <span>
                Deleted
                {isUpsert(change) &&
                  ` as ${change.value.version} by ${change.value.author} · ${parentLabel(change.value.parent)}`}{" "}
                · {change.headers.timestamp ?? "earlier log"}
              </span>
            )}
          </div>
        ))}
      </aside>
    </section>
  );
}

/** The version that a change started from. A fork shows as two changes with one parent. */
const parentLabel = (parent: number | null) => (parent === null ? "new file" : `from ${parent}`);
