import { useEffect, useRef, useState } from "react";
import ReactMarkdown from "react-markdown";
import { MarkdownLink } from "./MarkdownLink.tsx";
import { draftReads } from "./draft-reads.ts";
import { fileUrl, history, readFile, saveFile } from "./client.ts";
import type { History, OpenFile, Selection } from "./client.ts";

export function FilePanel({
  selection,
  headVersion,
  open,
}: {
  selection: Selection;
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
  const dirty = file !== null && file.text !== null && file.text !== draft;
  const reads = useRef(draftReads());
  const retry = useRef<{ text: string; base: number; author: string; opId: string } | null>(null);

  useEffect(() => {
    const controller = new AbortController();
    void history(selection, controller.signal)
      .then(setRows)
      .catch((error: Error) => {
        if (!controller.signal.aborted) setMessage(error.message);
      });

    return () => controller.abort();
  }, [selection, headVersion]);

  useEffect(() => {
    if (dirty || saving) return undefined;
    const applyRead = reads.current.begin();
    const controller = new AbortController();
    void readFile(selection, controller.signal)
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
    // A new manifest version refreshes a clean view. Draft edits retain the version they opened.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selection, headVersion, saving]);

  const save = () => {
    if (!file || file.text === null || saving) return;
    let attempt = retry.current;

    if (
      !attempt ||
      attempt.text !== draft ||
      attempt.base !== file.version ||
      attempt.author !== author
    ) {
      // oxlint-disable-next-line effecttsgo/crypto-random-uuid -- Browser write boundary creates a durable replay key for this attempted save.
      attempt = { text: draft, base: file.version, author, opId: crypto.randomUUID() };
      retry.current = attempt;
    }

    const sent = attempt;
    setSaving(true);
    setMessage("");
    void saveFile(selection, sent.text, sent.base, sent.author, sent.opId)
      .then((receipt) => {
        // The accepted receipt becomes the base of the next save, before the manifest catches up.
        setFile({
          text: sent.text,
          size: new TextEncoder().encode(sent.text).length,
          version: receipt.version,
        });
        retry.current = null;
        setMessage(receipt.overwrote ? "Saved; replaced a change you did not see" : "Saved");

        return history(selection).then(setRows);
      })
      .catch((error: Error) => setMessage(error.message))
      .finally(() => setSaving(false));
  };

  const selectedOperation = rows.find(
    (row) => row.version === (selection.at ?? file?.version),
  )?.operation;

  const download =
    selectedOperation?.type === "put" && "blob" in selectedOperation
      ? `/api/blobs/${selectedOperation.blob}`
      : fileUrl(selection);

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
          The server has a newer version. Your draft keeps its original base.
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
          File history <span>{rows.length} versions</span>
        </h3>
        {rows.toReversed().map(({ version, operation }) => (
          <div key={version} className="history-entry">
            <button onClick={() => open({ ...selection, at: version })}>{version}</button>
            <span>
              {operation.type === "delete" ? "Deleted" : "Written"} by {operation.author} ·{" "}
              {operation.updatedAt ?? "earlier log"}
            </span>
            {operation.overwrote && (
              <p>
                replaced a change you did not see ·{" "}
                <button onClick={() => open({ ...selection, at: operation.overwrote })}>
                  View replaced version
                </button>
              </p>
            )}
          </div>
        ))}
      </aside>
    </section>
  );
}
