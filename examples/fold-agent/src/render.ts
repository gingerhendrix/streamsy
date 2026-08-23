/** Human-readable rendering of durable Fold entries, shared by the CLI commands. */
import type { LogEntry } from "@humanlayer/fold-core";

const preview = (text: string, limit = 110): string => {
  const flat = text.replace(/\s+/g, " ").trim();
  return flat.length <= limit ? flat : `${flat.slice(0, limit - 1)}…`;
};

/**
 * Effect AI encodes a message whose content is plain text as a bare string, and
 * anything richer as an array of parts. Normalize both to a rendered string.
 */
const renderContent = <Part extends { readonly type: string }>(
  content: string | ReadonlyArray<Part>,
  part: (value: Part) => string,
): string => (typeof content === "string" ? content : content.map(part).join(" "));

const detail = (entry: LogEntry): string => {
  switch (entry._tag) {
    case "session_started":
      return `session=${entry.sessionId} root=${entry.rootAgentId}`;
    case "user-message":
      return renderContent(entry.message.content, (part) =>
        part.type === "text" ? part.text : `<${part.type}>`,
      );
    case "assistant-message":
      return renderContent(entry.message.content, (part) =>
        part.type === "text"
          ? part.text
          : part.type === "tool-call"
            ? `calls ${part.name}(${JSON.stringify(part.params)})`
            : `<${part.type}>`,
      );
    case "tool-result":
      return renderContent(entry.message.content, (part) =>
        part.type === "tool-result"
          ? `${part.name} -> ${JSON.stringify(part.result)}`
          : `<${part.type}>`,
      );
    case "agent-finished":
      return `${entry.outcome}${entry.resultText === null ? "" : `: ${entry.resultText}`}`;
    case "model-change":
      return `${entry.model.providerId}/${entry.model.modelId}`;
    default:
      return "";
  }
};

/** A one-line summary of one durable entry: its sequence, tag, and a short gist. */
export const formatEntry = (entry: LogEntry): string =>
  `${String(entry.seq).padStart(3, " ")}  ${entry._tag.padEnd(20, " ")} ${preview(detail(entry))}`;
