import { expect, test } from "bun:test";
import { renderToStaticMarkup } from "react-dom/server";
import ReactMarkdown from "react-markdown";
import { MarkdownLink } from "../../src/MarkdownLink.tsx";
import { Snippet } from "../../src/Snippet.tsx";
import { fileTree } from "../../src/Tree.tsx";
import { readableText, fileUrl } from "../../src/client.ts";

test("search snippet content is text even inside highlighted matches", () => {
  const html = renderToStaticMarkup(
    <Snippet
      parts={[
        { text: '<img src=x onerror="alert(1)">', match: false },
        { text: "<script>match</script>", match: true },
      ]}
    />,
  );

  expect(html).not.toContain("<img");
  expect(html).not.toContain("<script");
  expect(html).toContain("<mark>&lt;script&gt;match&lt;/script&gt;</mark>");
});

test("file tree keeps opaque folder ids and handles file-directory name collisions", () => {
  const row = {
    id: "a",
    folderId: "docs/雪",
    version: "v1",
    hash: "a",
    size: 1,
    updatedAt: "now",
    author: "test",
  };

  const root = fileTree([
    { ...row, path: "a" },
    { ...row, id: "b", path: "a/b.md" },
  ]);

  const folder = root.children.get("docs/雪");
  expect(folder?.children.get("a")?.file?.path).toBe("a");
  expect(folder?.children.get("a")?.children.get("b.md")?.file?.path).toBe("a/b.md");
});

test("text decoding preserves BOM and detects binary; file URLs encode IDs and path segments", () => {
  expect(readableText(new Uint8Array([239, 187, 191, 97]))).toBe("\ufeffa");
  expect(readableText(new Uint8Array([255]))).toBeNull();
  expect(readableText(new Uint8Array([0]))).toBeNull();
  expect(fileUrl({ folderId: "docs/雪", path: "a/100%.md", at: "v1" })).toBe(
    "/api/folders/docs%2F%E9%9B%AA/files/a/100%25.md?at=v1",
  );
});

test("Markdown links cannot navigate away from a draft", () => {
  const html = renderToStaticMarkup(
    <ReactMarkdown skipHtml components={{ a: MarkdownLink }}>
      {
        "[unsafe](javascript:alert) [data](data:text/html,x) [empty]() [relative](/docs/a) [external](https://example.com/docs)"
      }
    </ReactMarkdown>,
  );

  expect(html.match(/<a /g)).toHaveLength(1);
  expect(html).toContain('href="https://example.com/docs"');
  expect(html).toContain('target="_blank"');
  expect(html).toContain('rel="noopener noreferrer"');
  expect(html).toContain("unsafe data empty relative");
});
