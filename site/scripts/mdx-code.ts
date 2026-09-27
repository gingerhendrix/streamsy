import { fromMarkdown } from "mdast-util-from-markdown";
import { mdxFromMarkdown } from "mdast-util-mdx";
import { mdxjs } from "micromark-extension-mdxjs";
import type { Code, Nodes } from "mdast";

/** One fenced code block as MDX renders it. */
export interface CodeFence {
  readonly lang: string | null;
  readonly meta: string | null;
  readonly value: string;
  /** 1-based line of the opening fence in the page. */
  readonly line: number;
}

const visit = (node: Nodes, found: Array<Code>): void => {
  if (node.type === "code") found.push(node);
  if ("children" in node) for (const child of node.children) visit(child, found);
};

/** Every fenced code block in an MDX page, in document order, nested blocks included. */
export const codeFences = (text: string): ReadonlyArray<CodeFence> => {
  const tree = fromMarkdown(text, {
    extensions: [mdxjs()],
    mdastExtensions: [mdxFromMarkdown()],
  });
  const found: Array<Code> = [];
  visit(tree, found);
  return found.map((node) => ({
    lang: node.lang ?? null,
    meta: node.meta ?? null,
    value: node.value,
    line: node.position?.start.line ?? 0,
  }));
};

/** The `title="..."` value from a fence meta string, if there is one. */
export const fenceTitle = (meta: string | null): string | undefined =>
  meta === null ? undefined : /(?:^|\s)title="([^"]*)"/.exec(meta)?.[1];
