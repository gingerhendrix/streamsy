import type { ComponentProps } from "react";

/** Relative documentation links stay as text; external links cannot replace the editor. */
export function MarkdownLink({ href, children }: Pick<ComponentProps<"a">, "href" | "children">) {
  let safe = false;

  if (href) {
    try {
      safe = ["http:", "https:", "mailto:"].includes(new URL(href).protocol);
    } catch {
      // Empty, relative, and malformed URLs have no navigation target in this viewer.
    }
  }

  return safe ? (
    <a href={href} target="_blank" rel="noopener noreferrer">
      {children}
    </a>
  ) : (
    <>{children}</>
  );
}
