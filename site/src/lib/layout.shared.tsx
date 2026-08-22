import type { BaseLayoutProps } from "fumadocs-ui/layouts/shared";
import { StreamsyLogo } from "#/components/streamsy-logo";
import { withBasePath } from "#/lib/base-path";

export function baseOptions(): BaseLayoutProps {
  return {
    nav: {
      title: <StreamsyLogo className="mx-auto mt-2 h-12 w-auto" />,
      url: withBasePath("/docs"),
    },
    links: [
      { text: "Docs", url: withBasePath("/docs"), active: "nested-url" },
      { text: "Articles", url: withBasePath("/articles"), active: "nested-url" },
    ],
  };
}
