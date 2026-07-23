import type { BaseLayoutProps } from "fumadocs-ui/layouts/shared";
import { StreamsyLogo } from "#/components/streamsy-logo";

export function baseOptions(): BaseLayoutProps {
  return {
    nav: {
      title: <StreamsyLogo className="mx-auto mt-2 h-12 w-auto" />,
      url: "/docs",
    },
    links: [
      { text: "Docs", url: "/docs", active: "nested-url" },
      { text: "Articles", url: "/articles", active: "nested-url" },
    ],
  };
}
