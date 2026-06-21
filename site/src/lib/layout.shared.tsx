import type { BaseLayoutProps } from 'fumadocs-ui/layouts/shared'

export function baseOptions(): BaseLayoutProps {
  return {
    nav: {
      title: (
        <span className="font-logotype text-[44px] font-medium leading-none tracking-wide normal-case">
          streamsy
        </span>
      ),
      url: '/docs',
    },
  }
}
