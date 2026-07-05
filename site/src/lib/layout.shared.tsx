import type { BaseLayoutProps } from 'fumadocs-ui/layouts/shared'
import { StreamsyLogo } from '#/components/streamsy-logo'

export function baseOptions(): BaseLayoutProps {
  return {
    nav: {
      title: <StreamsyLogo className="mx-auto h-15 w-auto" />,
      url: '/docs',
    },
  }
}
