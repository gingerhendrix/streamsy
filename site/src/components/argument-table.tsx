'use client'

import type { ComponentProps } from 'react'
import { TypeTable } from 'fumadocs-ui/components/type-table'

type TypeTableProps = ComponentProps<typeof TypeTable>

/** A TypeTable with terminology appropriate for function parameters. */
export function ArgumentTable(props: TypeTableProps) {
  return (
    <div className="[&>div>div:first-child>p:first-child]:before:content-['Argument'] [&>div>div:first-child>p:first-child]:text-[0] [&>div>div:first-child>p:first-child]:before:text-sm">
      <TypeTable {...props} />
    </div>
  )
}
