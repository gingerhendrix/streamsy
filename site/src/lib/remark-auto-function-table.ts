import path from 'node:path'
import { createProject } from 'fumadocs-typescript'

type MdxAttribute = {
  type: 'mdxJsxAttribute'
  name: string
  value?: string | null
}

type MdxNode = {
  type?: string
  name?: string
  attributes?: MdxAttribute[]
  children?: MdxNode[]
}

function attribute(node: MdxNode, name: string): string | undefined {
  const value = node.attributes?.find((item) => item.name === name)?.value
  return typeof value === 'string' ? value : undefined
}

function setAttribute(node: MdxNode, name: string, value: string) {
  const existing = node.attributes?.find((item) => item.name === name)
  if (existing) existing.value = value
  else (node.attributes ??= []).push({ type: 'mdxJsxAttribute', name, value })
}

function walk(node: MdxNode, visit: (node: MdxNode) => void) {
  visit(node)
  node.children?.forEach((child) => walk(child, visit))
}

/**
 * Converts a function export into a synthetic interface containing its named
 * parameters. The standard Fumadocs generator can then document those
 * parameters without treating the function itself as an object type.
 */
export function remarkAutoFunctionTable() {
  return async (tree: MdxNode, file: { dirname?: string }) => {
    const nodes: MdxNode[] = []
    walk(tree, (node) => {
      if (node.name === 'auto-function-table') nodes.push(node)
    })
    if (nodes.length === 0) return

    const project = await createProject()

    for (const node of nodes) {
      const sourcePath = attribute(node, 'path')
      const exportName = attribute(node, 'name')
      if (!sourcePath || !exportName) {
        throw new Error('<auto-function-table> requires path and name attributes')
      }

      const absolutePath = path.resolve(file.dirname ?? process.cwd(), sourcePath)
      const sourceFile = project.addSourceFileAtPathIfExists(absolutePath)
      const declaration = sourceFile?.getExportedDeclarations().get(exportName)?.[0]
      const signature = declaration?.getType().getCallSignatures()[0]
      if (!declaration || !signature) {
        throw new Error(`${exportName} in ${absolutePath} is not a function export`)
      }

      const interfaceName = `$${exportName}Arguments`
      const fields = signature.getParameters().map((parameter) => {
        const optional = parameter.isOptional()
        const type = parameter.getTypeAtLocation(declaration).getText(declaration)
        return `  ${JSON.stringify(parameter.getName())}${optional ? '?' : ''}: ${type};`
      })

      const syntheticType = `\nexport interface ${interfaceName} {\n${fields.join('\n')}\n}`
      node.name = 'auto-argument-table'
      setAttribute(node, 'name', interfaceName)
      setAttribute(node, 'type', syntheticType)
    }
  }
}
