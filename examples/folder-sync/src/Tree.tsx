import type { File } from "../server/domain.ts";
import type { Selection } from "./client.ts";

export type TreeFile = File & { folderId: string };

export interface Branch {
  name: string;
  children: Map<string, Branch>;
  file?: TreeFile;
}

export const fileTree = (files: ReadonlyArray<TreeFile>) => {
  const root: Branch = { name: "", children: new Map() };

  for (const file of files) {
    let node = root;

    // Folder ids are opaque. A slash in one is part of its label.
    for (const name of [file.folderId, ...file.path.split("/")]) {
      let child = node.children.get(name);

      if (child === undefined) {
        child = { name, children: new Map() };
        node.children.set(name, child);
      }

      node = child;
    }

    node.file = file;
  }

  return root;
};

export function Tree({
  branch,
  selection,
  open,
}: {
  branch: Branch;
  selection: Selection | null;
  open: (selection: Selection) => void;
}) {
  return (
    <ul className="tree">
      {[...branch.children.values()]
        .toSorted((a, b) => a.name.localeCompare(b.name))
        .map((child) => (
          <li key={child.name}>
            {child.file && (
              <button
                className={
                  selection?.folderId === child.file.folderId && selection.path === child.file.path
                    ? "selected"
                    : ""
                }
                onClick={() =>
                  child.file && open({ folderId: child.file.folderId, path: child.file.path })
                }
              >
                <span aria-hidden="true">▤</span> {child.name}
              </button>
            )}
            {child.children.size > 0 && (
              <details open>
                <summary>{child.name}/</summary>
                <Tree branch={child} selection={selection} open={open} />
              </details>
            )}
          </li>
        ))}
    </ul>
  );
}
