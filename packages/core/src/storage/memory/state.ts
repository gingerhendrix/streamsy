import { MemoryStream } from "./stream.ts";

export type MemoryExpiryHandler = (streamId: string) => Promise<void> | void;

/** Shared in-memory stream registry. */
export class MemoryStreamState {
  private readonly streams = new Map<string, MemoryStream>();
  private readonly childEdges = new Map<string, Set<string>>();

  constructor(private readonly onScheduledExpiry?: MemoryExpiryHandler) {}

  getStream(id: string): MemoryStream {
    let stream = this.streams.get(id);
    if (!stream) {
      stream = new MemoryStream(
        id,
        () => this.deleteStream(id),
        () => this.onScheduledExpiry?.(id),
      );
      this.streams.set(id, stream);
    }
    return stream;
  }

  getExistingStream(id: string): MemoryStream | undefined {
    return this.streams.get(id);
  }

  addEdge(parent: string, child: string): void {
    const children = this.childEdges.get(parent) ?? new Set<string>();
    children.add(child);
    this.childEdges.set(parent, children);
  }

  dropEdge(parent: string, child: string): void {
    const children = this.childEdges.get(parent);
    if (!children) return;
    children.delete(child);
    if (children.size === 0) this.childEdges.delete(parent);
  }

  countDependents(parent: string): number {
    return this.childEdges.get(parent)?.size ?? 0;
  }

  private deleteStream(id: string): void {
    this.streams.delete(id);
    this.childEdges.delete(id);
  }
}
