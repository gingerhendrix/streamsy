import { MemoryStream } from "./stream.ts";

/** Shared in-memory stream registry. */
export class MemoryStreamState {
  private readonly streams = new Map<string, MemoryStream>();

  getStream(id: string): MemoryStream {
    let stream = this.streams.get(id);
    if (!stream) {
      stream = new MemoryStream(id, () => this.streams.delete(id));
      this.streams.set(id, stream);
    }
    return stream;
  }

  stream(id: string): MemoryStream {
    return this.getStream(id);
  }
}
