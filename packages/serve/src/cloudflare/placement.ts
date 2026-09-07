export interface Placement {
  readonly name: (streamPath: string) => string;
}

export const Placement = {
  byStream: (): Placement => ({ name: (streamPath) => streamPath }),
  byKey: (key: (streamPath: string) => string): Placement => ({ name: key }),
} as const;
