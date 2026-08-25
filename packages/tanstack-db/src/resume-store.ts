export interface StateSinkResume {
  readonly offset: string;
  readonly protocolVersion: number;
  readonly contractFingerprint: string;
}

export interface ResumeStore {
  readonly load: () => Promise<StateSinkResume | undefined>;
  readonly save: (resume: StateSinkResume) => Promise<void>;
  readonly clear: () => Promise<void>;
}

export function memoryResumeStore(initial?: StateSinkResume): ResumeStore {
  let current = initial;
  return {
    load: () => Promise.resolve(current),
    save: (resume) => {
      current = resume;
      return Promise.resolve();
    },
    clear: () => {
      current = undefined;
      return Promise.resolve();
    },
  };
}
