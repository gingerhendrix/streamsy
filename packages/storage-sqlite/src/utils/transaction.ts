import type { Database } from "bun:sqlite";

export const SQLITE_BUSY_RETRY_ATTEMPTS = 8;

export async function runImmediateTransactionWithBusyRetry<T>(
  db: Database,
  body: () => T,
): Promise<T> {
  for (let attempt = 0; attempt < SQLITE_BUSY_RETRY_ATTEMPTS; attempt++) {
    try {
      const transaction = db.transaction(body);
      return transaction.immediate();
    } catch (error) {
      if (isSqliteBusy(error)) {
        await Promise.resolve();
        continue;
      }
      throw error;
    }
  }
  throw new SqliteBusyRetryExhausted();
}

export class SqliteBusyRetryExhausted extends Error {
  constructor() {
    super("SQLite busy retry attempts exhausted");
    this.name = "SqliteBusyRetryExhausted";
  }
}

export function isSqliteBusy(error: unknown): boolean {
  if (!(error instanceof Error)) return false;
  const code = "code" in error ? String(error.code) : "";
  return code === "SQLITE_BUSY" || /SQLITE_BUSY|database is locked/i.test(error.message);
}
