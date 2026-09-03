import type { Database } from "bun:sqlite";

export const SQLITE_BUSY_RETRY_ATTEMPTS = 8;

export type ImmediateTransactionBody<Result> = () => Result;

interface SqliteError {
  readonly message: string;
  readonly code?: string | number;
}

export async function runImmediateTransactionWithBusyRetry<T>(
  db: Database,
  body: ImmediateTransactionBody<T>,
): Promise<T> {
  for (let attempt = 0; attempt < SQLITE_BUSY_RETRY_ATTEMPTS; attempt++) {
    try {
      const transaction = db.transaction(body);
      return transaction.immediate();
    } catch (error) {
      if (error instanceof Error && isSqliteBusy(error)) {
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

export function isSqliteBusy(error: SqliteError): boolean {
  const code = error.code === undefined ? "" : String(error.code);
  return code === "SQLITE_BUSY" || /SQLITE_BUSY|database is locked/i.test(error.message);
}
