import { Array, Effect, Option, Order } from "effect";
import type {
  PendingMark,
  ProjectionMarkSummary,
  ProjectionMarksApi,
} from "../../internal/projection-marks.ts";

/** Projection id to params to mark. Rows are replaced, never changed in place. */
export type MarkTable = Map<string, Map<string, PendingMark>>;

/** A private transaction draft of the marks. */
export const copyMarks = (marks: MarkTable): MarkTable =>
  new Map(Array.map([...marks], ([projection, rows]) => [projection, new Map(rows)]));

const rowsOf = (marks: MarkTable, projection: string) => {
  const found = marks.get(projection);

  if (found !== undefined) return found;
  const rows = new Map<string, PendingMark>();
  marks.set(projection, rows);

  return rows;
};

const byDue = Order.mapInput(
  Order.Tuple([Order.Number, Order.String]),
  (mark: PendingMark) => [mark.dueAt, mark.params] as const,
);

export const mark = (marks: MarkTable, projection: string, params: ReadonlyArray<string>) => {
  const rows = rowsOf(marks, projection);

  for (const value of params) {
    const found = rows.get(value);
    rows.set(
      value,
      found === undefined
        ? { params: value, seq: 1, attempts: 0, dueAt: 0, stopped: false, failure: Option.none() }
        : { ...found, seq: found.seq + 1 },
    );
  }
};

export const nextDue = (marks: MarkTable, projection: string): Option.Option<number> => {
  let earliest: number | undefined;

  for (const row of marks.get(projection)?.values() ?? []) {
    if (!row.stopped) earliest = earliest === undefined ? row.dueAt : Math.min(earliest, row.dueAt);
  }

  return Option.fromUndefinedOr(earliest);
};

export const due = (marks: MarkTable, projection: string, now: number, limit: number) =>
  Array.sort(
    Array.filter(
      [...(marks.get(projection)?.values() ?? [])],
      (row) => !row.stopped && row.dueAt <= now,
    ),
    byDue,
  ).slice(0, limit);

export const read = (marks: MarkTable, projection: string, params: ReadonlyArray<string>) => {
  const rows = marks.get(projection);
  const found = new Map<string, PendingMark>();

  for (const value of params) {
    const row = rows?.get(value);

    if (row !== undefined) found.set(value, row);
  }

  return found;
};

export const clear = (marks: MarkTable, projection: string, params: string, seq: number) => {
  const rows = marks.get(projection);

  const row = rows?.get(params);

  if (rows === undefined || row === undefined) return;

  if (row.seq === seq) rows.delete(params);
  else rows.set(params, { ...row, attempts: 0, dueAt: 0, stopped: false, failure: Option.none() });
};

export const remove = (marks: MarkTable, projection: string, params: string) => {
  marks.get(projection)?.delete(params);
};

export const fail = (
  marks: MarkTable,
  projection: string,
  params: string,
  options: Parameters<ProjectionMarksApi["fail"]>[2],
) => {
  const rows = rowsOf(marks, projection);
  const found = rows.get(params);
  rows.set(params, {
    params,
    seq: found?.seq ?? 1,
    attempts: options.attempts,
    dueAt: Option.getOrElse(options.dueAt, () => 0),
    stopped: Option.isNone(options.dueAt),
    failure: Option.some({ ...options.failure, message: options.failure.message.slice(0, 1_000) }),
  });
};

export const failed = (marks: MarkTable, projection: string, limit: number) =>
  Array.sort(
    Array.filter([...(marks.get(projection)?.values() ?? [])], (row) => row.attempts > 0),
    Order.combine(
      Order.mapInput(Order.Boolean, (row: PendingMark) => !row.stopped),
      byDue,
    ),
  ).slice(0, limit);

export const resume = (marks: MarkTable, projection: string) => {
  let changed = 0;
  const rows = marks.get(projection);

  if (rows === undefined) return 0;

  for (const [key, row] of rows) {
    if (!row.stopped) continue;
    rows.set(key, { ...row, dueAt: 0, stopped: false });
    changed += 1;
  }

  return changed;
};

export const retry = (marks: MarkTable, projection: string, params?: string) => {
  let changed = 0;
  const rows = marks.get(projection);

  if (rows === undefined) return 0;

  for (const [key, row] of rows) {
    if (
      (!row.stopped && (row.attempts === 0 || row.dueAt === 0)) ||
      (params !== undefined && key !== params)
    )
      continue;
    rows.set(key, { ...row, dueAt: 0, stopped: false });
    changed += 1;
  }

  return changed;
};

/** Classify each row once; a failed row at `now` is due, not backing off. */
export const summary = (
  marks: MarkTable,
  projection: string,
  now: number,
): ProjectionMarkSummary => {
  let fresh = 0;
  let retryDue = 0;
  let backingOff = 0;
  let stopped = 0;
  let earliest: number | undefined;

  for (const row of marks.get(projection)?.values() ?? []) {
    if (row.stopped) {
      stopped += 1;
      continue;
    }

    if (row.attempts === 0) fresh += 1;
    else if (row.dueAt <= now) retryDue += 1;
    else backingOff += 1;
    earliest = earliest === undefined ? row.dueAt : Math.min(earliest, row.dueAt);
  }

  return { fresh, retryDue, backingOff, stopped, nextDueAt: Option.fromUndefinedOr(earliest) };
};

/** A new private marks store. Each operation runs synchronously when its Effect executes. */
export const memoryProjectionMarks = (): ProjectionMarksApi => {
  const table: MarkTable = new Map();

  return {
    mark: (projection, params) => Effect.sync(() => mark(table, projection, params)),
    nextDue: (projection) => Effect.sync(() => nextDue(table, projection)),
    summary: (projection, now) => Effect.sync(() => summary(table, projection, now)),
    due: (projection, now, limit) => Effect.sync(() => due(table, projection, now, limit)),
    read: (projection, params) => Effect.sync(() => read(table, projection, params)),
    clear: (projection, params, seq) => Effect.sync(() => clear(table, projection, params, seq)),
    remove: (projection, params) => Effect.sync(() => remove(table, projection, params)),
    fail: (projection, params, options) =>
      Effect.sync(() => fail(table, projection, params, options)),
    retry: (projection, params) => Effect.sync(() => retry(table, projection, params)),
    resume: (projection) => Effect.sync(() => resume(table, projection)),
    failed: (projection, limit) => Effect.sync(() => failed(table, projection, limit)),
  };
};
