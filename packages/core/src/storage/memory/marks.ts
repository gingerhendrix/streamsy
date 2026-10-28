import { Array, Option, Order } from "effect";
import type { PendingMark } from "../../internal/projection-marks.ts";

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
        ? { params: value, seq: 1, attempts: 0, dueAt: 0 }
        : { ...found, seq: found.seq + 1 },
    );
  }
};

export const nextDue = (marks: MarkTable, projection: string): Option.Option<number> => {
  let earliest: number | undefined;

  for (const row of marks.get(projection)?.values() ?? [])
    earliest = earliest === undefined ? row.dueAt : Math.min(earliest, row.dueAt);

  return Option.fromUndefinedOr(earliest);
};

export const due = (marks: MarkTable, projection: string, now: number, limit: number) =>
  Array.sort(
    Array.filter([...(marks.get(projection)?.values() ?? [])], (row) => row.dueAt <= now),
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

  if (rows?.get(params)?.seq === seq) rows.delete(params);
};

export const remove = (marks: MarkTable, projection: string, params: string) => {
  marks.get(projection)?.delete(params);
};

export const defer = (
  marks: MarkTable,
  projection: string,
  params: string,
  attempts: number,
  dueAt: number,
) => {
  const rows = rowsOf(marks, projection);
  const found = rows.get(params);
  rows.set(params, { params, seq: found?.seq ?? 1, attempts, dueAt });
};

export const deferred = (marks: MarkTable, projection: string) =>
  Array.filter([...(marks.get(projection)?.values() ?? [])], (row) => row.attempts > 0).length;
