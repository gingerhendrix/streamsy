export function clone<T>(value: T): T {
  if (value === null || value === undefined) return value;
  return structuredClone(value);
}
