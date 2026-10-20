/**
 * True when `authorization` is `Bearer <token>`. The compare takes the same time
 * for every header of the same length. An empty token accepts nothing, so a
 * host with no writer token is read-only. Both hosts use this one check.
 */
export const bearerMatches = (authorization: string | null | undefined, token: string) => {
  if (token === "") return false;
  const expected = new TextEncoder().encode(`Bearer ${token}`);
  const supplied = new TextEncoder().encode(authorization ?? "");
  let difference = expected.length ^ supplied.length;

  for (const [index, byte] of expected.entries()) difference |= byte ^ (supplied[index] ?? 0);

  return difference === 0;
};
