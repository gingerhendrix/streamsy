/** A refresh owns the draft only until the next keystroke, even if the user later undoes it. */
export function draftReads() {
  let revision = 0;

  return {
    edited: () => revision++,
    begin: () => {
      const started = revision;

      return (apply: () => void) => {
        if (revision !== started) return;
        apply();
      };
    },
  };
}
