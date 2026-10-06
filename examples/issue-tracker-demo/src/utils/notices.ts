import { Effect, Fiber } from "effect";
import { useCallback, useEffect, useRef, useState } from "react";

export interface Notice {
  readonly id: number;
  readonly message: string;
}

/** How long a notice stays on the page. */
const noticeMillis = 4000;

/** Short messages that remove themselves, for writes that the server refused. */
export function useNotices() {
  const [notices, setNotices] = useState<ReadonlyArray<Notice>>([]);
  const nextId = useRef(0);
  const timers = useRef(new Set<Fiber.Fiber<void>>());

  useEffect(() => {
    const pending = timers.current;

    return () => {
      for (const timer of pending) Effect.runFork(Fiber.interrupt(timer));
    };
  }, []);

  const notify = useCallback((message: string) => {
    const id = nextId.current++;
    setNotices((current) => [...current, { id, message }]);

    const timer = Effect.runFork(
      Effect.sleep(noticeMillis).pipe(
        Effect.andThen(
          Effect.sync(() => {
            timers.current.delete(timer);
            setNotices((current) => current.filter((notice) => notice.id !== id));
          }),
        ),
      ),
    );

    timers.current.add(timer);
  }, []);

  return { notices, notify };
}
