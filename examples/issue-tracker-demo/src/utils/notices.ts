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
  const timers = useRef(new Set<ReturnType<typeof setTimeout>>());

  useEffect(() => {
    const pending = timers.current;
    return () => {
      for (const timer of pending) clearTimeout(timer);
    };
  }, []);

  const notify = useCallback((message: string) => {
    const id = nextId.current++;
    setNotices((current) => [...current, { id, message }]);
    const timer = setTimeout(() => {
      timers.current.delete(timer);
      setNotices((current) => current.filter((notice) => notice.id !== id));
    }, noticeMillis);
    timers.current.add(timer);
  }, []);

  return { notices, notify };
}
