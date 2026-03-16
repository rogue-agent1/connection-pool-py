#!/usr/bin/env python3
"""Generic connection pool with health checks."""
import threading, time, queue, sys

class Connection:
    _counter = 0
    def __init__(self):
        Connection._counter += 1; self.id = Connection._counter
        self.created = time.time(); self.last_used = time.time(); self.alive = True
    def execute(self, query): self.last_used = time.time(); return f"[conn-{self.id}] {query}"
    def close(self): self.alive = False
    def is_healthy(self): return self.alive and (time.time() - self.last_used) < 300

class ConnectionPool:
    def __init__(self, min_size=2, max_size=10, timeout=5):
        self.pool = queue.Queue(maxsize=max_size)
        self.min = min_size; self.max = max_size; self.timeout = timeout
        self.total = 0; self.lock = threading.Lock()
        for _ in range(min_size): self._add_conn()
    def _add_conn(self):
        with self.lock:
            if self.total < self.max:
                self.pool.put(Connection()); self.total += 1; return True
        return False
    def acquire(self):
        try: conn = self.pool.get(timeout=self.timeout)
        except queue.Empty:
            if self._add_conn(): return self.acquire()
            raise RuntimeError("Pool exhausted")
        if not conn.is_healthy(): conn.close(); self.total -= 1; return self.acquire()
        return conn
    def release(self, conn):
        if conn.is_healthy(): self.pool.put(conn)
        else: conn.close(); self.total -= 1
    def stats(self):
        return {"total": self.total, "available": self.pool.qsize(), "in_use": self.total - self.pool.qsize()}

if __name__ == "__main__":
    pool = ConnectionPool(min_size=3, max_size=5)
    conns = [pool.acquire() for _ in range(3)]
    for c in conns: print(c.execute("SELECT 1"))
    print(f"Stats: {pool.stats()}")
    for c in conns: pool.release(c)
    print(f"After release: {pool.stats()}")
