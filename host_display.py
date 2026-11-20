"""Host display (optional, PHASE 5): a countdown on the server console.

This is the only use of asyncio in the project, and it stays at the level seen
in class: `async def`, `await`, `asyncio.sleep`, started with `asyncio.run`.
It is a small, isolated helper. The game is correct without it, and it never
touches the game state: it only prints.

ONE daemon thread runs for the whole game. The game side (barrier actions)
only updates a tiny shared state (is a round running? when does it end?), the
display thread reads it every tick and is the only one that writes on the
console line. So there is never a second writer to erase or overwrite the
countdown, and a daemon thread cannot keep the program alive.
"""

import asyncio
import threading
import time
from typing import Optional

TICK = 0.2     # seconds between two redraws


class HostDisplay:
    """Shows "[countdown] question 3/10: 12 s left" while a round is running.

    start() / close() are called by the server. start_round() / end_round() are
    called by the barrier actions, that is by a worker thread. A small lock
    protects the shared state. It is a leaf lock: it is never held while doing
    anything else (no print, no join).
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._label: Optional[str] = None        # None: no round in progress
        self._deadline = 0.0                     # time.monotonic() value
        self._closed = False
        self._thread: Optional[threading.Thread] = None

    # -------------------------------------------------------- server side
    def start(self) -> None:
        """Start the display thread (daemon)."""
        self._thread = threading.Thread(target=self._main, name="countdown",
                                        daemon=True)
        self._thread.start()

    def close(self) -> None:
        """Stop the display thread and wait for it: when this returns, the
        countdown line has been erased and nothing more will be printed."""
        with self._lock:
            self._closed = True
        if self._thread is not None:
            self._thread.join()                  # outside the lock
            self._thread = None

    # ---------------------------------------------------------- game side
    def start_round(self, number: int, total: int, seconds: float) -> None:
        with self._lock:
            self._label = f"[countdown] question {number}/{total}"
            self._deadline = time.monotonic() + seconds

    def end_round(self) -> None:
        with self._lock:
            self._label = None

    # ------------------------------------------------------ display thread
    def _main(self) -> None:
        """Body of the display thread."""
        asyncio.run(self._show())

    async def _show(self) -> None:
        """Redraw the countdown at every tick, erase it when the round is over."""
        showing = False
        while True:
            with self._lock:
                label, deadline, closed = self._label, self._deadline, self._closed
            if closed:
                break
            remaining = deadline - time.monotonic()
            if label is not None and remaining > 0:
                whole = int(remaining) + 1       # 14.2 s left is displayed as 15
                # Redrawn at every tick: if another output wiped the line, it comes back
                print(f"\r{label}: {whole:>2} s left   ", end="", flush=True)
                showing = True
            elif showing:
                self._erase()
                showing = False
            await asyncio.sleep(TICK)            # the only await: lets the clock advance
        if showing:
            self._erase()

    @staticmethod
    def _erase() -> None:
        print("\r" + " " * 45 + "\r", end="", flush=True)