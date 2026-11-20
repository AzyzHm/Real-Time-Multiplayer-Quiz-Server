"""Logger and leaderboard process (PHASE 4).

Producer/consumer pattern seen in class (multiprocessing.Queue):
  - producers : the threads of the server process (workers, barrier actions)
                put small events (dicts) in an mp.Queue
  - consumer  : this process reads the events, writes game.log, prints the live
                ranking after each question and the final statistics
  - end       : the sentinel None (as in the course example) stops the consumer

Why a separate process? The work is light, so the reason is NOT speed. It is
isolation and IPC: file writing and statistics stay off the game threads, and
the only way to talk to this process is the queue (processes do not share
memory). Everything put in the queue must be picklable: events only contain
str, int, float, bool and lists.

Events are plain dicts with a "type" key, for example
  {"type": "answer", "player": "Sana", "q": 3, "correct": True,
   "points": 150, "first": True, "t": 4.2}
Types: join, leave, game_start, answer, timeout, disconnect, round_end, game_over.
"""

import multiprocessing as mp
import queue
import signal
import sys
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

PARENT_CHECK = 1.0     # seconds between two checks that the server is still alive
STOP_TIMEOUT = 5.0     # seconds to wait for the process to end after the sentinel


# ---------------------------------------------------------------- statistics
@dataclass
class PlayerStats:
    answers: int = 0
    correct: int = 0
    firsts: int = 0                 # "first correct answer" bonuses
    timeouts: int = 0
    disconnected: bool = False
    times: List[float] = field(default_factory=list)          # all answers
    correct_times: List[float] = field(default_factory=list)  # correct answers only


class GameStats:
    """Accumulates the events of one game and builds the final report.

    A plain class (no process inside), so it can be tested on its own.
    """

    def __init__(self) -> None:
        self.players: Dict[str, PlayerStats] = {}
        self.questions: Dict[int, List[int]] = {}   # question id -> [asked, correct]
        self.ranking: List[list] = []
        self.has_game = False

    def _player(self, name: str) -> PlayerStats:
        return self.players.setdefault(name, PlayerStats())

    def add(self, event: dict) -> None:
        kind = event.get("type")
        if kind == "game_start":
            self.has_game = True
        elif kind == "answer":
            stats = self._player(event["player"])
            stats.answers += 1
            stats.times.append(event["t"])
            asked = self.questions.setdefault(event["q"], [0, 0])
            asked[0] += 1
            if event["correct"]:
                stats.correct += 1
                stats.correct_times.append(event["t"])
                asked[1] += 1
            if event["first"]:
                stats.firsts += 1
        elif kind == "timeout":
            self._player(event["player"]).timeouts += 1
            self.questions.setdefault(event["q"], [0, 0])[0] += 1
        elif kind == "disconnect":
            self._player(event["player"]).disconnected = True
        elif kind in ("round_end", "game_over"):
            self.ranking = event["ranking"]

    def report(self) -> List[str]:
        """The final statistics, as lines of text."""
        lines = ["=== Final statistics ===", "Final ranking:"]
        for position, (name, score) in enumerate(self.ranking, start=1):
            lines.append(f"  {position}. {name}: {score}")

        ranked = [name for name, _ in self.ranking]
        names = ranked + [n for n in self.players if n not in ranked]
        lines.append("Per player:")
        for name in names:
            s = self.players.get(name, PlayerStats())
            rate = 100 * s.correct / s.answers if s.answers else 0
            average = f"{sum(s.times) / len(s.times):.2f} s" if s.times else "n/a"
            fastest = f"{min(s.correct_times):.2f} s" if s.correct_times else "n/a"
            left = ", disconnected" if s.disconnected else ""
            lines.append(f"  {name}: {s.answers} answers, {s.correct} correct "
                         f"({rate:.0f}%), average response {average}, fastest "
                         f"correct {fastest}, {s.firsts} first bonus(es), "
                         f"{s.timeouts} timeout(s){left}")

        asked_questions = {q: v for q, v in self.questions.items() if v[0] > 0}
        if asked_questions:
            hardest = min(asked_questions, key=lambda q: asked_questions[q][1] / asked_questions[q][0])
            asked, correct = asked_questions[hardest]
            lines.append(f"Hardest question: id {hardest} ({correct}/{asked} correct)")
        return lines


def console_line(text: str) -> None:
    """Print one full line on the console.

    The host console may be showing a countdown (host_display.py) on the
    current line. On a real terminal, wipe that line first so the two outputs
    do not mix. The countdown redraws itself a moment later.
    """
    wipe = "\r" + " " * 79 + "\r" if sys.stdout.isatty() else ""
    print(wipe + text, flush=True)


# ---------------------------------------------------------------- formatting
def format_event(event: dict) -> str:
    """One log line: time, event type, then key=value pairs."""
    ts = event.get("ts", time.time())
    stamp = time.strftime("%H:%M:%S", time.localtime(ts)) + f".{int(ts * 1000) % 1000:03d}"
    parts = []
    for key, value in event.items():
        if key in ("type", "ts"):
            continue
        if key == "ranking":
            value = ",".join(f"{name}:{score}" for name, score in value)
        elif isinstance(value, list):
            value = ",".join(str(item) for item in value)
        parts.append(f"{key}={value}")
    return f"[{stamp}] {event.get('type', '?'):<10} " + " ".join(parts)


def format_live_ranking(event: dict) -> str:
    """One console line, printed after each question."""
    entries = " | ".join(f"{position}. {name} {score}" for position, (name, score)
                         in enumerate(event["ranking"], start=1))
    return f"[leaderboard] after question {event.get('number')}/{event.get('total')}: {entries}"


# ------------------------------------------------------------ process body
def run_logger(events, log_path: str, quiet: bool = False) -> None:
    """Body of the logger process (a module-level function: with the "spawn"
    start method, used on Windows and macOS, the target must be importable
    and its arguments picklable). With quiet=True nothing is printed on the
    console (used by the benchmark); the log file is written anyway."""
    # Ctrl+C in the terminal reaches every process of the group. The server
    # stops this process cleanly with the sentinel, so ignore SIGINT here.
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    parent = mp.parent_process()
    stats = GameStats()
    reported = False

    with open(log_path, "a", encoding="utf-8") as log:
        def write(line: str) -> None:
            log.write(line + "\n")
            log.flush()

        def report() -> None:
            for line in stats.report():
                write(line)
                if not quiet:
                    console_line(line)

        write(f"=== Quiz log started {time.strftime('%Y-%m-%d %H:%M:%S')} ===")
        while True:
            try:
                event = events.get(timeout=PARENT_CHECK)
            except queue.Empty:
                # Safety net: if the server was killed it can no longer send the
                # sentinel, so do not wait forever.
                if parent is not None and not parent.is_alive():
                    break
                continue
            if event is None:                    # end signal
                break

            write(format_event(event))
            stats.add(event)
            kind = event.get("type")
            if kind == "round_end" and not quiet:
                console_line(format_live_ranking(event))
            elif kind == "game_over" and not reported:
                report()
                reported = True

        if stats.has_game and not reported:      # the game was interrupted
            report()
        write("=== Quiz log closed ===")


# ------------------------------------------------- server side of the logger
class GameLogger:
    """Owns the queue and the logger process. Used by the server process.

    publish() can be called from any thread (mp.Queue.put is thread-safe and
    does not block: a hidden feeder thread of the queue writes in the pipe).
    """

    def __init__(self, log_path: str, quiet: bool = False) -> None:
        self._log_path = log_path
        context = mp.get_context("spawn")        # same behaviour on every OS
        self._events = context.Queue()
        # daemon: if the server crashes, the logger does not outlive it
        self._process = context.Process(target=run_logger,
                                        args=(self._events, log_path, quiet),
                                        name="logger", daemon=True)
        self._running = False

    @property
    def exitcode(self) -> Optional[int]:
        return self._process.exitcode

    def start(self) -> None:
        """Start the process. Raises OSError if the log file cannot be opened."""
        open(self._log_path, "a", encoding="utf-8").close()   # fail early, in the parent
        self._process.start()
        self._running = True

    def publish(self, event: dict) -> None:
        """Send one event to the logger process. Never raises."""
        if not self._running:
            return
        event = dict(event)                      # do not modify the caller's dict
        event.setdefault("ts", time.time())
        try:
            self._events.put(event)
        except (ValueError, OSError):            # queue already closed
            pass

    def stop(self, timeout: float = STOP_TIMEOUT) -> None:
        """Send the sentinel, wait for the process, release the queue.

        Call it AFTER every thread that publishes has been joined, so that the
        sentinel is really the last item of the queue. Safe to call twice.
        """
        if not self._running:
            return
        self._running = False                    # no new event after this point
        self._events.put(None)
        self._process.join(timeout)
        if self._process.is_alive():
            print("[logger] does not stop, terminating it")
            self._process.terminate()
            self._process.join()
        if self._process.exitcode != 0:
            # The reader is dead: the queue's feeder thread could block forever
            # on a full pipe, so do not wait for it.
            self._events.cancel_join_thread()
        self._events.close()
        self._events.join_thread()