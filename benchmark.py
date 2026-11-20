"""Optional benchmark (PHASE 5): what does logging cost the game threads?

N player threads answer the same question at the same instant. Each one scores
its answer with GameState.submit_answer (under the lock) and then logs it.
Three ways of logging are compared:

  none     : no logging at all (the baseline)
  inline   : each game thread formats and writes the log file itself
             (one lock around the file, flush after each line)
  process  : the game thread only puts the event in an mp.Queue, the logger
             process does the file writing (this project's solution)

What to expect, honestly: the logger process is NOT a speedup compared with
no logging. Its job is to keep file I/O and statistics off the game threads.
The interesting comparison is inline versus process.

Usage: python benchmark.py [--players 200] [--rounds 5]
"""

import argparse
import os
import statistics
import tempfile
import threading
import time

from game_state import GameState, Question
from logger_process import GameLogger, format_event


class InlineLogger:
    """The naive solution: every game thread writes to the file itself."""

    def __init__(self, path: str) -> None:
        self._file = open(path, "a", encoding="utf-8")
        self._lock = threading.Lock()            # one writer at a time on the file

    def publish(self, event: dict) -> None:
        line = format_event(event) + "\n"
        with self._lock:
            self._file.write(line)
            self._file.flush()

    def close(self) -> None:
        self._file.close()


def run_mode(mode: str, players: int, rounds: int, folder: str):
    """Returns (per-answer latencies in ms, total time in s)."""
    path = os.path.join(folder, f"{mode}.log")
    logger = None
    if mode == "inline":
        logger = InlineLogger(path)
    elif mode == "process":
        logger = GameLogger(path, quiet=True)   # no console output
        logger.start()

    questions = [Question(i + 1, "?", {"A": "a", "B": "b"}, "B", 10) for i in range(rounds)]
    state = GameState(questions, max_players=players,
                      publish=logger.publish if logger else None)
    for i in range(players):
        state.add_player(f"p{i}", None)
    state.close_lobby()

    latencies = [[] for _ in range(players)]     # one list per thread: no sharing

    def player(index: int) -> None:
        for _ in range(rounds):
            state.start_barrier.wait()           # all players start together
            started = time.perf_counter()
            result = state.submit_answer(index + 1, state.round_info().question_id, "B")
            if logger is not None:
                logger.publish({"type": "answer", "player": f"p{index}", "q": 1,
                                "correct": result.correct, "points": result.points,
                                "first": result.first, "t": 0.0})
            latencies[index].append((time.perf_counter() - started) * 1000)
            state.end_barrier.wait()

    threads = [threading.Thread(target=player, args=(i,)) for i in range(players)]
    begin = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    total = time.perf_counter() - begin

    if mode == "inline":
        logger.close()
    elif mode == "process":
        logger.stop()                            # included: all events are really written
        total = time.perf_counter() - begin
    return [ms for per_thread in latencies for ms in per_thread], total


def main() -> None:
    parser = argparse.ArgumentParser(description="Cost of logging for N simultaneous answers")
    parser.add_argument("--players", type=int, default=200)
    parser.add_argument("--rounds", type=int, default=5)
    args = parser.parse_args()

    print(f"{args.players} players answer at the same instant, {args.rounds} rounds\n")
    print(f"{'mode':<9} {'mean latency':>13} {'max latency':>12} {'total time':>11}")
    with tempfile.TemporaryDirectory() as folder:
        for mode in ("none", "inline", "process"):
            latencies, total = run_mode(mode, args.players, args.rounds, folder)
            print(f"{mode:<9} {statistics.mean(latencies):>10.2f} ms "
                  f"{max(latencies):>9.2f} ms {total:>9.2f} s")
    print("\nlatency = time a game thread spends scoring + logging one answer")
    print("total time for 'process' includes stopping the logger (all events written)")


if __name__ == "__main__":      # mandatory guard: the logger process uses spawn
    main()