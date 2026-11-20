"""Bot clients for stress tests and the race condition demo (PHASE 5).

Each bot is a thread with its own TCP connection, playing like a console
client: JOIN, READY when the lobby closes, then an answer to each question.
The bots know the right answers (they read questions.json), so they can all
answer CORRECTLY at the same moment and compete for the "first correct
answer" bonus.

With --sync, all the bots wait on a threading.Barrier after receiving a
question and send their answer at the very same instant. This is the harshest
test for the server: exactly ONE player must get the bonus. At the end the
bots check this from the clients' side and report any question where several
players got the bonus (the race condition).

Typical demo, in two terminals:
    python server.py --autostart 12 --max-players 12 [--unsafe]
    python bots.py --players 12 --sync
"""

import argparse
import os
import random
import socket
import sys
import threading
import time
from typing import Dict, List, Optional

import protocol as p
from game_state import load_questions

HERE = os.path.dirname(os.path.abspath(__file__))
QUESTIONS_FILE = os.path.join(HERE, "questions.json")
RECV_TIMEOUT = 120.0      # a bot never waits forever for the server
GATE_TIMEOUT = 10.0       # max wait for the other bots at the answer barrier
JOIN_ERRORS = ("lobby closed", "lobby full", "name already taken", "invalid name")


class BotThread(threading.Thread):
    """One bot player."""

    def __init__(self, player: str, address: str, port: int,
                 answers: Dict[int, str], accuracy: float = 1.0,
                 delay_max: float = 1.0,
                 gate: Optional[threading.Barrier] = None) -> None:
        super().__init__(name=f"bot-{player}")
        self.player = player
        self._address = address
        self._port = port
        self._answers = answers          # question id -> right letter
        self._accuracy = accuracy        # probability of a right answer
        self._delay_max = delay_max      # random thinking time, in seconds
        self._gate = gate                # if set: everybody answers at the same instant
        self.results: List[dict] = []    # RESULT messages received
        self.final_ranking: Optional[list] = None
        self.error: Optional[str] = None

    def run(self) -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect((self._address, self._port))
            sock.settimeout(RECV_TIMEOUT)
            self._play(p.MessageChannel(sock))
        except (p.ProtocolError, OSError) as exc:
            self.error = str(exc)
        finally:
            sock.close()                 # the server waits for the client to close first

    def _play(self, channel: p.MessageChannel) -> None:
        channel.send_message(p.make_message(p.JOIN, name=self.player))
        joined = False
        ready_sent = False
        while True:
            message = channel.recv_message()
            if message is None:
                return
            kind = message["type"]
            if kind == p.WELCOME:
                joined = True
            elif kind == p.LOBBY and not message.get("open", True) and not ready_sent:
                ready_sent = True
                channel.send_message(p.make_message(p.READY))
            elif kind == p.QUESTION:
                self._answer(channel, message)
            elif kind == p.RESULT:
                self.results.append(message)
            elif kind == p.GAME_OVER:
                self.final_ranking = message["ranking"]
                return
            elif kind == p.ERROR:
                self.error = message.get("reason")
                if not joined and self.error in JOIN_ERRORS:
                    return

    def _answer(self, channel: p.MessageChannel, question: dict) -> None:
        letters = list(question["choices"])
        right = self._answers.get(question["q"], letters[0])
        if random.random() < self._accuracy:
            choice = right
        else:
            choice = random.choice([letter for letter in letters if letter != right])

        if self._gate is not None:
            try:
                self._gate.wait(GATE_TIMEOUT)    # everybody sends at the same instant
            except threading.BrokenBarrierError:
                pass                             # a bot is missing: go on without sync
        else:
            time.sleep(random.uniform(0, self._delay_max))
        channel.send_message(p.make_message(p.ANSWER, q=question["q"], choice=choice))


def find_bonus_anomalies(bots: List[BotThread]) -> List[str]:
    """Check the results from the clients' side.

    For each question exactly one player must have the "first" bonus if at least
    one player answered correctly, and nobody otherwise. Anything else means
    the server updated the shared round data without proper mutual exclusion.
    """
    per_question: Dict[int, List[int]] = {}      # question id -> [correct, firsts]
    for bot in bots:
        for result in bot.results:
            counts = per_question.setdefault(result["q"], [0, 0])
            counts[0] += bool(result["correct"])
            counts[1] += bool(result["first"])
    anomalies = []
    for question_id, (correct, firsts) in sorted(per_question.items()):
        expected = 1 if correct else 0
        if firsts != expected:
            anomalies.append(f"question {question_id}: {firsts} first bonuses for "
                             f"{correct} correct answers (expected {expected})")
    return anomalies


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Bot players for the quiz server")
    parser.add_argument("--players", type=int, default=6, help="number of bots")
    parser.add_argument("--address", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=p.PORT)
    parser.add_argument("--prefix", default="bot", help="name prefix of the bots")
    parser.add_argument("--accuracy", type=float, default=1.0,
                        help="probability that a bot answers correctly (default 1)")
    parser.add_argument("--delay-max", type=float, default=1.0,
                        help="max random thinking time in seconds (default 1)")
    parser.add_argument("--sync", action="store_true",
                        help="all bots answer at the very same instant (race demo)")
    args = parser.parse_args(argv)

    try:
        answers = {q.id: q.answer for q in load_questions(QUESTIONS_FILE)}
    except (OSError, ValueError) as exc:
        print(f"Cannot load {QUESTIONS_FILE}: {exc}")
        return 2

    gate = threading.Barrier(args.players) if args.sync else None
    bots = [BotThread(f"{args.prefix}{i + 1}", args.address, args.port, answers,
                      args.accuracy, args.delay_max, gate)
            for i in range(args.players)]
    print(f"Starting {len(bots)} bots{' (synchronised answers)' if args.sync else ''}.")
    print("The game starts when the host types 'start' (or with server --autostart).")
    for bot in bots:
        bot.start()
        time.sleep(0.02)                         # do not flood the listen queue
    for bot in bots:
        bot.join()

    finished = sum(1 for bot in bots if bot.final_ranking is not None)
    print(f"{finished}/{len(bots)} bots reached GAME_OVER")
    for bot in bots:
        if bot.final_ranking is None:
            print(f"  {bot.player}: did not finish ({bot.error})")
    anomalies = find_bonus_anomalies(bots)
    if anomalies:
        print(f"RACE CONDITION DETECTED in {len(anomalies)} question(s):")
        for line in anomalies:
            print(f"  {line}")
        return 1
    print("OK: exactly one first-answer bonus per question")
    return 0


if __name__ == "__main__":
    sys.exit(main())