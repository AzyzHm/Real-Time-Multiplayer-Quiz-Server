"""Integration tests: a real QuizServer on a free port, real sockets.

Run: python test_server.py
"""

import socket
import threading
import time
import unittest

import protocol as p
from game_state import Question
from server import QuizServer

QUESTIONS = [
    Question(1, "First?", {"A": "a", "B": "b", "C": "c", "D": "d"}, "B", 1.5),
    Question(2, "Second?", {"A": "a", "B": "b", "C": "c", "D": "d"}, "C", 1.5),
]
GOOD = {1: "B", 2: "C"}          # the right answers


def connect(port: int, timeout: float = 3) -> p.MessageChannel:
    # The timeout applies to recv: a test fails instead of hanging forever
    return p.MessageChannel(socket.create_connection(("127.0.0.1", port), timeout=timeout))


def read_until(channel: p.MessageChannel, wanted) -> dict:
    """Read messages until `wanted(message)` is true, and return that message."""
    while True:
        message = channel.recv_message()
        if message is None:
            raise AssertionError("connection closed while waiting for a message")
        if wanted(message):
            return message


def lobby_with(names):
    return lambda m: m["type"] == p.LOBBY and m["players"] == names


def join(port: int, name: str) -> p.MessageChannel:
    channel = connect(port)
    channel.send_message(p.make_message(p.JOIN, name=name))
    read_until(channel, lambda m: m["type"] == p.WELCOME)
    return channel


class ServerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.server = QuizServer("127.0.0.1", 0, QUESTIONS)   # port 0: free port
        self.server.start()
        self.port = self.server.port
        self.channels = []

    def tearDown(self) -> None:
        for channel in self.channels:
            channel.close()
        self.server.stop()

    def client(self, name: str) -> p.MessageChannel:
        channel = join(self.port, name)
        self.channels.append(channel)
        return channel

    def test_join_then_lobby(self) -> None:
        ali = self.client("Ali")
        read_until(ali, lobby_with(["Ali"]))

    def test_everybody_sees_new_players(self) -> None:
        ali = self.client("Ali")
        sana = self.client("Sana")
        read_until(ali, lobby_with(["Ali", "Sana"]))
        read_until(sana, lobby_with(["Ali", "Sana"]))

    def test_duplicate_name_refused(self) -> None:
        self.client("Ali")
        other = connect(self.port)
        self.channels.append(other)
        other.send_message(p.make_message(p.JOIN, name="ali"))
        error = read_until(other, lambda m: m["type"] == p.ERROR)
        self.assertEqual(error["reason"], "name already taken")

    def test_quit_updates_the_lobby(self) -> None:
        ali = self.client("Ali")
        sana = self.client("Sana")
        read_until(ali, lobby_with(["Ali", "Sana"]))
        sana.send_message(p.make_message(p.QUIT))
        read_until(ali, lobby_with(["Ali"]))

    def test_abrupt_disconnect_updates_the_lobby(self) -> None:
        ali = self.client("Ali")
        sana = self.client("Sana")
        read_until(ali, lobby_with(["Ali", "Sana"]))
        sana.close()                                  # no QUIT message
        read_until(ali, lobby_with(["Ali"]))

    def test_start_needs_two_players(self) -> None:
        self.client("Ali")
        self.assertFalse(self.server.start_game())

    def test_late_join_refused_after_start(self) -> None:
        ali = self.client("Ali")
        self.client("Sana")
        self.assertTrue(self.server.start_game())
        closed = read_until(ali, lambda m: m["type"] == p.LOBBY and not m["open"])
        self.assertEqual(closed["players"], ["Ali", "Sana"])
        late = connect(self.port)
        self.channels.append(late)
        late.send_message(p.make_message(p.JOIN, name="Eya"))
        error = read_until(late, lambda m: m["type"] == p.ERROR)
        self.assertEqual(error["reason"], "lobby closed")

    def test_silent_client_does_not_block_the_others(self) -> None:
        """The reason for threads: an iterative server would freeze here."""
        silent = connect(self.port)                   # connects, never sends JOIN
        self.channels.append(silent)
        ali = self.client("Ali")
        read_until(ali, lobby_with(["Ali"]))

    def test_six_simultaneous_joins_then_lobby_full(self) -> None:
        names = [f"p{i}" for i in range(6)]
        gate = threading.Barrier(6)
        slots = [None] * 6

        def run(i: int) -> None:
            gate.wait()
            slots[i] = join(self.port, names[i])

        threads = [threading.Thread(target=run, args=(i,)) for i in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.channels.extend(slots)

        for channel in slots:                         # everybody ends with the full list
            read_until(channel, lambda m: m["type"] == p.LOBBY and len(m["players"]) == 6)

        seventh = connect(self.port)
        self.channels.append(seventh)
        seventh.send_message(p.make_message(p.JOIN, name="late"))
        error = read_until(seventh, lambda m: m["type"] == p.ERROR)
        self.assertEqual(error["reason"], "lobby full")

    def test_stop_wakes_up_idle_clients(self) -> None:
        ali = self.client("Ali")
        read_until(ali, lobby_with(["Ali"]))
        self.server.stop()                            # must not hang
        self.assertIsNone(ali.recv_message())         # server closed our connection


class Bot(threading.Thread):
    """A scripted player running in its own thread (a tiny version of bots.py).

    answers: question id -> letter to send (missing id: stay silent)
    delay:   seconds to wait before answering
    leave_after_question: close the socket as soon as the first question arrives
    leave_when_closed:    close the socket as soon as the lobby closes (no READY)
    """

    def __init__(self, port, name, answers=None, delay=0.0,
                 leave_after_question=False, leave_when_closed=False):
        super().__init__(name=f"bot-{name}")
        self.port, self.player = port, name
        self.answers = GOOD if answers is None else answers
        self.delay = delay
        self.leave_after_question = leave_after_question
        self.leave_when_closed = leave_when_closed
        self.joined = threading.Event()
        self.messages = []

    def run(self) -> None:
        channel = connect(self.port, timeout=10)
        try:
            channel.send_message(p.make_message(p.JOIN, name=self.player))
            while True:
                message = channel.recv_message()
                if message is None:
                    return
                self.messages.append(message)
                kind = message["type"]
                if kind == p.WELCOME:
                    self.joined.set()
                elif kind == p.LOBBY and not message["open"]:
                    if self.leave_when_closed:
                        return
                    channel.send_message(p.make_message(p.READY))
                elif kind == p.QUESTION:
                    if self.leave_after_question:
                        return
                    choice = self.answers.get(message["q"])
                    if choice is not None:
                        time.sleep(self.delay)
                        channel.send_message(
                            p.make_message(p.ANSWER, q=message["q"], choice=choice))
                elif kind == p.GAME_OVER:
                    return                        # closing first lets the server finish
        finally:
            self.joined.set()                     # never leave the test waiting
            channel.close()

    def of_type(self, msg_type):
        return [m for m in self.messages if m["type"] == msg_type]


class GameTest(unittest.TestCase):
    def setUp(self) -> None:
        self.server = QuizServer("127.0.0.1", 0, QUESTIONS)
        self.server.start()

    def tearDown(self) -> None:
        self.server.stop()

    def play(self, *bots: Bot, timeout: float = 15) -> None:
        """Start the bots, wait until they all joined, start the game, wait for the end."""
        for bot in bots:
            bot.start()
            self.assertTrue(bot.joined.wait(5))   # sequential joins: stable order
        self.assertTrue(self.server.start_game())
        self.assertTrue(self.server.wait_until_done(timeout), "the game did not finish")
        for bot in bots:
            bot.join(5)
            self.assertFalse(bot.is_alive())

    def test_full_game_with_two_players(self) -> None:
        port = self.server.port
        ali = Bot(port, "Ali")                          # answers at once
        sana = Bot(port, "Sana", delay=0.3)             # answers a bit later
        self.play(ali, sana)

        results = ali.of_type(p.RESULT)
        self.assertEqual([(r["q"], r["points"], r["first"], r["answer"]) for r in results],
                         [(1, 150, True, "B"), (2, 150, True, "C")])
        results = sana.of_type(p.RESULT)
        self.assertEqual([(r["points"], r["first"]) for r in results],
                         [(100, False), (100, False)])
        self.assertEqual(ali.of_type(p.SCORES)[0]["ranking"], [["Ali", 150], ["Sana", 100]])
        final = ali.of_type(p.GAME_OVER)[0]["ranking"]
        self.assertEqual(final, [["Ali", 300], ["Sana", 200]])
        self.assertEqual(sana.of_type(p.GAME_OVER)[0]["ranking"], final)

    def test_questions_do_not_leak_the_answer(self) -> None:
        port = self.server.port
        bots = (Bot(port, "Ali"), Bot(port, "Sana"))
        self.play(*bots)
        for question in bots[0].of_type(p.QUESTION):
            self.assertNotIn("answer", question)
            self.assertEqual(question["total"], 2)

    def test_everybody_answers_at_the_same_instant(self) -> None:
        port = self.server.port
        bots = [Bot(port, f"p{i}") for i in range(5)]
        self.play(*bots)
        for round_index in range(2):
            firsts = [b.of_type(p.RESULT)[round_index]["first"] for b in bots]
            self.assertEqual(sum(firsts), 1)             # exactly one bonus per round
        final = bots[0].of_type(p.GAME_OVER)[0]["ranking"]
        self.assertEqual(sum(score for _, score in final), 5 * 200 + 2 * 50)

    def test_silent_player_does_not_block_the_round(self) -> None:
        port = self.server.port
        ali = Bot(port, "Ali")
        silent = Bot(port, "Silent", answers={})        # joins, ready, never answers
        started = time.monotonic()
        self.play(ali, silent)
        elapsed = time.monotonic() - started
        self.assertGreater(elapsed, 2.5)                 # two rounds waited for the time limit
        self.assertLess(elapsed, 8)
        result = silent.of_type(p.RESULT)[0]
        self.assertEqual((result["correct"], result["points"], result["answer"]),
                         (False, 0, "B"))
        self.assertEqual(ali.of_type(p.GAME_OVER)[0]["ranking"],
                         [["Ali", 300], ["Silent", 0]])

    def test_player_leaving_during_a_round_does_not_block_the_others(self) -> None:
        port = self.server.port
        ali = Bot(port, "Ali")
        leaver = Bot(port, "Leaver", leave_after_question=True)
        started = time.monotonic()
        self.play(ali, leaver)
        self.assertLess(time.monotonic() - started, 5)   # no waiting for the time limit
        self.assertEqual(ali.of_type(p.GAME_OVER)[0]["ranking"],
                         [["Ali", 300], ["Leaver", 0]])

    def test_player_leaving_before_ready_does_not_block_the_others(self) -> None:
        port = self.server.port
        ali = Bot(port, "Ali")
        leaver = Bot(port, "Leaver", leave_when_closed=True)
        self.play(ali, leaver)
        self.assertEqual(len(ali.of_type(p.GAME_OVER)), 1)

    def test_everybody_leaving_ends_the_game(self) -> None:
        port = self.server.port
        bots = [Bot(port, "A", leave_when_closed=True),
                Bot(port, "B", leave_when_closed=True)]
        self.play(*bots)                                 # play() fails if the game hangs


if __name__ == "__main__":
    unittest.main()