"""Integration tests: a real QuizServer on a free port, real sockets.

Run: python test_server.py
"""

import socket
import threading
import unittest

import protocol as p
from server import QuizServer


def connect(port: int) -> p.MessageChannel:
    # The timeout applies to recv: a test fails instead of hanging forever
    return p.MessageChannel(socket.create_connection(("127.0.0.1", port), timeout=3))


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
        self.server = QuizServer("127.0.0.1", 0)     # port 0: the OS picks a free port
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


if __name__ == "__main__":
    unittest.main()