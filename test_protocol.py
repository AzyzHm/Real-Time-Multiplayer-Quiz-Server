"""Tests of the framing in protocol.py (run: python test_protocol.py)."""

import socket
import threading
import unittest

import protocol as p


class FramingTest(unittest.TestCase):
    def setUp(self) -> None:
        # A connected pair of sockets: what a sends, b receives
        self.a, self.b = socket.socketpair()
        self.sender = p.MessageChannel(self.a)
        self.receiver = p.MessageChannel(self.b)

    def tearDown(self) -> None:
        self.a.close()
        self.b.close()

    def test_single_message(self) -> None:
        self.sender.send_message(p.make_message(p.JOIN, name="Sana"))
        self.assertEqual(self.receiver.recv_message(),
                         {"type": "JOIN", "name": "Sana"})

    def test_glued_messages(self) -> None:
        """Two messages sent in ONE write must come out as two messages."""
        raw = b'{"type":"WELCOME","player_id":1}\n{"type":"LOBBY","players":["Ali"]}\n'
        self.a.sendall(raw)
        self.assertEqual(self.receiver.recv_message()["type"], "WELCOME")
        self.assertEqual(self.receiver.recv_message()["type"], "LOBBY")

    def test_split_message(self) -> None:
        """One message cut in two writes must be reassembled."""
        self.a.sendall(b'{"type":"QU')
        self.a.sendall(b'IT"}\n')
        self.assertEqual(self.receiver.recv_message(), {"type": "QUIT"})

    def test_accents_and_newline_in_text(self) -> None:
        text = "Café\nthé, élève"
        self.sender.send_message(p.make_message(p.ERROR, reason=text))
        self.assertEqual(self.receiver.recv_message()["reason"], text)

    def test_closed_connection_returns_none(self) -> None:
        self.a.close()
        self.assertIsNone(self.receiver.recv_message())

    def test_invalid_json_raises(self) -> None:
        self.a.sendall(b"not json\n")
        with self.assertRaises(p.ProtocolError):
            self.receiver.recv_message()

    def test_message_without_type_raises(self) -> None:
        self.a.sendall(b'{"name":"Sana"}\n')
        with self.assertRaises(p.ProtocolError):
            self.receiver.recv_message()

    def test_timeout_keeps_partial_data(self) -> None:
        """A timeout in the middle of a message must not lose the bytes."""
        self.receiver.set_timeout(0.2)
        self.a.sendall(b'{"type":"QU')
        with self.assertRaises(socket.timeout):
            self.receiver.recv_message()
        self.a.sendall(b'IT"}\n')
        self.assertEqual(self.receiver.recv_message(), {"type": "QUIT"})

    def test_concurrent_senders_do_not_mix_their_bytes(self) -> None:
        """Several threads write to the SAME channel (a broadcast does that)."""
        senders, per_sender, size = 6, 100, 3000
        payload = "x" * size

        def send_many(sender_id: int) -> None:
            for n in range(per_sender):
                self.sender.send_message(
                    p.make_message("TEST", sender=sender_id, n=n, data=payload))

        threads = [threading.Thread(target=send_many, args=(i,)) for i in range(senders)]
        for t in threads:
            t.start()
        received = [self.receiver.recv_message() for _ in range(senders * per_sender)]
        for t in threads:
            t.join()

        self.assertTrue(all(m["data"] == payload for m in received))
        for sender_id in range(senders):              # per-sender order is kept
            seen = [m["n"] for m in received if m["sender"] == sender_id]
            self.assertEqual(seen, list(range(per_sender)))


if __name__ == "__main__":
    unittest.main()