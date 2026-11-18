"""Protocol layer of the quiz: JSON lines over TCP.

TCP is a stream of bytes, not a stream of messages. Two messages sent
back to back can arrive glued together in one recv(), and one message can
arrive cut in two. To solve this, every message is one JSON object
followed by a newline ("\\n"). The receiver keeps a buffer and only
returns a message once a full line is available.

json.dumps escapes newlines inside strings (as the two characters \\ and n),
so the delimiter can never appear inside a message payload.

Thread safety of a MessageChannel:
  - Reading: ONE reader thread only (the worker of that player). The receive
    buffer is not protected.
  - Writing: any thread may call send_message(). A broadcast from worker A
    can target the socket of worker B while B is also sending, so writes on
    one socket are serialised by a small per-channel lock (_send_lock).
    This lock is a "leaf" lock: nothing else is ever acquired while holding
    it, so it cannot take part in a deadlock.
"""

import json
import socket
import threading
from typing import Iterable, Optional

# ---------------------------------------------------------------- constants
HOST = ""            # empty string: reachable through any address of the machine
PORT = 50000         # free port above 1023 (as in the course)
ENCODING = "utf-8"
RECV_SIZE = 4096     # small power of two, as advised in the course
DELIMITER = b"\n"    # end of one message

# Client -> server message types
JOIN = "JOIN"
ANSWER = "ANSWER"
QUIT = "QUIT"

# Server -> client message types
WELCOME = "WELCOME"
LOBBY = "LOBBY"
QUESTION = "QUESTION"
RESULT = "RESULT"
SCORES = "SCORES"
GAME_OVER = "GAME_OVER"
ERROR = "ERROR"


class ProtocolError(Exception):
    """Raised when received bytes are not a valid protocol message."""


def make_message(msg_type: str, **fields) -> dict:
    """Build a protocol message, for example make_message(JOIN, name="Sana")."""
    return {"type": msg_type, **fields}


class MessageChannel:
    """Wraps a connected TCP socket and exchanges JSON-line messages.

    The socket itself is kept public-free on purpose: the rest of the
    program only sees send_message() and recv_message().
    """

    def __init__(self, sock: socket.socket) -> None:
        self._sock = sock
        self._buffer = b""   # bytes received but not yet returned as a message
        self._send_lock = threading.Lock()   # one writer at a time on this socket

    def send_message(self, message: dict) -> None:
        """Send one message completely.

        sendall() loops on send() until every byte is sent, which is the
        'loop until complete' rule seen in the course.
        NEVER call this while holding the game state lock: a slow client
        would block every other thread waiting for that lock.
        """
        data = json.dumps(message).encode(ENCODING) + DELIMITER
        with self._send_lock:      # two threads must not interleave their bytes
            self._sock.sendall(data)

    def recv_message(self) -> Optional[dict]:
        """Return the next message, or None if the peer closed the connection.

        Blocks until a full line is available. If a socket timeout is set
        (see set_timeout), socket.timeout is raised and the partial data
        stays in the buffer, so nothing is lost for the next call.
        Raises ProtocolError if the line is not a valid message.
        """
        while DELIMITER not in self._buffer:
            chunk = self._sock.recv(RECV_SIZE)
            if chunk == b"":          # empty bytes: the peer closed the socket
                return None
            self._buffer += chunk

        line, self._buffer = self._buffer.split(DELIMITER, 1)
        return self._decode(line)

    def set_timeout(self, seconds: Optional[float]) -> None:
        """Set a timeout on recv (None means blocking). Used later per round."""
        self._sock.settimeout(seconds)

    def close(self) -> None:
        """Shut down and close the socket.

        Safe to call several times and from another thread: shutdown() wakes
        up a thread that is blocked in recv() on this socket (it then gets
        b"" and recv_message() returns None).
        """
        try:
            self._sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass                   # already closed, or never connected
        self._sock.close()

    @staticmethod
    def _decode(line: bytes) -> dict:
        """Turn one received line into a message dict, or raise ProtocolError."""
        try:
            message = json.loads(line.decode(ENCODING))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ProtocolError(f"invalid message: {line!r}") from exc
        if not isinstance(message, dict) or "type" not in message:
            raise ProtocolError(f"message without a type: {line!r}")
        return message


def broadcast(channels: Iterable[MessageChannel], message: dict) -> None:
    """Send one message to several channels (1 to N, as in the course).

    A channel that fails is skipped: the worker thread of that player will
    notice the broken connection by itself and clean up.
    Call this OUTSIDE the game state lock.
    """
    for channel in channels:
        try:
            channel.send_message(message)
        except OSError:
            pass