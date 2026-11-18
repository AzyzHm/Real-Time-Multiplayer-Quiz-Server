"""Quiz console client, PHASE 3: lobby, questions, answers, rankings.

Usage: python client.py [server_ip] [port]

The server can send a message at any time (another player joins, a new
question arrives...), while the main thread waits for the keyboard. So a
small receiver thread handles the server messages as they arrive.
During a question, type the letter of your answer (A, B, C, D).
Type 'quit' to leave.
"""

import socket
import sys
import threading
from typing import Dict, Optional

import protocol as p

# After these errors, asking for another name is useless
FATAL_JOIN_ERRORS = ("lobby closed", "lobby full")


class ConsoleClient:
    """Console client: handshake in the main thread, then a receiver thread."""

    def __init__(self, address: str, port: int) -> None:
        self._address = address
        self._port = port
        self._channel = None
        self._stop = threading.Event()           # set when the session is over
        self._last_lobby_version = 0             # to ignore stale lobby messages
        self._ready_sent = False
        # Question being asked. Written by the receiver thread, read and
        # modified by the main thread: protected by this lock.
        self._lock = threading.Lock()
        self._question_id: Optional[int] = None  # None: no question at the moment
        self._choices: Dict[str, str] = {}
        self._answered = False

    def run(self) -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect((self._address, self._port))
        except OSError as exc:
            print(f"Cannot connect to {self._address}:{self._port}: {exc}")
            return
        self._channel = p.MessageChannel(sock)
        print("Connected to the server")

        try:
            if not self._join():
                return
            # Daemon: a helper thread must never keep the program alive
            threading.Thread(target=self._receive_loop, name="receiver",
                             daemon=True).start()
            self._keyboard_loop()
        except (p.ProtocolError, OSError) as exc:
            print(f"Connection problem: {exc}")
        except (EOFError, KeyboardInterrupt):
            print("\nBye")
        finally:
            self._stop.set()
            self._channel.close()                # also wakes up the receiver thread
            print("Connection closed")

    # ------------------------------------------------------------- handshake
    def _join(self) -> bool:
        """Ask a name until the server accepts it. Returns False if we cannot join."""
        while True:
            name = input("Your name: ").strip()
            self._channel.send_message(p.make_message(p.JOIN, name=name))
            while True:
                reply = self._channel.recv_message()
                if reply is None:
                    print("Server closed the connection")
                    return False
                self._handle(reply)
                if reply["type"] == p.WELCOME:
                    return True
                if reply["type"] == p.ERROR:
                    break
            if reply.get("reason") in FATAL_JOIN_ERRORS:
                return False

    # --------------------------------------------------------------- threads
    def _receive_loop(self) -> None:
        """Receiver thread: handle every message sent by the server."""
        try:
            while not self._stop.is_set():
                message = self._channel.recv_message()
                if message is None:
                    if not self._stop.is_set():
                        print("\n[server] connection closed, press Enter to exit")
                    break
                self._handle(message)
        except (p.ProtocolError, OSError) as exc:
            if not self._stop.is_set():
                print(f"\n[server] connection problem: {exc}, press Enter to exit")
        finally:
            self._stop.set()

    def _keyboard_loop(self) -> None:
        """Main thread: read the keyboard and send messages."""
        while not self._stop.is_set():
            text = input("> ").strip()
            if self._stop.is_set():
                break
            if text.lower() == "quit":
                self._stop.set()                 # before QUIT: the receiver stays quiet
                self._channel.send_message(p.make_message(p.QUIT))
                break
            if text:
                self._answer(text.upper())

    def _answer(self, choice: str) -> None:
        """Send the answer to the current question (once)."""
        with self._lock:                         # check-and-set, like the server does
            question_id = self._question_id
            if question_id is None:
                problem = "No question right now"
            elif self._answered:
                problem = "You already answered this question"
            elif choice not in self._choices:
                problem = f"Choose one of: {', '.join(self._choices)}"
            else:
                self._answered = True
                problem = None
        if problem is not None:
            print(problem)
            return
        # Sent AFTER the lock is released: never do network I/O under a lock
        self._channel.send_message(p.make_message(p.ANSWER, q=question_id, choice=choice))
        print("Answer sent, waiting for the others...")

    # --------------------------------------------------------------- display
    def _handle(self, message: dict) -> None:
        """Display one server message and react to it. Only one thread at a time
        calls this: the main thread during the handshake, then the receiver."""
        msg_type = message["type"]
        if msg_type == p.WELCOME:
            print(f"[server] Welcome, you are player {message.get('player_id')}")
        elif msg_type == p.LOBBY:
            self._handle_lobby(message)
        elif msg_type == p.QUESTION:
            self._handle_question(message)
        elif msg_type == p.RESULT:
            self._handle_result(message)
        elif msg_type == p.SCORES:
            self._show_ranking("Ranking", message.get("ranking", []))
        elif msg_type == p.GAME_OVER:
            self._show_ranking("GAME OVER, final ranking", message.get("ranking", []))
            self._stop.set()
            self._channel.close()                # the server waits for us to close first
            print("Press Enter to exit")
        elif msg_type == p.ERROR:
            print(f"[server] Error: {message.get('reason')}")
        else:
            print(f"[server] {message}")

    def _handle_lobby(self, message: dict) -> None:
        version = message.get("version", 0)
        if version <= self._last_lobby_version:
            return                                # older than what we already show
        self._last_lobby_version = version
        players = ", ".join(message.get("players", []))
        if message.get("open", True):
            print(f"[server] Lobby (open): {players}")
            return
        print(f"[server] Lobby closed, the game is starting: {players}")
        if not self._ready_sent:                  # tell the server we can start
            self._ready_sent = True
            self._channel.send_message(p.make_message(p.READY))

    def _handle_question(self, message: dict) -> None:
        with self._lock:
            self._choices = dict(message["choices"])
            self._answered = False
            self._question_id = message["q"]
        print(f"\nQuestion {message.get('number')}/{message.get('total')} "
              f"({message.get('time_limit')} s): {message['text']}")
        for letter, text in message["choices"].items():
            print(f"  {letter}) {text}")

    def _handle_result(self, message: dict) -> None:
        with self._lock:
            self._question_id = None              # the round is over
        if message.get("correct"):
            bonus = " (first!)" if message.get("first") else ""
            print(f"[result] Correct! +{message.get('points')}{bonus}")
        else:
            print(f"[result] No points. The answer was {message.get('answer')}")

    @staticmethod
    def _show_ranking(title: str, ranking: list) -> None:
        print(f"[{title}]")
        for position, (name, score) in enumerate(ranking, start=1):
            print(f"  {position}. {name}: {score}")


def main() -> None:
    address = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else p.PORT
    ConsoleClient(address, port).run()


if __name__ == "__main__":
    main()