"""Quiz console client, PHASE 2: join the lobby and follow it live.

Usage: python client.py [server_ip] [port]

The server can send a message at any time (for example when another player
joins), while the main thread waits for the keyboard. So a small receiver
thread displays the server messages as they arrive.
Type any text to send it as a test ANSWER (rejected until phase 3),
type 'quit' to leave.
"""

import socket
import sys
import threading

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
                self._show(reply)
                if reply["type"] == p.WELCOME:
                    return True
                if reply["type"] == p.ERROR:
                    break
            if reply.get("reason") in FATAL_JOIN_ERRORS:
                return False

    # --------------------------------------------------------------- threads
    def _receive_loop(self) -> None:
        """Receiver thread: display every message sent by the server."""
        try:
            while not self._stop.is_set():
                message = self._channel.recv_message()
                if message is None:
                    print("\n[server] connection closed, press Enter to exit")
                    break
                self._show(message)
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
                self._channel.send_message(
                    p.make_message(p.ANSWER, q=0, choice=text.upper()))

    # --------------------------------------------------------------- display
    def _show(self, message: dict) -> None:
        """Display one server message. Only one thread at a time calls this:
        the main thread during the handshake, then the receiver thread."""
        msg_type = message["type"]
        if msg_type == p.WELCOME:
            print(f"[server] Welcome, you are player {message.get('player_id')}")
        elif msg_type == p.LOBBY:
            version = message.get("version", 0)
            if version <= self._last_lobby_version:
                return                            # older than what we already show
            self._last_lobby_version = version
            status = "open" if message.get("open", True) else "CLOSED, game starting"
            players = ", ".join(message.get("players", []))
            print(f"[server] Lobby ({status}): {players}")
        elif msg_type == p.ERROR:
            print(f"[server] Error: {message.get('reason')}")
        else:
            print(f"[server] {message}")


def main() -> None:
    address = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else p.PORT
    ConsoleClient(address, port).run()


if __name__ == "__main__":
    main()