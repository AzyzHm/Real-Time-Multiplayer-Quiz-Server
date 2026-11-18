"""Quiz server, PHASE 1: basic ITERATIVE server (one client at a time).

Goal of this phase: check that the protocol and the framing work between
a real server and a real client. There is no concurrency yet.
Phase 2 will replace serve_client() by one worker thread per player.
"""

import socket
import sys

import protocol as p


class QuizServer:
    """Listens on a TCP port and serves clients one after the other."""

    def __init__(self, host: str = p.HOST, port: int = p.PORT) -> None:
        self._host = host
        self._port = port
        self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Reuse the address immediately after a restart (seen in the course)
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    def run(self) -> None:
        """Bind, listen, then accept clients forever (until Ctrl+C)."""
        try:
            self._listener.bind((self._host, self._port))
        except socket.error as exc:
            print(f"Cannot bind port {self._port}: {exc}")
            sys.exit(1)
        self._listener.listen(5)
        print(f"Server listening on port {self._port} (Ctrl+C to stop)")

        try:
            while True:
                service_socket, client_info = self._listener.accept()
                print(f"Client connected: {client_info[0]}:{client_info[1]}")
                self.serve_client(p.MessageChannel(service_socket))
        except KeyboardInterrupt:
            print("\nServer stopped")
        finally:
            self._listener.close()

    def serve_client(self, channel: p.MessageChannel) -> None:
        """Dialogue with one client until QUIT or disconnection."""
        player_name = None
        try:
            while True:
                message = channel.recv_message()
                if message is None:                      # client closed the socket
                    print("Client disconnected")
                    break
                player_name = self._handle(channel, message, player_name)
                if message["type"] == p.QUIT:
                    print(f"{player_name or 'Client'} left")
                    break
        except p.ProtocolError as exc:
            print(f"Protocol error: {exc}")
        except OSError as exc:                           # reset by peer, etc.
            print(f"Connection error: {exc}")
        finally:
            channel.close()

    def _handle(self, channel: p.MessageChannel, message: dict,
                player_name):
        """Answer one message. Returns the (possibly new) player name."""
        msg_type = message["type"]

        if msg_type == p.JOIN:
            player_name = str(message.get("name", "")).strip()
            if not player_name:
                channel.send_message(p.make_message(p.ERROR, reason="name required"))
                return None
            # Two messages sent back to back: the client framing must cope
            channel.send_message(p.make_message(p.WELCOME, player_id=1))
            channel.send_message(p.make_message(p.LOBBY, players=[player_name]))

        elif msg_type == p.ANSWER:
            # No game yet in phase 1, so any answer is rejected
            channel.send_message(p.make_message(p.ERROR, reason="no game in progress"))

        elif msg_type != p.QUIT:
            channel.send_message(p.make_message(p.ERROR, reason="unknown message type"))

        return player_name


if __name__ == "__main__":
    QuizServer().run()