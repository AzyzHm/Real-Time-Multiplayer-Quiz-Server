"""Quiz server, PHASE 2: concurrent server (listener thread + worker per client).

Threads of the server process:
  - main thread     : the host console (commands: list, start, quit)
  - listener thread : blocks on accept(), creates one ClientWorker per
                      connection, then goes back to accept()
  - worker threads  : one per client, see worker.py

The shared data (players, lobby) lives in GameState, protected by ONE lock.
Phase 3 will use start_game() to create the barriers and run the rounds.
"""

import socket
import threading
from typing import List, Optional

import protocol as p
from game_state import GameState, LobbyError
from worker import ClientWorker, broadcast_lobby

ACCEPT_TIMEOUT = 0.5   # seconds: lets the listener check the stop flag regularly


class QuizServer:
    """Listens on a TCP port and serves many clients at the same time."""

    def __init__(self, host: str = p.HOST, port: int = p.PORT) -> None:
        self._host = host
        self._port = port
        self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Reuse the address immediately after a restart (seen in the course)
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._state = GameState()
        self._stop = threading.Event()          # shared stop flag (course: "arreter un thread")
        self._listener_thread: Optional[threading.Thread] = None
        # Only the listener thread touches this list while the server runs.
        # stop() reads it AFTER join()-ing the listener, so no lock is needed.
        self._workers: List[ClientWorker] = []

    @property
    def port(self) -> int:
        """Real port in use (useful when the server was created with port 0)."""
        return self._listener.getsockname()[1]

    # ----------------------------------------------------------- life cycle
    def start(self) -> None:
        """Bind, listen and start the listener thread. Raises OSError if bind fails."""
        self._listener.bind((self._host, self._port))
        self._listener.listen(5)
        self._listener_thread = threading.Thread(target=self._accept_loop,
                                                 name="listener")
        self._listener_thread.start()

    def stop(self) -> None:
        """Clean shutdown: stop the listener, wake up the workers, join everything."""
        self._stop.set()
        if self._listener_thread is not None:
            self._listener_thread.join()         # no more worker is created after this
        self._listener.close()
        for worker in self._workers:
            worker.disconnect()                  # closing the socket wakes up recv()
        for worker in self._workers:
            worker.join()

    def run(self) -> None:
        """Start the server and run the host console until 'quit' or Ctrl+C."""
        try:
            self.start()
        except OSError as exc:
            print(f"Cannot bind port {self._port}: {exc}")
            return
        print(f"Server listening on port {self.port}")
        print("Host commands: list, start, quit")
        try:
            self._console_loop()
        except (KeyboardInterrupt, EOFError):
            print()
        finally:
            self.stop()
            print("Server stopped")

    # ------------------------------------------------------- listener thread
    def _accept_loop(self) -> None:
        """Body of the listener thread: accept clients and create their workers."""
        self._listener.settimeout(ACCEPT_TIMEOUT)
        while not self._stop.is_set():
            try:
                service_socket, client_info = self._listener.accept()
            except socket.timeout:
                continue                         # just re-check the stop flag
            except OSError:
                break                            # listening socket closed
            service_socket.settimeout(None)      # blocking recv in the worker
            print(f"Client connected: {client_info[0]}:{client_info[1]}")

            worker = ClientWorker(p.MessageChannel(service_socket),
                                  client_info, self._state)
            self._workers = [w for w in self._workers if w.is_alive()]  # forget finished ones
            self._workers.append(worker)
            worker.start()

    # --------------------------------------------------------- host console
    def _console_loop(self) -> None:
        while True:
            command = input("host> ").strip().lower()
            if command == "quit":
                return
            elif command == "list":
                snapshot = self._state.lobby_snapshot()
                status = "open" if snapshot.is_open else "closed"
                print(f"Lobby {status}: {', '.join(snapshot.names) or '(empty)'}")
            elif command == "start":
                self.start_game()
            elif command:
                print("Commands: list, start, quit")

    def start_game(self) -> bool:
        """Close the lobby. Returns False (and says why) if it cannot start."""
        try:
            names = self._state.close_lobby()
        except LobbyError as exc:
            print(f"Cannot start: {exc.reason}")
            return False
        broadcast_lobby(self._state)             # clients see that the lobby is closed
        print(f"Lobby closed with {len(names)} players: {', '.join(names)}")
        # PHASE 3: create start_barrier / end_barrier (parties = len(names))
        # and launch the rounds from here.
        return True


if __name__ == "__main__":
    QuizServer().run()