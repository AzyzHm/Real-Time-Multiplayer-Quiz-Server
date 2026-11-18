"""One worker thread per connected client (the "executant" of the course).

The listener thread of the server accepts a connection, creates a
ClientWorker for it and goes back to accept(). Every worker lives its own
dialogue with its client, so a silent or slow client never blocks the others.
"""

import threading
from typing import Optional, Tuple

import protocol as p
from game_state import GameState, LobbyError, Player


def broadcast_lobby(state: GameState) -> None:
    """Send the current lobby to every player (1 to N).

    The lock is taken and released INSIDE lobby_snapshot(). The sends happen
    afterwards, with no game lock held.
    The `version` lets a client ignore a stale list: two workers can
    broadcast at the same time and their messages may cross on the network.
    """
    snapshot = state.lobby_snapshot()
    message = p.make_message(p.LOBBY, version=snapshot.version,
                             players=snapshot.names, open=snapshot.is_open)
    p.broadcast(snapshot.channels, message)


class ClientWorker(threading.Thread):
    """Reads the messages of one client and reacts to them."""

    def __init__(self, channel: p.MessageChannel, client_info: Tuple[str, int],
                 state: GameState) -> None:
        super().__init__(name=f"worker-{client_info[0]}:{client_info[1]}")
        self._channel = channel
        self._state = state
        self._player: Optional[Player] = None    # set once JOIN is accepted

    def disconnect(self) -> None:
        """Called by the server at shutdown: closing the socket wakes up recv()."""
        self._channel.close()

    # ------------------------------------------------------------- thread body
    def run(self) -> None:
        try:
            self._dialogue()
        except p.ProtocolError as exc:
            print(f"[{self.name}] protocol error: {exc}")
        except OSError as exc:                   # reset by peer, shutdown, ...
            print(f"[{self.name}] connection error: {exc}")
        finally:
            self._leave()
            self._channel.close()

    def _dialogue(self) -> None:
        """Read messages until QUIT or until the client disconnects."""
        while True:
            message = self._channel.recv_message()   # blocking, like in the course
            if message is None:                      # peer closed the socket
                print(f"[{self.name}] disconnected")
                return
            if message["type"] == p.QUIT:
                return
            self._handle(message)

    # ------------------------------------------------------------ message handlers
    def _handle(self, message: dict) -> None:
        msg_type = message["type"]
        if msg_type == p.JOIN:
            self._join(message)
        elif msg_type == p.ANSWER:
            # Phase 3 will score answers here, under the state lock
            self._send_error("no game in progress")
        else:
            self._send_error("unknown message type")

    def _join(self, message: dict) -> None:
        if self._player is not None:
            self._send_error("already joined")
            return
        try:
            self._player = self._state.add_player(str(message.get("name", "")),
                                                  self._channel)
        except LobbyError as exc:
            self._send_error(exc.reason)
            return

        print(f"[{self.name}] joined as {self._player.name}")
        self._channel.send_message(
            p.make_message(p.WELCOME, player_id=self._player.player_id))
        broadcast_lobby(self._state)

    def _leave(self) -> None:
        """Remove the player (if any) and tell the others."""
        if self._player is None:
            return
        lobby_changed = self._state.remove_player(self._player.player_id)
        self._player = None
        if lobby_changed:
            broadcast_lobby(self._state)

    def _send_error(self, reason: str) -> None:
        self._channel.send_message(p.make_message(p.ERROR, reason=reason))