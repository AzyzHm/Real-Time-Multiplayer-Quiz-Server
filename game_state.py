"""Shared game state of the quiz (PHASE 2: lobby part).

All worker threads and the host console read and modify this object, so it
is the shared memory that needs mutual exclusion.

Design rules (see project description, section 7):
  1. ONE lock only (self._lock). No nested locks, so no deadlock between
     two game locks (cf. TD1 exercise 1).
  2. Every method takes the lock with `with`, so it is released even when
     an exception is raised inside the block.
  3. NOTHING is sent on a socket while the lock is held. Methods only read
     or modify data and return. The caller sends AFTER the lock is released
     (snapshot pattern: copy under the lock, use the copy outside).
"""

import threading
from dataclasses import dataclass
from typing import Dict, List, NamedTuple

from protocol import MessageChannel

MIN_PLAYERS = 2
MAX_PLAYERS = 6
MAX_NAME_LENGTH = 20


class LobbyError(Exception):
    """A join or a start was refused. `reason` is sent to the client as is."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


@dataclass
class Player:
    """One participant. `connected` becomes False for a "ghost" player."""
    player_id: int
    name: str
    channel: MessageChannel
    connected: bool = True


class LobbySnapshot(NamedTuple):
    """A consistent copy of the lobby, safe to use after the lock is released."""
    version: int                       # increases at every lobby change
    is_open: bool
    names: List[str]
    channels: List[MessageChannel]


class GameState:
    """Players, lobby status and (from phase 3) scores and round data."""

    def __init__(self, min_players: int = MIN_PLAYERS,
                 max_players: int = MAX_PLAYERS) -> None:
        self._lock = threading.Lock()          # the ONE lock of the game
        self._min_players = min_players
        self._max_players = max_players
        self._players: Dict[int, Player] = {}  # player_id -> Player, in join order
        self._next_id = 1
        self._lobby_open = True
        self._lobby_version = 0

    # ------------------------------------------------------------ lobby
    def add_player(self, name: str, channel: MessageChannel) -> Player:
        """Register a player. Raises LobbyError if the join is refused.

        The checks and the insertion are ONE critical section. Without the
        lock, two clients joining at the same instant with the same name
        (or for the last free place) could both pass the checks, exactly
        like the `cpt = cpt + 1` race of the course.
        """
        name = name.strip()
        if not name or len(name) > MAX_NAME_LENGTH:
            raise LobbyError("invalid name")

        with self._lock:
            if not self._lobby_open:
                raise LobbyError("lobby closed")
            if len(self._players) >= self._max_players:
                raise LobbyError("lobby full")
            if any(p.name.lower() == name.lower() for p in self._players.values()):
                raise LobbyError("name already taken")

            player = Player(self._next_id, name, channel)
            self._players[player.player_id] = player
            self._next_id += 1
            self._lobby_version += 1
            return player

    def remove_player(self, player_id: int) -> bool:
        """A player left. Returns True if the lobby list changed.

        While the lobby is open the player is simply removed. Once the game
        has started the player is only marked disconnected (a "ghost", used
        in phase 3 so that the barriers keep a fixed number of parties).
        """
        with self._lock:
            player = self._players.get(player_id)
            if player is None:
                return False
            if self._lobby_open:
                del self._players[player_id]
                self._lobby_version += 1
                return True
            player.connected = False
            return False

    def close_lobby(self) -> List[str]:
        """Close the lobby (host starts the game). Returns the player names.

        Raises LobbyError if already closed or if there are too few players.
        """
        with self._lock:
            if not self._lobby_open:
                raise LobbyError("lobby already closed")
            if len(self._players) < self._min_players:
                raise LobbyError(f"need at least {self._min_players} players")
            self._lobby_open = False
            self._lobby_version += 1
            return [p.name for p in self._players.values()]

    def lobby_snapshot(self) -> LobbySnapshot:
        """Copy of the lobby taken under the lock.

        The caller uses the copy (to build a message and send it) after the
        lock has been released, so a slow client can never freeze the state.
        """
        with self._lock:
            connected = [p for p in self._players.values() if p.connected]
            return LobbySnapshot(
                version=self._lobby_version,
                is_open=self._lobby_open,
                names=[p.name for p in connected],
                channels=[p.channel for p in connected],
            )