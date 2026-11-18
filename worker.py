"""One worker thread per connected client (the "executant" of the course).

The listener thread of the server accepts a connection, creates a
ClientWorker for it and goes back to accept(). Every worker lives its own
dialogue with its client, so a silent or slow client never blocks the others.

Life of a worker:
  1. Lobby: reads JOIN / QUIT messages (blocking recv).
  2. The host closes the lobby, the client answers READY, the worker plays
     the game: for each question, start_barrier -> QUESTION -> wait for the
     answer (socket timeout) -> score under the lock -> end_barrier ->
     RESULT + SCORES. Then GAME_OVER.
  3. If the client disappears once the game has started, the worker becomes a
     "ghost": it keeps calling the barriers every round without any socket
     I/O, because a Barrier needs a fixed number of parties.
"""

import socket
import threading
import time
from typing import Optional, Tuple

import protocol as p
from game_state import (AnswerResult, GameState, Leave, LobbyError, RoundError,
                        RoundInfo)

CLOSE_WAIT = 3.0   # seconds to let the client read GAME_OVER and close first


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
        self._player = None                      # set once JOIN is accepted
        self._connected = True                   # False: the client is gone (ghost)
        self._game_played = False                # _play_game() already ran

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
            self._finish()

    def _dialogue(self) -> None:
        """Read messages until QUIT, a disconnection, or the end of the game."""
        while True:
            message = self._channel.recv_message()   # blocking, like in the course
            if message is None:                      # peer closed the socket
                print(f"[{self.name}] disconnected")
                return
            if message["type"] == p.QUIT:
                return
            if self._handle(message):                # True: the game was played
                return

    def _finish(self) -> None:
        """The client is gone (or the game is over): clean up.

        Lobby still open: the player disappears and the others are told.
        Game started but not played by this worker (disconnection before
        READY): become a ghost and play the game anyway, otherwise the
        others would wait forever on the barriers.
        """
        try:
            if self._player is None:
                return
            outcome = self._state.remove_player(self._player.player_id)
            if outcome is Leave.REMOVED:
                broadcast_lobby(self._state)
            elif outcome is Leave.GHOST and not self._game_played:
                self._connected = False
                self._play_game()
        finally:
            self._channel.close()

    # ------------------------------------------------------------ lobby handlers
    def _handle(self, message: dict) -> bool:
        """Handle one lobby message. Returns True once the game has been played."""
        msg_type = message["type"]
        if msg_type == p.JOIN:
            self._join(message)
        elif msg_type == p.READY:
            return self._ready()
        elif msg_type == p.ANSWER:
            self._send_error("no game in progress")
        else:
            self._send_error("unknown message type")
        return False

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

    def _ready(self) -> bool:
        if self._player is None or not self._state.is_started():
            self._send_error("game not started")
            return False
        self._play_game()
        return True

    def _send_error(self, reason: str) -> None:
        self._channel.send_message(p.make_message(p.ERROR, reason=reason))

    # ------------------------------------------------------------------ the game
    def _play_game(self) -> None:
        """Play every round, then GAME_OVER. Also used by ghosts (no socket I/O)."""
        self._game_played = True
        try:
            for _ in range(self._state.total_rounds):
                self._play_round()
            self._send_game(p.make_message(p.GAME_OVER,
                                           ranking=self._state.frozen_ranking()))
            if self._connected:
                self._wait_for_close()
        except threading.BrokenBarrierError:
            # Safety net: somebody did not reach a barrier in time. Every
            # worker gets this error at the same moment, so all stop together.
            print(f"[{self.name}] barrier broken, game aborted")
            self._send_game(p.make_message(p.ERROR, reason="game aborted"))
        finally:
            self._state.player_done()

    def _play_round(self) -> None:
        state = self._state
        state.start_barrier.wait()          # everybody ready. Action: pick the question
        info = state.round_info()
        result = self._ask_question(info) if self._connected else None
        state.end_barrier.wait()            # everybody answered or timed out. Action: freeze ranking
        self._send_round_summary(info, result)

    def _ask_question(self, info: RoundInfo) -> Optional[AnswerResult]:
        """Send the question and wait for the answer. None means no answer."""
        self._send_game(p.make_message(
            p.QUESTION, q=info.question_id, number=info.number, total=info.total,
            text=info.text, choices=info.choices, time_limit=info.time_limit))
        if not self._connected:
            return None
        try:
            return self._wait_for_answer(info)
        except (p.ProtocolError, OSError) as exc:
            print(f"[{self.name}] {exc}")
            self._mark_ghost()
            return None
        finally:
            if self._connected:
                self._channel.set_timeout(None)

    def _wait_for_answer(self, info: RoundInfo) -> Optional[AnswerResult]:
        """Wait for a valid ANSWER until the shared deadline of the round."""
        while True:
            remaining = info.deadline - time.monotonic()
            if remaining <= 0:
                return None                          # time is over: counts as no answer
            self._channel.set_timeout(remaining)     # a silent player cannot block the round
            try:
                message = self._channel.recv_message()
            except socket.timeout:
                return None
            if message is None or message["type"] == p.QUIT:
                self._mark_ghost()
                return None
            if message["type"] != p.ANSWER:
                self._send_game(p.make_message(p.ERROR, reason="round in progress"))
                continue
            try:
                # Scoring happens inside the state lock. The lock is released
                # when submit_answer returns, BEFORE we send anything.
                result = self._state.submit_answer(self._player.player_id,
                                                   message.get("q"),
                                                   message.get("choice", ""))
            except RoundError as exc:
                self._send_game(p.make_message(p.ERROR, reason=exc.reason))
                continue
            # PHASE 4: push the "answer" event to the logger queue here
            return result

    def _send_round_summary(self, info: RoundInfo,
                            result: Optional[AnswerResult]) -> None:
        self._send_game(p.make_message(
            p.RESULT, q=info.question_id,
            correct=result.correct if result else False,
            points=result.points if result else 0,
            first=result.first if result else False,
            answer=info.correct_answer))
        self._send_game(p.make_message(p.SCORES,
                                       ranking=self._state.frozen_ranking()))

    def _wait_for_close(self) -> None:
        """Let the client read GAME_OVER and close first.

        If the server closed first while unread data was still pending, the
        operating system could reset the connection and the client could lose
        GAME_OVER.
        """
        self._channel.set_timeout(CLOSE_WAIT)
        try:
            while self._channel.recv_message() is not None:
                pass
        except (p.ProtocolError, OSError):
            pass                                     # timeout or reset: just stop waiting

    # ------------------------------------------------------------------- ghosts
    def _send_game(self, message: dict) -> None:
        """Send during the game. A ghost sends nothing; a failure makes a ghost."""
        if not self._connected:
            return
        try:
            self._channel.send_message(message)
        except OSError:
            self._mark_ghost()

    def _mark_ghost(self) -> None:
        """The client is gone: keep playing the barriers without any I/O."""
        if not self._connected:
            return
        self._connected = False
        if self._player is not None:
            self._state.remove_player(self._player.player_id)   # -> Leave.GHOST
        self._channel.close()