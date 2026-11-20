"""Shared game state of the quiz: lobby, questions, rounds, scores.

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

Barriers (threading.Barrier) are created when the lobby closes, with one
party per player. Their `action` runs in ONE thread (the last one to arrive)
before the others are released. The actions below take the lock but never
send anything.
"""

import contextlib
import json
import threading
import time
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Dict, Iterable, List, NamedTuple, Optional, Set, Tuple

from protocol import MessageChannel

MIN_PLAYERS = 2
MAX_PLAYERS = 6
MAX_NAME_LENGTH = 20

POINTS_CORRECT = 100     # correct answer
BONUS_FIRST = 50         # extra points for the first correct answer of a round

# UNSAFE demo mode only: pause (seconds) placed between "check" and "write" in the
# scoring, to make the race condition easy to see (the course example needed
# thousands of iterations; here a tiny sleep is enough).
RACE_DELAY = 0.001

# A barrier gives up (BrokenBarrierError) after the longest question time limit
# plus this margin. The margin must be larger than the time a client may need
# to answer READY (the first start barrier waits for every player).
BARRIER_MARGIN = 20.0


class LobbyError(Exception):
    """A join or a start was refused. `reason` is sent to the client as is."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class RoundError(Exception):
    """An answer was refused (wrong question, invalid choice)."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class Leave(Enum):
    """What happened when a player left (decided atomically under the lock)."""
    NOT_A_PLAYER = 1
    REMOVED = 2        # the lobby was open: the player simply disappeared
    GHOST = 3          # the game has started: the player stays as a "ghost"


@dataclass
class Question:
    id: int
    text: str
    choices: Dict[str, str]
    answer: str
    time_limit: float


def load_questions(path: str) -> List[Question]:
    """Read and check the questions file. Raises ValueError if it is invalid."""
    with open(path, encoding="utf-8") as file:
        raw = json.load(file)
    questions: List[Question] = []
    for item in raw:
        try:
            question = Question(id=int(item["id"]), text=str(item["text"]),
                                choices=dict(item["choices"]),
                                answer=str(item["answer"]).upper(),
                                time_limit=float(item["time_limit"]))
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"invalid question {item!r}: {exc}") from exc
        if question.answer not in question.choices or question.time_limit <= 0:
            raise ValueError(f"invalid question {item!r}")
        questions.append(question)
    if len({q.id for q in questions}) != len(questions):
        raise ValueError("question ids must be unique")
    return questions


@dataclass
class Player:
    """One participant. `connected` becomes False for a "ghost" player."""
    player_id: int
    name: str
    channel: MessageChannel
    connected: bool = True
    score: int = 0


class LobbySnapshot(NamedTuple):
    """A consistent copy of the lobby, safe to use after the lock is released."""
    version: int                       # increases at every lobby change
    is_open: bool
    names: List[str]
    channels: List[MessageChannel]


class RoundInfo(NamedTuple):
    """What a worker needs to run the current round (copy, no lock needed)."""
    question_id: int
    number: int                        # 1-based position of the question
    total: int
    text: str
    choices: Dict[str, str]
    time_limit: float
    correct_answer: str                # never put in a QUESTION message
    deadline: float                    # time.monotonic() value, same for everybody


class AnswerResult(NamedTuple):
    correct: bool
    points: int
    first: bool                        # got the "first correct answer" bonus
    correct_answer: str
    response_time: float               # seconds since the round started


class GameState:
    """Players, lobby status, questions, scores and round data."""

    def __init__(self, questions: Optional[Iterable[Question]] = None,
                 min_players: int = MIN_PLAYERS, max_players: int = MAX_PLAYERS,
                 finished: Optional[threading.Event] = None,
                 publish: Optional[Callable[[dict], None]] = None,
                 safe: bool = True) -> None:
        self._lock = threading.Lock()          # the ONE lock of the game
        # Same lock: a Condition built on it adds no second lock. Used to wait
        # for lobby changes without busy waiting.
        self._lobby_changed = threading.Condition(self._lock)
        self._publish = publish                # sends an event to the logger process
        # safe=False is the DEMO of the race condition: the critical section of
        # submit_answer then runs without any lock.
        self._safe = safe
        self._scoring_guard = self._lock if safe else contextlib.nullcontext()
        self._min_players = min_players
        self._max_players = max_players
        self._questions: List[Question] = list(questions or [])
        self._finished = finished if finished is not None else threading.Event()

        self._players: Dict[int, Player] = {}  # player_id -> Player, in join order
        self._ready: Set[int] = set()          # players who answered READY
        self._next_id = 1
        self._lobby_open = True
        self._lobby_version = 0

        # Round data, reset by the start barrier action
        self._next_index = 0                   # index of the next question to ask
        self._current: Optional[Question] = None
        self._round_started = 0.0
        self._answers: Dict[int, AnswerResult] = {}   # player_id -> result of this round
        self._first_correct_id: Optional[int] = None  # who took the bonus
        self._frozen_ranking: List[list] = []  # ranking taken by the end barrier action

        self._start_barrier: Optional[threading.Barrier] = None
        self._end_barrier: Optional[threading.Barrier] = None
        self._done_count = 0                   # players whose worker finished the game

    # ------------------------------------------------------------ read-only
    @property
    def total_rounds(self) -> int:
        return len(self._questions)            # never modified after __init__

    @property
    def start_barrier(self) -> threading.Barrier:
        """Created by close_lobby(). Workers read it after the lobby closed."""
        assert self._start_barrier is not None, "lobby not closed yet"
        return self._start_barrier

    @property
    def end_barrier(self) -> threading.Barrier:
        assert self._end_barrier is not None, "lobby not closed yet"
        return self._end_barrier

    def is_started(self) -> bool:
        with self._lock:
            return not self._lobby_open

    def publish_event(self, event: dict) -> None:
        """Hand an event to the logger process (if there is one).

        Deliberately NOT under the lock: callers build the event under the
        lock if needed, release it, then publish. The callback only puts a
        picklable dict in an mp.Queue (thread-safe, does not block).
        """
        if self._publish is not None:
            self._publish(event)

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
            self._lobby_changed.notify_all()     # wake up wait_for_players()
            return player

    def remove_player(self, player_id: int) -> Leave:
        """A player left. The outcome is decided under the lock.

        While the lobby is open the player is simply removed. Once the game
        has started the player is only marked disconnected (a "ghost"): its
        worker keeps calling the barriers, which need a fixed number of parties.
        """
        with self._lock:
            player = self._players.get(player_id)
            if player is None:
                return Leave.NOT_A_PLAYER
            if self._lobby_open:
                del self._players[player_id]
                self._lobby_version += 1
                self._lobby_changed.notify_all()
                return Leave.REMOVED
            player.connected = False
            return Leave.GHOST

    def close_lobby(self) -> List[str]:
        """Close the lobby (host starts the game) and create the two barriers.

        Returns the player names. Raises LobbyError if already closed or if
        there are too few players. After this call the number of parties is
        fixed: players can no longer be removed, only turned into ghosts.
        """
        with self._lock:
            if not self._lobby_open:
                raise LobbyError("lobby already closed")
            if len(self._players) < self._min_players:
                raise LobbyError(f"need at least {self._min_players} players")

            parties = len(self._players)
            longest = max((q.time_limit for q in self._questions), default=0.0)
            timeout = longest + BARRIER_MARGIN
            self._start_barrier = threading.Barrier(parties, self._begin_round, timeout)
            self._end_barrier = threading.Barrier(parties, self._end_round, timeout)

            self._lobby_open = False
            self._lobby_version += 1
            self._lobby_changed.notify_all()
            return [p.name for p in self._players.values()]

    def wait_for_players(self, count: int) -> bool:
        """Block until `count` players are in the lobby. Used by --autostart.

        Returns True if the count was reached while the lobby is still open,
        False if the lobby was closed first (the host started the game).

        Condition pattern from the course: the lock is held, wait() releases
        it while sleeping, and the test is in a `while` (not an `if`), because
        the state may have changed again when the thread wakes up.
        """
        with self._lobby_changed:
            while self._lobby_open and len(self._players) < count:
                self._lobby_changed.wait()
            return self._lobby_open

    def mark_ready(self, player_id: int) -> None:
        """The player's client answered READY."""
        with self._lock:
            self._ready.add(player_id)

    def unready_players(self) -> List[Tuple[str, MessageChannel]]:
        """Connected players who did not answer READY (name, channel)."""
        with self._lock:
            return [(p.name, p.channel) for p in self._players.values()
                    if p.connected and p.player_id not in self._ready]

    def abort_game(self) -> None:
        """Break both barriers: every worker waiting on one gets
        BrokenBarrierError at once, instead of waiting for the barrier timeout.
        Used when a worker thread crashes."""
        for barrier in (self._start_barrier, self._end_barrier):
            if barrier is not None:
                barrier.abort()

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

    # ------------------------------------------------------------ barrier actions
    def _begin_round(self) -> None:
        """Action of start_barrier: pick the question, reset the round data.

        Runs in ONE thread, before any worker is released. Takes the lock,
        sends nothing.
        """
        with self._lock:
            self._current = self._questions[self._next_index]
            self._next_index += 1
            self._answers = {}
            self._first_correct_id = None
            self._round_started = time.monotonic()
            event = {"type": "round_start", "q": self._current.id,
                     "number": self._next_index, "total": len(self._questions),
                     "time_limit": self._current.time_limit}
        self.publish_event(event)               # after the lock is released

    def _end_round(self) -> None:
        """Action of end_barrier: freeze the ranking for this round.

        Every worker reads the same frozen ranking, whatever the speed of the
        other workers. Takes the lock, sends nothing.
        """
        with self._lock:
            self._frozen_ranking = self._ranking()
            event = {"type": "round_end", "q": self._current.id,
                     "number": self._next_index, "total": len(self._questions),
                     "ranking": [entry[:] for entry in self._frozen_ranking]}
        self.publish_event(event)               # after the lock is released

    # ------------------------------------------------------------ rounds
    def round_info(self) -> RoundInfo:
        """Copy of the data of the current round (call after start_barrier)."""
        with self._lock:
            question = self._current
            assert question is not None, "no round in progress"
            return RoundInfo(question.id, self._next_index, len(self._questions),
                             question.text, dict(question.choices),
                             question.time_limit, question.answer,
                             self._round_started + question.time_limit)

    def submit_answer(self, player_id: int, question_id: int,
                      choice: str) -> AnswerResult:
        """Score one answer. Raises RoundError if it is refused.

        CRITICAL SECTION (lock held): everything between reading the round
        data and writing the score, in particular "has somebody already taken
        the bonus? if not, take it", is ONE atomic block. Otherwise two
        players answering at the same instant could both get the bonus, and
        two score updates could overwrite each other (lost update, like
        `cpt = cpt + 1`).
        The lock is released before the caller sends anything.

        In the --unsafe demo the guard is NOT the lock, and a tiny pause widens
        the gap between the check and the write (see _race_window).
        """
        with self._scoring_guard:
            question = self._current
            if question is None or question_id != question.id:
                raise RoundError("wrong question")
            previous = self._answers.get(player_id)
            if previous is not None:
                return previous                # one answer per player and round
            choice = str(choice).upper()
            if choice not in question.choices:
                raise RoundError("invalid choice")

            correct = choice == question.answer
            points = 0
            first = False
            if correct:
                points = POINTS_CORRECT
                if self._first_correct_id is None:       # check ...
                    self._race_window()                  # (no-op in safe mode)
                    self._first_correct_id = player_id   # ... and set, atomically
                    points += BONUS_FIRST
                    first = True
                self._players[player_id].score += points

            result = AnswerResult(correct, points, first, question.answer,
                                  time.monotonic() - self._round_started)
            self._answers[player_id] = result
            return result

    def _race_window(self) -> None:
        """UNSAFE demo only: sleep between "check" and "set" so that several
        threads pass the check before any of them sets the marker, and several
        players get the "first" bonus. In safe mode this does nothing."""
        if not self._safe:
            time.sleep(RACE_DELAY)

    def frozen_ranking(self) -> List[list]:
        """Ranking taken by the last end barrier action: [[name, score], ...]."""
        with self._lock:
            return [entry[:] for entry in self._frozen_ranking]

    def _ranking(self) -> List[list]:
        """Players sorted by score (ties keep the join order). Lock must be held."""
        ordered = sorted(self._players.values(), key=lambda p: -p.score)
        return [[p.name, p.score] for p in ordered]

    # ------------------------------------------------------------ end of game
    def player_done(self) -> None:
        """A worker finished its game. The last one ends the game.

        The last worker publishes "game_over" BEFORE setting `finished`. The
        server joins every worker before it stops the logger, so this event
        always reaches the queue before the end sentinel.
        """
        with self._lock:
            self._done_count += 1
            last = self._done_count >= len(self._players)
            ranking = [e[:] for e in (self._frozen_ranking or self._ranking())]
        if last:
            self.publish_event({"type": "game_over", "ranking": ranking})
            self._finished.set()