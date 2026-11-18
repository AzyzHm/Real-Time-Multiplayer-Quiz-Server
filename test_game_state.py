"""Tests of game_state.py (run: python test_game_state.py)."""

import threading
import unittest

from game_state import (BONUS_FIRST, POINTS_CORRECT, GameState, Leave,
                        LobbyError, Question, RoundError)


class LobbyTest(unittest.TestCase):
    def test_ids_are_unique_and_increasing(self) -> None:
        state = GameState()
        a = state.add_player("Ali", None)
        b = state.add_player("Sana", None)
        self.assertEqual((a.player_id, b.player_id), (1, 2))

    def test_refused_joins(self) -> None:
        state = GameState(max_players=2)
        state.add_player("Ali", None)
        for name, reason in [("  ", "invalid name"), ("x" * 50, "invalid name"),
                             ("ALI", "name already taken")]:
            with self.assertRaises(LobbyError) as ctx:
                state.add_player(name, None)
            self.assertEqual(ctx.exception.reason, reason)
        state.add_player("Sana", None)
        with self.assertRaises(LobbyError) as ctx:
            state.add_player("Eya", None)
        self.assertEqual(ctx.exception.reason, "lobby full")

    def test_version_increases_at_each_change(self) -> None:
        state = GameState()
        versions = [state.lobby_snapshot().version]
        player = state.add_player("Ali", None)
        versions.append(state.lobby_snapshot().version)
        state.remove_player(player.player_id)
        versions.append(state.lobby_snapshot().version)
        self.assertEqual(versions, sorted(set(versions)))

    def test_close_lobby_needs_enough_players(self) -> None:
        state = GameState(min_players=2)
        state.add_player("Ali", None)
        with self.assertRaises(LobbyError):
            state.close_lobby()
        state.add_player("Sana", None)
        self.assertEqual(state.close_lobby(), ["Ali", "Sana"])
        with self.assertRaises(LobbyError):
            state.close_lobby()                  # already closed

    def test_join_refused_after_close(self) -> None:
        state = GameState()
        state.add_player("Ali", None)
        state.add_player("Sana", None)
        state.close_lobby()
        with self.assertRaises(LobbyError) as ctx:
            state.add_player("Eya", None)
        self.assertEqual(ctx.exception.reason, "lobby closed")

    def test_leaving_after_close_makes_a_ghost(self) -> None:
        state = GameState()
        a = state.add_player("Ali", None)
        state.add_player("Sana", None)
        state.close_lobby()
        self.assertIs(state.remove_player(a.player_id), Leave.GHOST)
        self.assertEqual(state.lobby_snapshot().names, ["Sana"])

    def test_leave_outcomes(self) -> None:
        state = GameState()
        a = state.add_player("Ali", None)
        self.assertIs(state.remove_player(a.player_id), Leave.REMOVED)
        self.assertIs(state.remove_player(a.player_id), Leave.NOT_A_PLAYER)


class ConcurrencyTest(unittest.TestCase):
    """Many threads call add_player at the very same moment (start gate: Barrier)."""

    def _run_threads(self, state: GameState, names) -> list:
        gate = threading.Barrier(len(names))
        results = [None] * len(names)            # one slot per thread, no sharing

        def join(i: int) -> None:
            gate.wait()                          # all threads start together
            try:
                results[i] = state.add_player(names[i], None)
            except LobbyError:
                results[i] = None

        threads = [threading.Thread(target=join, args=(i,)) for i in range(len(names))]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        return results

    def test_same_name_accepted_once(self) -> None:
        results = self._run_threads(GameState(max_players=50), ["Ali"] * 30)
        self.assertEqual(sum(r is not None for r in results), 1)

    def test_never_more_players_than_places(self) -> None:
        names = [f"p{i}" for i in range(30)]
        results = self._run_threads(GameState(max_players=6), names)
        accepted = [r for r in results if r is not None]
        self.assertEqual(len(accepted), 6)
        self.assertEqual(len({r.player_id for r in accepted}), 6)


QUESTIONS = [
    Question(1, "First?", {"A": "a", "B": "b", "C": "c"}, "B", 5),
    Question(2, "Second?", {"A": "a", "B": "b", "C": "c"}, "C", 5),
]


def started_game(players: int) -> GameState:
    """A state whose lobby is closed, with `players` players (ids 1..players)."""
    state = GameState(QUESTIONS, max_players=players)
    for i in range(players):
        state.add_player(f"p{i + 1}", None)
    state.close_lobby()
    return state


def pass_barrier(barrier: threading.Barrier, parties: int) -> None:
    """Make `parties` threads reach the barrier, so that its action runs."""
    threads = [threading.Thread(target=barrier.wait) for _ in range(parties)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


class RoundTest(unittest.TestCase):
    def test_barriers_have_one_party_per_player(self) -> None:
        state = started_game(3)
        self.assertEqual(state.start_barrier.parties, 3)
        self.assertEqual(state.end_barrier.parties, 3)

    def test_start_barrier_action_prepares_the_round(self) -> None:
        state = started_game(2)
        pass_barrier(state.start_barrier, 2)
        info = state.round_info()
        self.assertEqual((info.question_id, info.number, info.total), (1, 1, 2))
        pass_barrier(state.end_barrier, 2)
        pass_barrier(state.start_barrier, 2)
        self.assertEqual(state.round_info().question_id, 2)

    def test_scoring_first_correct_gets_the_bonus_once(self) -> None:
        state = started_game(3)
        pass_barrier(state.start_barrier, 3)
        first = state.submit_answer(1, 1, "B")
        second = state.submit_answer(2, 1, "b")          # lower case accepted
        wrong = state.submit_answer(3, 1, "A")
        self.assertEqual((first.points, first.first), (POINTS_CORRECT + BONUS_FIRST, True))
        self.assertEqual((second.points, second.first), (POINTS_CORRECT, False))
        self.assertEqual((wrong.correct, wrong.points), (False, 0))
        pass_barrier(state.end_barrier, 3)
        self.assertEqual(state.frozen_ranking(), [["p1", 150], ["p2", 100], ["p3", 0]])

    def test_bonus_is_reset_for_each_round(self) -> None:
        state = started_game(2)
        for question_id, good in [(1, "B"), (2, "C")]:
            pass_barrier(state.start_barrier, 2)
            self.assertTrue(state.submit_answer(1, question_id, good).first)
            pass_barrier(state.end_barrier, 2)
        self.assertEqual(state.frozen_ranking()[0], ["p1", 300])

    def test_refused_answers(self) -> None:
        state = started_game(2)
        pass_barrier(state.start_barrier, 2)
        with self.assertRaises(RoundError):
            state.submit_answer(1, 99, "B")              # wrong question
        with self.assertRaises(RoundError):
            state.submit_answer(1, 1, "Z")               # invalid choice

    def test_second_answer_does_not_score_twice(self) -> None:
        state = started_game(2)
        pass_barrier(state.start_barrier, 2)
        a = state.submit_answer(1, 1, "B")
        b = state.submit_answer(1, 1, "B")
        self.assertEqual(a, b)
        pass_barrier(state.end_barrier, 2)
        self.assertEqual(state.frozen_ranking()[0], ["p1", 150])

    def test_finished_event_after_every_player_is_done(self) -> None:
        finished = threading.Event()
        state = GameState(QUESTIONS, finished=finished)
        state.add_player("a", None)
        state.add_player("b", None)
        state.close_lobby()
        state.player_done()
        self.assertFalse(finished.is_set())
        state.player_done()
        self.assertTrue(finished.is_set())


class ScoringConcurrencyTest(unittest.TestCase):
    def test_simultaneous_correct_answers_give_exactly_one_bonus(self) -> None:
        """The check-and-set of the bonus and the score updates are atomic."""
        players = 20
        for _ in range(20):                              # repeat to give a race a chance
            state = GameState(QUESTIONS, max_players=players)
            for i in range(players):
                state.add_player(f"p{i}", None)
            state.close_lobby()
            pass_barrier(state.start_barrier, players)

            gate = threading.Barrier(players)
            results = [None] * players

            def answer(i: int) -> None:
                gate.wait()                              # everybody answers at the same instant
                results[i] = state.submit_answer(i + 1, 1, "B")

            threads = [threading.Thread(target=answer, args=(i,)) for i in range(players)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            self.assertEqual(sum(r.first for r in results), 1)
            pass_barrier(state.end_barrier, players)
            total = sum(score for _, score in state.frozen_ranking())
            self.assertEqual(total, players * POINTS_CORRECT + BONUS_FIRST)


if __name__ == "__main__":
    unittest.main()