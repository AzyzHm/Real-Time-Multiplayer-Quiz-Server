"""Tests of game_state.py (run: python test_game_state.py)."""

import threading
import unittest

from game_state import GameState, LobbyError


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
        self.assertFalse(state.remove_player(a.player_id))   # list not changed for clients
        self.assertEqual(state.lobby_snapshot().names, ["Sana"])


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


if __name__ == "__main__":
    unittest.main()