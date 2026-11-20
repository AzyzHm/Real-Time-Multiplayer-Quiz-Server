"""Tests of bots.py: the bonus check, the race demo and a stress test.

Run: python test_bots.py
"""

import threading
import time
import unittest
from types import SimpleNamespace

from bots import BotThread, find_bonus_anomalies
from server import QuizServer
from test_server import GOOD, QUESTIONS


def fake_bot(*results):
    """A bot with the given (q, correct, first) results."""
    return SimpleNamespace(results=[{"q": q, "correct": c, "first": f} for q, c, f in results])


class AnomalyCheckTest(unittest.TestCase):
    def test_one_bonus_per_question_is_fine(self) -> None:
        bots = [fake_bot((1, True, True), (2, False, False)),
                fake_bot((1, True, False), (2, False, False))]
        self.assertEqual(find_bonus_anomalies(bots), [])

    def test_two_bonuses_are_detected(self) -> None:
        bots = [fake_bot((1, True, True)), fake_bot((1, True, True))]
        anomalies = find_bonus_anomalies(bots)
        self.assertEqual(len(anomalies), 1)
        self.assertIn("question 1: 2 first bonuses for 2 correct answers", anomalies[0])

    def test_a_bonus_without_correct_answer_is_detected(self) -> None:
        self.assertEqual(len(find_bonus_anomalies([fake_bot((1, False, True))])), 1)

    def test_no_bonus_although_somebody_was_right_is_detected(self) -> None:
        self.assertEqual(len(find_bonus_anomalies([fake_bot((1, True, False))])), 1)


def play_game(test: unittest.TestCase, players: int, safe: bool, sync: bool,
              accuracy: float = 1.0, delay_max: float = 0.0):
    """One whole game with `players` bots against a real server (no logger)."""
    server = QuizServer("127.0.0.1", 0, QUESTIONS, max_players=players,
                        autostart=players, safe=safe)
    server.start()
    test.addCleanup(server.stop)
    gate = threading.Barrier(players) if sync else None
    bots = [BotThread(f"bot{i}", "127.0.0.1", server.port, GOOD, accuracy,
                      delay_max, gate) for i in range(players)]
    for bot in bots:
        bot.start()
        time.sleep(0.01)
    test.assertTrue(server.wait_until_done(30), "the game did not finish")
    for bot in bots:
        bot.join(10)
    test.assertEqual([b.error for b in bots if b.error], [])
    test.assertTrue(all(b.final_ranking is not None for b in bots))
    return bots


class RaceDemoTest(unittest.TestCase):
    def test_unsafe_server_gives_several_bonuses(self) -> None:
        """The key demo: without the lock, simultaneous answers all get the bonus."""
        for _ in range(3):                                 # normally the first game is enough
            anomalies = find_bonus_anomalies(play_game(self, 12, safe=False, sync=True))
            if anomalies:
                break
        self.assertTrue(anomalies, "no race condition seen")

    def test_safe_server_gives_exactly_one_bonus(self) -> None:
        anomalies = find_bonus_anomalies(play_game(self, 12, safe=True, sync=True))
        self.assertEqual(anomalies, [])


class StressTest(unittest.TestCase):
    def test_many_bots_with_random_answers(self) -> None:
        bots = play_game(self, 30, safe=True, sync=False, accuracy=0.7, delay_max=0.3)
        self.assertEqual(find_bonus_anomalies(bots), [])

        # every bot received the same final ranking, with all the players in it
        rankings = {tuple(map(tuple, b.final_ranking)) for b in bots}
        self.assertEqual(len(rankings), 1)
        ranking = bots[0].final_ranking
        self.assertEqual(len(ranking), 30)

        # the sum of the final scores matches what the bots were told, round by round
        expected = 0
        for question in QUESTIONS:
            results = [r for b in bots for r in b.results if r["q"] == question.id]
            self.assertEqual(len(results), 30)             # one result per bot and round
            expected += sum(r["points"] for r in results)
        self.assertEqual(sum(score for _, score in ranking), expected)


if __name__ == "__main__":
    unittest.main()