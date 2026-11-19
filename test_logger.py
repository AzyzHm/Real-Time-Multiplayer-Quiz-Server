"""Tests of logger_process.py and of the logger inside a real game.

Run: python test_logger.py
(The logger process uses the "spawn" start method, so this file must keep its
`if __name__ == "__main__":` guard.)
"""

import os
import tempfile
import threading
import time
import unittest

import protocol as p
from logger_process import GameLogger, GameStats, format_event, format_live_ranking
from server import QuizServer
from test_server import QUESTIONS, Bot


def answer(player, q, correct, first, t, points=100):
    return {"type": "answer", "player": player, "q": q, "correct": correct,
            "points": points, "first": first, "t": t}


class FormatTest(unittest.TestCase):
    def test_answer_line(self) -> None:
        line = format_event(answer("Sana", 3, True, True, 4.2, points=150))
        for part in ("answer", "player=Sana", "q=3", "correct=True", "points=150",
                     "first=True", "t=4.2"):
            self.assertIn(part, line)

    def test_ranking_line(self) -> None:
        line = format_event({"type": "round_end", "q": 1, "ranking": [["Ali", 150], ["Sana", 100]]})
        self.assertIn("ranking=Ali:150,Sana:100", line)

    def test_live_ranking(self) -> None:
        text = format_live_ranking({"number": 2, "total": 10,
                                    "ranking": [["Ali", 300], ["Sana", 200]]})
        self.assertEqual(text, "[leaderboard] after question 2/10: 1. Ali 300 | 2. Sana 200")


class StatsTest(unittest.TestCase):
    def make_stats(self) -> GameStats:
        stats = GameStats()
        for event in [
            {"type": "game_start", "players": ["Ali", "Sana", "Eya"], "questions": 2},
            answer("Ali", 1, True, True, 1.0, 150),
            answer("Sana", 1, False, False, 3.0, 0),
            {"type": "timeout", "player": "Eya", "q": 1},
            answer("Ali", 2, True, True, 2.0, 150),
            answer("Sana", 2, True, False, 4.0),
            {"type": "disconnect", "player": "Eya"},
            {"type": "game_over", "ranking": [["Ali", 300], ["Sana", 100], ["Eya", 0]]},
        ]:
            stats.add(event)
        return stats

    def test_report(self) -> None:
        text = "\n".join(self.make_stats().report())
        self.assertIn("1. Ali: 300", text)
        self.assertIn("Ali: 2 answers, 2 correct (100%), average response 1.50 s, "
                      "fastest correct 1.00 s, 2 first bonus(es), 0 timeout(s)", text)
        self.assertIn("Sana: 2 answers, 1 correct (50%), average response 3.50 s, "
                      "fastest correct 4.00 s, 0 first bonus(es), 0 timeout(s)", text)
        self.assertIn("Eya: 0 answers, 0 correct (0%), average response n/a, "
                      "fastest correct n/a, 0 first bonus(es), 1 timeout(s), disconnected", text)
        # question 1: 3 asked (2 answers + 1 timeout), 1 correct. Question 2: 2 asked, 2 correct
        self.assertIn("Hardest question: id 1 (1/3 correct)", text)

    def test_players_are_listed_in_ranking_order(self) -> None:
        lines = self.make_stats().report()
        names = [l.split(":")[0].strip() for l in lines if "answers," in l]
        self.assertEqual(names, ["Ali", "Sana", "Eya"])


class LoggerProcessTest(unittest.TestCase):
    def setUp(self) -> None:
        self.folder = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.folder.name, "game.log")
        self.logger = GameLogger(self.path)

    def tearDown(self) -> None:
        self.logger.stop()
        self.folder.cleanup()

    def lines(self):
        with open(self.path, encoding="utf-8") as file:
            return file.read().splitlines()

    def test_events_are_written_in_order_and_the_sentinel_ends_the_process(self) -> None:
        self.logger.start()
        for n in range(500):
            self.logger.publish({"type": "join", "player": f"p{n}", "n": n})
        self.logger.stop()

        self.assertEqual(self.logger.exitcode, 0)
        lines = self.lines()
        self.assertIn("log started", lines[0])
        self.assertIn("log closed", lines[-1])
        body = lines[1:-1]
        self.assertEqual(len(body), 500)                       # nothing lost: None was last
        self.assertTrue(all(f"n={n}" in line for n, line in enumerate(body)))

    def test_report_after_game_over(self) -> None:
        self.logger.start()
        for event in [{"type": "game_start", "players": ["Ali"], "questions": 1},
                      answer("Ali", 1, True, True, 1.0, 150),
                      {"type": "round_end", "q": 1, "number": 1, "total": 1,
                       "ranking": [["Ali", 150]]},
                      {"type": "game_over", "ranking": [["Ali", 150]]}]:
            self.logger.publish(event)
        self.logger.stop()
        text = "\n".join(self.lines())
        self.assertEqual(text.count("=== Final statistics ==="), 1)
        self.assertIn("1. Ali: 150", text)

    def test_report_when_the_game_was_interrupted(self) -> None:
        self.logger.start()
        self.logger.publish({"type": "game_start", "players": ["Ali"], "questions": 3})
        self.logger.publish(answer("Ali", 1, True, True, 1.0, 150))
        self.logger.stop()                                     # no game_over was sent
        self.assertEqual("\n".join(self.lines()).count("=== Final statistics ==="), 1)

    def test_no_report_without_a_game(self) -> None:
        self.logger.start()
        self.logger.publish({"type": "join", "player": "Ali"})
        self.logger.stop()
        self.assertNotIn("Final statistics", "\n".join(self.lines()))

    def test_log_file_is_appended(self) -> None:
        for _ in range(2):
            logger = GameLogger(self.path)
            logger.start()
            logger.publish({"type": "join", "player": "Ali"})
            logger.stop()
        self.assertEqual("\n".join(self.lines()).count("log started"), 2)

    def test_unwritable_log_fails_early_in_the_parent(self) -> None:
        bad = GameLogger(os.path.join(self.folder.name, "missing_folder", "game.log"))
        with self.assertRaises(OSError):
            bad.start()

    def test_stop_twice_and_publish_after_stop_are_harmless(self) -> None:
        self.logger.start()
        self.logger.stop()
        self.logger.stop()
        self.logger.publish({"type": "join", "player": "late"})   # ignored, no exception

    def test_dead_logger_does_not_block_stop(self) -> None:
        """If the logger process died, stop() must not hang on the full pipe."""
        self.logger.start()
        self.logger._process.terminate()
        self.logger._process.join()
        for n in range(3000):                                  # more than a pipe can hold
            self.logger.publish({"type": "join", "player": "x" * 100, "n": n})

        stopper = threading.Thread(target=self.logger.stop, kwargs={"timeout": 2})
        stopper.start()
        stopper.join(15)
        self.assertFalse(stopper.is_alive(), "stop() is blocked")


class LoggedGameTest(unittest.TestCase):
    """A complete game over real sockets, with the real logger process."""

    def run_game(self, *bots: Bot) -> str:
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        path = os.path.join(folder.name, "game.log")
        server = QuizServer("127.0.0.1", 0, QUESTIONS, GameLogger(path))
        server.start()
        try:
            for bot in bots:
                bot.port = server.port
                bot.start()
                self.assertTrue(bot.joined.wait(5))
            self.assertTrue(server.start_game())
            self.assertTrue(server.wait_until_done(15), "the game did not finish")
            for bot in bots:
                bot.join(5)
        finally:
            server.stop()                      # joins the workers, then stops the logger
        with open(path, encoding="utf-8") as file:
            return file.read()

    def kinds(self, text: str, kind: str) -> int:
        return sum(1 for line in text.splitlines() if f"] {kind:<10} " in line)

    def test_a_game_is_fully_logged(self) -> None:
        text = self.run_game(Bot(0, "Ali"), Bot(0, "Sana", delay=0.3))
        self.assertEqual(self.kinds(text, "join"), 2)
        self.assertEqual(self.kinds(text, "game_start"), 1)
        self.assertEqual(self.kinds(text, "answer"), 4)        # 2 players x 2 questions
        self.assertEqual(self.kinds(text, "round_end"), 2)
        self.assertEqual(self.kinds(text, "game_over"), 1)
        self.assertEqual(text.count("=== Final statistics ==="), 1)
        self.assertIn("1. Ali: 300", text)
        self.assertIn("player=Ali q=1 correct=True points=150 first=True", text)
        # the order of the log follows the order of the game
        order = [l.split("]")[1].split()[0] for l in text.splitlines() if l.startswith("[")]
        self.assertEqual(order[0], "join")
        self.assertEqual(order[-1], "game_over")

    def test_timeouts_and_disconnections_are_logged(self) -> None:
        text = self.run_game(Bot(0, "Ali"), Bot(0, "Silent", answers={}),
                             Bot(0, "Leaver", leave_after_question=True))
        self.assertEqual(self.kinds(text, "disconnect"), 1)
        self.assertEqual(self.kinds(text, "timeout"), 2)       # Silent, on both questions
        self.assertIn("player=Leaver", text)
        self.assertIn("disconnected", text)                    # in the final statistics


if __name__ == "__main__":
    unittest.main()