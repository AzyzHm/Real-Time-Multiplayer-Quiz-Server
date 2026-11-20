"""Tests of host_display.py (run: python test_host_display.py)."""

import contextlib
import io
import threading
import time
import unittest

from host_display import HostDisplay


def display_threads():
    return [t for t in threading.enumerate() if t.name == "countdown"]


class HostDisplayTest(unittest.TestCase):
    def run_display(self, actions) -> str:
        """Start a display, run `actions(display)`, close it, return what it printed."""
        out = io.StringIO()
        display = HostDisplay()
        with contextlib.redirect_stdout(out):
            display.start()
            actions(display)
            display.close()
        self.assertEqual(display_threads(), [])            # the thread is gone
        return out.getvalue()

    def test_counts_down_then_erases_its_line(self) -> None:
        def actions(display: HostDisplay) -> None:
            display.start_round(2, 10, 1.3)
            time.sleep(1.6)                                # the round runs out by itself

        text = self.run_display(actions)
        self.assertIn("[countdown] question 2/10:  2 s left", text)
        self.assertIn("[countdown] question 2/10:  1 s left", text)
        self.assertTrue(text.endswith("\r"))               # the line was erased

    def test_end_round_erases_the_line_before_the_time_is_over(self) -> None:
        def actions(display: HostDisplay) -> None:
            display.start_round(1, 3, 30)
            time.sleep(0.5)
            display.end_round()
            time.sleep(0.5)

        text = self.run_display(actions)
        self.assertIn("question 1/3: 30 s left", text)
        self.assertNotIn("question 1/3: 29 s left", text)  # it did not keep counting
        self.assertTrue(text.endswith("\r"))

    def test_next_round_replaces_the_previous_one_on_the_same_thread(self) -> None:
        seen = []

        def actions(display: HostDisplay) -> None:
            display.start_round(1, 3, 30)
            time.sleep(0.4)
            seen.append(len(display_threads()))
            display.end_round()
            display.start_round(2, 3, 30)                  # the next round follows at once
            time.sleep(0.4)
            seen.append(len(display_threads()))

        text = self.run_display(actions)
        self.assertEqual(seen, [1, 1])                     # always ONE display thread
        self.assertIn("question 2/3", text)

    def test_close_returns_quickly_and_is_repeatable(self) -> None:
        display = HostDisplay()
        with contextlib.redirect_stdout(io.StringIO()):
            display.start()
            display.start_round(1, 1, 30)
            started = time.monotonic()
            display.close()
            display.close()                                # second call: harmless
        self.assertLess(time.monotonic() - started, 1.0)

    def test_nothing_is_printed_between_rounds(self) -> None:
        text = self.run_display(lambda display: time.sleep(0.5))
        self.assertEqual(text, "")


if __name__ == "__main__":
    unittest.main()