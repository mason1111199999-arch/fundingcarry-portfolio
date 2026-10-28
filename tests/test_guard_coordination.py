"""Offline regression tests for coordination; no live orders or credentials."""
import sys
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "v4_demo"))
import fundingcarryv4_server as v4


class GuardCoordinationTests(unittest.TestCase):
    def test_guard_waits_until_submission_finishes(self):
        entered, release, observed = threading.Event(), threading.Event(), threading.Event()
        def submission(_payload):
            entered.set()
            if not release.wait(2):
                raise TimeoutError("test timed out")
        def cycle(_client):
            observed.set()
        with patch.object(v4, "_submit_preflight_coordinated", submission), patch.object(v4, "_guard_cycle_coordinated", cycle):
            submit = threading.Thread(target=v4.submit_preflight, args=({},))
            guard = threading.Thread(target=v4._guard_cycle, args=(None,))
            submit.start()
            self.assertTrue(entered.wait(2))
            guard.start()
            try:
                self.assertFalse(observed.wait(0.1))
            finally:
                release.set()
                submit.join(2)
                guard.join(2)
            self.assertTrue(observed.is_set())
            self.assertFalse(submit.is_alive())
            self.assertFalse(guard.is_alive())

    def test_public_submission_remains_disabled(self):
        with self.assertRaises(PermissionError):
            v4.submit_preflight({})


if __name__ == "__main__":
    unittest.main()
