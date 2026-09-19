import fcntl
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import platform_support
import session_observation as observation


class LifecycleObservationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def test_missing_endpoint_and_unheld_locks_are_stopped_without_creating_files(self):
        before = list(self.root.iterdir())
        self.assertEqual(observation.lifecycle(self.root, None), 'stopped')
        self.assertEqual(list(self.root.iterdir()), before)
        self.assertEqual(observation.lifecycle(self.root, dict(pid=123)), 'running')

    def test_unresponsive_or_stale_endpoint_is_unknown_not_stopped(self):
        path = platform_support.control_socket_path(self.root)
        path.touch(mode=0o600)
        self.assertEqual(observation.lifecycle(self.root, None), 'unknown')
        path.unlink()

    def test_orphan_notifier_and_active_supervisor_are_not_silently_stopped(self):
        for filename in ('notifier.lock', 'supervisor.lock'):
            path = self.root / filename
            path.touch(mode=0o600)
            with path.open('rb') as stream:
                fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
                self.assertEqual(observation.lifecycle(self.root, None), 'unknown')
            self.assertEqual(observation.lifecycle(self.root, None), 'stopped')

    def test_probe_failure_is_unknown(self):
        with mock.patch.object(observation, 'endpoint_present', side_effect=PermissionError):
            self.assertEqual(observation.lifecycle(self.root, None), 'unknown')
