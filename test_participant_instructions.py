"""Cross-version guidance boundaries; never touch real participant homes."""
import fcntl
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

import codex_instructions as legacy
import participant_instructions as guidance


class MigrationTests(unittest.TestCase):
    def test_legacy_module_is_the_same_implementation(self):
        self.assertIs(legacy.update, guidance.update)
        self.assertIs(legacy.section, guidance.section)

    def test_each_legacy_section_migrates_once_and_removes_with_outside_bytes_intact(self):
        for agent in ('codex', 'deepseek'):
            for newline in ('\n', '\r\n'):
                with self.subTest(agent=agent, newline=newline), tempfile.TemporaryDirectory() as d:
                    home = Path(d)
                    path = home / 'AGENTS.md'
                    before, after = 'Personal text.\r\n', 'More personal text.\r\n'
                    begin, end = guidance.LEGACY_MARKERS[agent]
                    old = (begin + 'old runtime\n' + end).replace('\n', newline)
                    path.write_bytes((before + old + after).encode())
                    guidance.update(home, Path('/synthetic/new'), agent=agent)
                    data = path.read_bytes().decode()
                    self.assertTrue(data.startswith(before))
                    self.assertTrue(data.endswith(after))
                    self.assertEqual(data.count('<!-- BEGIN KOINON '), 1)
                    self.assertNotIn('PEER BRIDGE -->', data)
                    self.assertNotIn('old runtime', data)
                    guidance.update(home, Path('/synthetic/new'), agent=agent)
                    self.assertEqual(path.read_bytes(), data.encode())
                    guidance.update(home, Path('/synthetic/new'), agent=agent, remove=True)
                    self.assertEqual(path.read_bytes(), (before + after).encode())

    def test_old_sections_remove_independently(self):
        with tempfile.TemporaryDirectory() as d:
            home = Path(d)
            path = home / 'AGENTS.md'
            sections = {a: b + a + '\n' + e for a, (b, e) in guidance.LEGACY_MARKERS.items()}
            path.write_text('Personal\n' + sections['codex'] + sections['deepseek'])
            guidance.update(home, Path('/synthetic'), remove=True)
            self.assertEqual(path.read_text(), 'Personal\n' + sections['deepseek'])
            guidance.update(home, Path('/synthetic'), remove=True, agent='deepseek')
            self.assertEqual(path.read_text(), 'Personal\n')

    def test_invalid_sections_preserve_both_files_before_any_publish(self):
        cb, ce = guidance.MARKERS['codex']
        db, de = guidance.MARKERS['deepseek']
        lb, le = guidance.LEGACY_MARKERS['codex']
        invalid = (cb + 'truncated', ce + cb, cb + ce + lb + le,
                   cb + db + ce + de, db + 'other participant truncated')
        for broken in invalid:
            with self.subTest(broken=broken), tempfile.TemporaryDirectory() as d:
                home = Path(d)
                base, override = home / 'AGENTS.md', home / 'AGENTS.override.md'
                original = 'Personal\n' + lb + 'valid old\n' + le
                base.write_text(original)
                override.write_text(broken)
                with self.assertRaises(ValueError):
                    guidance.update(home, Path('/synthetic'))
                self.assertEqual(base.read_text(), original)
                self.assertEqual(override.read_text(), broken)
                self.assertFalse(list(home.glob('*.before-*')))
                self.assertFalse(list(home.glob('*.tmp.*')))

    def test_legacy_locks_exclude_cross_participant_update_and_keep_inodes(self):
        real_flock = fcntl.flock
        for held_agent, updating_agent in (('codex', 'deepseek'), ('deepseek', 'codex')):
            with self.subTest(held_agent=held_agent), tempfile.TemporaryDirectory() as d:
                home = Path(d)
                paths = {a: home / n for a, n in guidance.GUIDANCE_LOCK_NAMES.items()}
                for path in paths.values():
                    path.touch(mode=0o600)
                inodes = {a: p.stat().st_ino for a, p in paths.items()}
                reached, finished = threading.Event(), threading.Event()
                errors = []
                def observed_flock(fd, operation):
                    import os
                    if os.fstat(fd).st_ino == inodes[held_agent]:
                        reached.set()
                    return real_flock(fd, operation)
                def update():
                    try:
                        guidance.update(home, Path('/synthetic'), agent=updating_agent)
                    except BaseException as exc:
                        errors.append(exc)
                    finally:
                        finished.set()
                with paths[held_agent].open('a') as held, patch.object(guidance.fcntl, 'flock', observed_flock):
                    real_flock(held.fileno(), fcntl.LOCK_EX)
                    worker = threading.Thread(target=update)
                    worker.start()
                    try:
                        self.assertTrue(reached.wait(5))
                        self.assertFalse(finished.is_set())
                        self.assertFalse((home / 'AGENTS.md').exists())
                    finally:
                        real_flock(held.fileno(), fcntl.LOCK_UN)
                        worker.join(5)
                    self.assertFalse(worker.is_alive())
                    self.assertEqual(errors, [])
                self.assertEqual({a: p.stat().st_ino for a, p in paths.items()}, inodes)
                self.assertTrue((home / 'AGENTS.md').exists())
