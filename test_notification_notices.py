from pathlib import Path
import shlex
import sys
import unittest
from unittest import mock
from notification_provider import MAX_NOTICE_BYTES

from participant_lock import identity
import notification_notices as notices


class NoticeTests(unittest.TestCase):
    def setUp(self):
        self.participant = identity('codex', 'synthetic-participant')
        self.binding = dict(binding='a' * 64, binding_instance='b' * 32,
            memory_state_dir='/synthetic/custom memory', repo_path='/synthetic/repo/.git',
            repo_key='c' * 16, observed_store='SECRET STORE', observed_head=987654321,
            body='SECRET MEMORY BODY')
        self.row = dict(seq=9, kind='memory-pointer', binding='a' * 64, binding_instance='b' * 32)

    def test_memory_notice_has_exact_quoted_root_and_no_memory_payload_or_head(self):
        text = notices.render([self.row], '/synthetic/bridge', self.participant, {'a' * 64: self.binding})
        expected = shlex.join([sys.executable, str(Path(notices.__file__).resolve().with_name('memory.py')),
            '--service-dir', self.binding['memory_state_dir'], '--repo-path', self.binding['repo_path'],
            '--consumer', notices.consumer_key(self.participant, self.binding['repo_key']), 'sync'])
        self.assertIn(expected, text)
        for secret in ('SECRET STORE', 'SECRET MEMORY BODY', '987654321', 'synthetic-participant'):
            self.assertNotIn(secret, text)
        self.assertIn('grants no permission', text)
        self.assertIn('acknowledge only after processing', text)

    def test_consumer_is_stable_and_scoped_to_participant_provider_and_repository(self):
        first = notices.consumer_key(self.participant, 'c' * 16)
        self.assertEqual(first, notices.consumer_key(dict(self.participant), 'c' * 16))
        self.assertNotEqual(first, notices.consumer_key(identity('deepseek', 'synthetic-participant'), 'c' * 16))
        self.assertNotEqual(first, notices.consumer_key(identity('codex', 'other-synthetic'), 'c' * 16))
        self.assertNotEqual(first, notices.consumer_key(self.participant, 'd' * 16))
        self.assertLessEqual(len(first), 128)

    def test_rebound_or_removed_binding_cannot_route_an_old_pointer(self):
        for bindings in ({}, {'a' * 64: dict(self.binding, binding_instance='e' * 32)}):
            with self.subTest(bindings=bool(bindings)), self.assertRaises(LookupError):
                notices.render([self.row], '/synthetic/bridge', self.participant, bindings)

    def test_ordinary_notice_omits_all_extra_row_content(self):
        rows = [dict(seq=3, kind='peer', body='SECRET PEER BODY'), dict(seq=5, kind='peer')]
        text = notices.render(rows, '/synthetic/bridge', self.participant, {})
        self.assertIn('2 new peer message(s), through sequence 5', text)
        self.assertIn('--after 2', text)
        self.assertNotIn('SECRET PEER BODY', text)
        self.assertIn('permission laundering', text)

    def test_mixed_or_duplicate_groups_are_refused(self):
        for rows in ([self.row, dict(seq=10, kind='peer')],
                     [dict(seq=1, kind='peer'), dict(seq=1, kind='peer')]):
            with self.assertRaises(ValueError):
                notices.render(rows, '/synthetic/bridge', self.participant, {'a' * 64: self.binding})

    def test_maximum_quoted_paths_fit_the_provider_notice_budget(self):
        long_path = '/' + '/'.join(["'" * 240] * 16)
        binding = dict(self.binding, repo_path=long_path, memory_state_dir=long_path)
        with mock.patch.object(notices, '__file__', long_path + '/notification_notices.py'):
            text = notices.render([self.row], long_path, self.participant, {'a' * 64: binding})
        self.assertLessEqual(len(text.encode()), MAX_NOTICE_BYTES)
        self.assertGreater(len(text.encode()), 16 * 1024)
