from contextlib import closing
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest import mock

import durable_state
import notification_journal as journal
import notification_migration as migration
from participant_lock import identity
from test_notification_journal import work

PARTICIPANT = 'synthetic-session'
TARGET = identity('codex', PARTICIPANT)['digest']


class MigrationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.marker_path = self.root / 'notify-migration.json'
        self.cursor_path = self.root / 'notify-cursor.json'

    def open(self, *, capable=True, activation=None, after=5, participant=PARTICIPANT):
        return migration.Migration(self.root, 'codex', participant, after,
                                   capable=capable, activation=activation)

    def evidence(self, owner):
        return dict(target_digest=TARGET, nonce=owner.marker['nonce'])

    def complete(self):
        owner = self.open()
        evidence = self.evidence(owner)
        owner.confirm_activation(evidence)
        owner.store.seed_pointers(dict(records=[]))
        return owner, evidence

    def test_operational_error_preserves_preparing_state_for_retry(self):
        fault = sqlite3.OperationalError('synthetic lock contention')
        fault.sqlite_errorcode = sqlite3.SQLITE_BUSY
        with mock.patch.object(migration.journal, 'Journal', side_effect=fault):
            with self.assertRaises(sqlite3.OperationalError) as raised:
                self.open()
        self.assertIs(raised.exception, fault)
        before = durable_state.read(self.marker_path)
        self.assertEqual(before['state'], 'preparing')
        with closing(self.open()) as owner:
            self.assertEqual(owner.marker['nonce'], before['nonce'])
            self.assertEqual(owner.marker['through'], before['through'])
            self.assertEqual(owner.marker['state'], 'ready')

    def test_legacy_checkpoint_is_imported_and_guarded_without_advancement(self):
        durable_state.publish(self.cursor_path, dict(thread=PARTICIPANT, through=17))
        with closing(self.open(after=0)) as owner:
            self.assertEqual(owner.store.meta()['scan_through'], 17)
            self.assertEqual(owner.marker['state'], 'ready')
            self.assertFalse(owner.store.meta()['activation_confirmed'])
            self.assertEqual(owner.activation_request,
                dict(op='activate-notification-journal', **self.evidence(owner)))
            self.assertEqual(durable_state.read(self.cursor_path),
                dict(thread=PARTICIPANT, through=17, journal_required=True))

    def test_missing_legacy_uses_explicit_initial_position(self):
        with closing(self.open(after=23)) as owner:
            self.assertEqual(owner.store.meta()['scan_through'], 23)
        self.assertEqual(durable_state.read(self.cursor_path)['through'], 23)

    def test_invalid_legacy_identity_or_counter_prevents_any_marker(self):
        for value in (dict(thread='other', through=1), dict(thread=PARTICIPANT, through=True),
                      dict(thread=PARTICIPANT, through=-1), dict(thread=PARTICIPANT, through=1 << 63)):
            with self.subTest(value=value):
                durable_state.publish(self.cursor_path, value)
                with self.assertRaises(journal.JournalError):
                    self.open()
                self.assertFalse(self.marker_path.exists())
                self.assertFalse(migration.sqlite_files(self.root)[0].exists())

    def test_foreign_target_is_refused_in_every_marker_state(self):
        for state in ('preparing', 'rebuilding', 'ready'):
            with self.subTest(state=state):
                value = dict(version=1, state=state, target_digest='a' * 64, nonce='b' * 32,
                             through=7, history_lost=state == 'rebuilding', previous_nonce=None)
                durable_state.publish(self.marker_path, value)
                with mock.patch.object(migration.journal, 'Journal') as opening:
                    with self.assertRaises(journal.JournalError):
                        self.open()
                    opening.assert_not_called()
                self.assertEqual(durable_state.read(self.marker_path), value)

    def test_interrupted_ready_publication_resumes_only_matching_bootstrap(self):
        publish = durable_state.publish
        def fail_ready(path, value):
            if path == self.marker_path and value['state'] == 'ready':
                raise OSError('synthetic failure before ready publication')
            publish(path, value)
        with mock.patch.object(durable_state, 'publish', side_effect=fail_ready):
            with self.assertRaises(OSError):
                self.open(after=17)
        preparing = durable_state.read(self.marker_path)
        self.assertEqual(preparing['state'], 'preparing')
        with closing(self.open(after=0)) as owner:
            self.assertEqual(owner.marker['nonce'], preparing['nonce'])
            self.assertEqual(owner.store.meta()['scan_through'], 17)
            self.assertFalse(owner.store.meta()['activation_confirmed'])

    def test_uncertain_ready_publication_stops_then_can_be_verified_on_restart(self):
        publish = durable_state.publish
        def fail_after_ready(path, value):
            publish(path, value)
            if path == self.marker_path and value['state'] == 'ready':
                raise OSError('synthetic failure after replacement')
        with mock.patch.object(durable_state, 'publish', side_effect=fail_after_ready):
            with self.assertRaises(OSError):
                self.open()
        self.assertEqual(durable_state.read(self.marker_path)['state'], 'ready')
        with closing(self.open()) as owner:
            self.assertFalse(owner.store.meta()['activation_confirmed'])
            self.assertIsNotNone(owner.activation_request)

    def test_preparing_marker_cannot_adopt_delivery_history(self):
        with closing(self.open()) as owner:
            with owner.store.transaction():
                owner.store.count('attempts')
            marker = dict(owner.marker, state='preparing')
        durable_state.publish(self.marker_path, marker)
        before = migration.sqlite_files(self.root)[0].read_bytes()
        with self.assertRaises(journal.JournalError):
            self.open()
        self.assertEqual(migration.sqlite_files(self.root)[0].read_bytes(), before)

    def test_missing_ready_journal_never_reimports_the_legacy_cursor(self):
        with closing(self.open()) as owner:
            nonce = owner.marker['nonce']
        for path in migration.sqlite_files(self.root):
            path.unlink(missing_ok=True)
        with self.assertRaises(journal.JournalError):
            self.open(after=0)
        self.assertFalse(migration.sqlite_files(self.root)[0].exists())
        self.assertEqual(durable_state.read(self.marker_path)['nonce'], nonce)

    def test_missing_marker_and_journal_refuse_when_activation_or_guard_remains(self):
        owner, evidence = self.complete()
        owner.close()
        self.marker_path.unlink()
        for path in migration.sqlite_files(self.root):
            path.unlink(missing_ok=True)
        with self.assertRaises(journal.JournalError):
            self.open(activation=evidence)
        with self.assertRaises(journal.JournalError):
            self.open(activation=None)
        self.assertFalse(self.marker_path.exists())

    def test_lost_activation_reply_retries_the_same_pair_without_reset(self):
        with closing(self.open()) as owner:
            evidence = self.evidence(owner)
        with closing(self.open(activation=evidence)) as owner:
            self.assertEqual(owner.activation_request, dict(op='activate-notification-journal', **evidence))
            owner.confirm_activation(evidence)
        with closing(self.open(activation=evidence)) as owner:
            self.assertTrue(owner.store.meta()['activation_confirmed'])
            self.assertEqual(owner.activation_request, dict(op='activate-notification-journal', **evidence))

    def test_confirmed_flag_cannot_mask_missing_or_different_bridge_evidence(self):
        owner, evidence = self.complete()
        owner.close()
        for supplied in (None, dict(evidence, nonce='c' * 32), dict(evidence, target_digest='d' * 64)):
            with self.subTest(supplied=supplied), self.assertRaises(journal.JournalError):
                self.open(activation=supplied)
        with closing(self.open(activation=evidence)) as owner:
            self.assertEqual(owner.activation_request['nonce'], evidence['nonce'])

    def test_old_bridge_requires_prior_confirmed_activation(self):
        with self.assertRaises(journal.JournalError):
            self.open(capable=False)
        with closing(self.open()) as owner:
            evidence = self.evidence(owner)
        with self.assertRaises(journal.JournalError):
            self.open(capable=False)
        with closing(self.open(activation=evidence)) as owner:
            owner.confirm_activation(evidence)
        with closing(self.open(capable=False)) as owner:
            self.assertIsNone(owner.activation_request)
            self.assertTrue(owner.store.meta()['activation_confirmed'])

    def test_rebuild_requires_explicit_loss_acceptance_and_preserves_existing_files(self):
        owner, evidence = self.complete()
        owner.close()
        before = migration.sqlite_files(self.root)[0].read_bytes()
        for accepted in (False, True):
            with self.subTest(accepted=accepted), self.assertRaises(journal.JournalError):
                migration.Migration.rebuild(self.root, 'codex', PARTICIPANT,
                    accept_history_loss=accepted, capable=True, activation=evidence, ack_through=3)
            self.assertEqual(migration.sqlite_files(self.root)[0].read_bytes(), before)

    def test_rebuild_uses_ack_watermark_and_health_ack_preserves_history_identity(self):
        durable_state.publish(self.cursor_path, dict(thread=PARTICIPANT, through=17))
        owner, evidence = self.complete()
        owner.close()
        for path in migration.sqlite_files(self.root):
            path.unlink(missing_ok=True)
        with closing(migration.Migration.rebuild(self.root, 'codex', PARTICIPANT,
                accept_history_loss=True, capable=True, activation=evidence, ack_through=7)) as owner:
            self.assertEqual(owner.store.meta()['scan_through'], 7)
            self.assertNotEqual(owner.marker['nonce'], evidence['nonce'])
            self.assertEqual(owner.activation_request['op'], 'rebuild-notification-journal-activation')
            self.assertEqual(owner.activation_request['expected_previous_nonce'], evidence['nonce'])
            replacement = self.evidence(owner)
            owner.confirm_activation(replacement)
            self.assertTrue(owner.store.status()['history_lost'])
            owner.store.acknowledge_health()
            self.assertFalse(owner.store.status()['history_lost'])
            self.assertTrue(owner.store.meta()['history_lost'])
            self.assertTrue(owner.marker['history_lost'])
        self.assertEqual(durable_state.read(self.cursor_path)['through'], 17)
        with closing(self.open(activation=replacement)) as owner:
            self.assertTrue(owner.store.meta()['history_lost'])
            self.assertTrue(owner.store.meta()['history_acknowledged'])

    def test_interrupted_rebuild_resumes_its_recorded_nonce_and_watermark(self):
        owner, evidence = self.complete()
        owner.close()
        for path in migration.sqlite_files(self.root):
            path.unlink(missing_ok=True)
        with mock.patch.object(migration.journal, 'Journal', side_effect=sqlite3.OperationalError('synthetic')):
            with self.assertRaises(sqlite3.OperationalError):
                migration.Migration.rebuild(self.root, 'codex', PARTICIPANT,
                    accept_history_loss=True, capable=True, activation=evidence, ack_through=7)
        marker = durable_state.read(self.marker_path)
        self.assertEqual(marker['state'], 'rebuilding')
        with closing(migration.Migration.rebuild(self.root, 'codex', PARTICIPANT,
                accept_history_loss=True, capable=True, activation=evidence, ack_through=9)) as owner:
            self.assertEqual(owner.marker['nonce'], marker['nonce'])
            self.assertEqual(owner.store.meta()['scan_through'], 7)

    def test_rebuild_refuses_foreign_legacy_cursor_before_changing_marker(self):
        owner, evidence = self.complete()
        owner.close()
        for path in migration.sqlite_files(self.root):
            path.unlink(missing_ok=True)
        before = self.marker_path.read_bytes()
        durable_state.publish(self.cursor_path, dict(thread='other-synthetic-session', through=17))
        with self.assertRaises(journal.JournalError):
            migration.Migration.rebuild(self.root, 'codex', PARTICIPANT,
                accept_history_loss=True, capable=True, activation=evidence, ack_through=7)
        self.assertEqual(self.marker_path.read_bytes(), before)
        self.assertFalse(migration.sqlite_files(self.root)[0].exists())
