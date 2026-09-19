from contextlib import closing
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest import mock

import notification_journal as journal


class JournalStorageTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'notify-journal.sqlite3'
        self.identity = journal.identity('codex', 'a' * 64, 'b' * 32, 17)

    def open(self, **options):
        return journal.Journal(self.path, self.identity, **options)

    def test_bootstrap_and_reopen_preserve_exact_identity_and_checkpoint(self):
        with closing(self.open(create=True, bootstrap=True)) as store:
            self.assertEqual(store.meta(), self.identity)
        with closing(self.open(bootstrap=True)) as store:
            self.assertEqual(store.meta()['scan_through'], 17)
            self.assertEqual(store.rows(), [])

    def test_readiness_never_adopts_an_empty_or_missing_store(self):
        with self.assertRaises(sqlite3.OperationalError):
            self.open()
        with closing(sqlite3.connect(self.path)):
            pass
        with self.assertRaises(journal.JournalError):
            self.open()

    def test_wrong_identity_does_not_modify_the_existing_store(self):
        with closing(self.open(create=True)):
            pass
        before = self.path.read_bytes()
        for key, value in (('target_digest', 'c' * 64), ('nonce', 'd' * 32),
                           ('provider', 'deepseek'), ('imported_through', 18),
                           ('history_lost', True)):
            with self.subTest(key=key), self.assertRaises(journal.JournalError):
                journal.Journal(self.path, dict(self.identity, **{key: value}))
            self.assertEqual(self.path.read_bytes(), before)

    def test_unrecognized_catalog_is_refused_before_configuration(self):
        with closing(sqlite3.connect(self.path)) as db:
            db.execute('CREATE TABLE meta(key TEXT,value TEXT)')
            db.commit()
        before = self.path.read_bytes()
        with mock.patch.object(journal.Journal, 'configure') as configure:
            with self.assertRaises(journal.JournalError):
                self.open(create=True, bootstrap=True)
            configure.assert_not_called()
        self.assertEqual(self.path.read_bytes(), before)

    def test_required_storage_configuration_is_verified(self):
        with closing(self.open(create=True)) as store:
            for pragma, expected in (('page_size', 4096), ('max_page_count', 1024),
                                     ('journal_mode', 'wal'), ('locking_mode', 'exclusive'),
                                     ('synchronous', 2), ('cache_spill', 0), ('temp_store', 2),
                                     ('wal_autocheckpoint', 0), ('auto_vacuum', 0),
                                     ('fullfsync', 1), ('checkpoint_fullfsync', 1)):
                with self.subTest(pragma=pragma):
                    self.assertEqual(store.db.execute('PRAGMA ' + pragma).fetchone()[0], expected)

    def test_checkpoint_failure_prevents_every_transaction_body(self):
        with closing(self.open(create=True)) as store:
            with mock.patch.object(store, 'reset_log', side_effect=journal.JournalError('journal_checkpoint_failed')):
                entered = False
                with self.assertRaises(journal.JournalError):
                    with store.transaction():
                        entered = True
                self.assertFalse(entered)
                self.assertFalse(store.db.in_transaction)
                self.assertEqual(store.meta(), self.identity)

    def test_all_mutation_entry_points_refuse_a_failed_log_reset(self):
        with closing(self.open(create=True)) as store:
            calls = (
                lambda: store.ingest(dict(through=18, records=[work(18)])),
                lambda: store.seed_pointers(dict(records=[])),
                lambda: store.reserve([18], 0),
                lambda: store.resolve('unknown', 0),
                lambda: store.retry([18]),
                lambda: store.reconcile(dict(ack_through=18, records=[], bindings={}, pointers={})),
                lambda: store.confirm_activation(dict(target_digest='a' * 64, nonce='b' * 32)),
                store.acknowledge_health,
            )
            before = store.meta()
            for index, call in enumerate(calls):
                with self.subTest(index=index), mock.patch.object(store, 'reset_log',
                        side_effect=journal.JournalError('journal_checkpoint_failed')):
                    with self.assertRaisesRegex(journal.JournalError, 'journal_checkpoint_failed'):
                        call()
                    self.assertFalse(store.db.in_transaction)
                    self.assertEqual(store.meta(), before)
                    self.assertEqual(store.rows(), [])

    def test_failed_mutation_rolls_back_all_metadata(self):
        with closing(self.open(create=True)) as store:
            with self.assertRaises(RuntimeError):
                with store.transaction():
                    store.put('scan_through', 99)
                    raise RuntimeError('synthetic crash before commit')
            self.assertFalse(store.db.in_transaction)
            self.assertEqual(store.meta()['scan_through'], 17)
        with closing(self.open()) as store:
            self.assertEqual(store.meta()['scan_through'], 17)

    def test_invalid_committed_state_cannot_be_bootstrapped_again(self):
        with closing(self.open(create=True)) as store:
            with store.transaction():
                store.put('pointers_seeded', True)
        with self.assertRaises(journal.JournalError):
            self.open(create=True, bootstrap=True)
        with closing(self.open()) as store:
            self.assertTrue(store.meta()['pointers_seeded'])

    def test_metadata_updates_require_a_transaction(self):
        with closing(self.open(create=True)) as store:
            with self.assertRaises(RuntimeError):
                store.put('scan_through', 18)
            with self.assertRaises(journal.JournalError):
                with store.transaction():
                    store.put('scan_through', 16)
            self.assertEqual(store.meta()['scan_through'], 17)

    def test_declared_file_budget_is_independent_and_enforced_before_open(self):
        self.assertEqual(journal.MAX_DATABASE_BYTES + journal.MAX_WAL_BYTES, 8479136)
        with self.path.open('wb') as stream:
            stream.truncate(journal.MAX_DATABASE_BYTES + 1)
        with mock.patch.object(journal.sqlite3, 'connect') as connect:
            with self.assertRaises(journal.JournalError):
                self.open(create=True)
            connect.assert_not_called()
        self.assertEqual(self.path.stat().st_size, journal.MAX_DATABASE_BYTES + 1)

    def test_maximum_work_rows_fit_the_budget_and_excess_rolls_back(self):
        with closing(self.open(create=True)) as store:
            with store.transaction():
                store.db.executemany('INSERT INTO work VALUES(?,?,?,?,?,?,?,?)',
                    ((i, 'memory-pointer', 'c' * 64, 'd' * 32, 'pending', 0, 0, 0)
                     for i in range(1, journal.MAX_WORK + 1)))
            self.assertEqual(len(store.rows()), journal.MAX_WORK)
            store.check_file_sizes()
            with self.assertRaises(journal.JournalError):
                with store.transaction():
                    store.db.execute('INSERT INTO work VALUES(?,?,?,?,?,?,?,?)',
                        (journal.MAX_WORK + 1, 'peer', None, None, 'pending', 0, 0, 0))
            self.assertEqual(len(store.rows()), journal.MAX_WORK)
            store.check_file_sizes()


def work(seq, kind='peer'):
    return dict(seq=seq, kind=kind, binding=None if kind == 'peer' else 'c' * 64,
                binding_instance=None if kind == 'peer' else 'd' * 32)


class JournalAttemptTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'journal.sqlite3'
        self.identity = journal.identity('codex', 'a' * 64, 'b' * 32, 0)
        self.store = journal.Journal(self.path, self.identity, create=True)
        self.store.confirm_activation(dict(target_digest='a' * 64, nonce='b' * 32))
        self.store.seed_pointers(dict(records=[]))
        self.addCleanup(lambda: self.store.close())

    def ingest(self, *records):
        return self.store.ingest(dict(through=max(row['seq'] for row in records), records=list(records)))

    def snapshot(self, records=(), ack=0, bindings=None, pointers=None):
        return dict(records=list(records), ack_through=ack,
                    bindings=bindings, pointers=pointers)

    def test_reservation_survives_restart_and_recovery_consumes_its_budget(self):
        self.ingest(work(1))
        self.store.reserve([1], 10)
        self.store.close()
        self.store = journal.Journal(self.path, self.identity)
        self.assertTrue(self.store.recover_attempt(20))
        row = self.store.rows()[0]
        self.assertEqual((row['disposition'], row['attempts'], row['uncertain'], row['retry_at']),
                         ('pending', 1, 1, 50))
        self.assertFalse(self.store.recover_attempt(21))
        self.assertEqual(self.store.due(49), [])
        self.assertEqual(self.store.due(50), [1])
        self.store.reserve([1], 50)
        self.assertEqual(self.store.rows()[0]['attempts'], 2)

    def test_exhaustion_is_durable_and_later_work_can_proceed(self):
        self.ingest(work(1), work(2))
        for now in (0, 30, 90):
            self.store.reserve([1], now)
            self.store.resolve('unknown', now)
        row = self.store.rows()[0]
        self.assertEqual((row['disposition'], row['attempts'], row['uncertain']), ('unknown', 3, 1))
        self.assertEqual(self.store.meta()['scan_through'], 1)
        self.assertEqual(self.store.due(100), [2])
        self.store.reserve([2], 100)
        self.store.resolve('delivered', 100)
        self.assertEqual(self.store.meta()['scan_through'], 2)
        self.assertEqual([item['seq'] for item in self.store.rows()], [1])
        counters = self.store.meta()['counters']
        self.assertEqual((counters['delivered'], counters['unknown'], counters['attempts']), (1, 1, 4))
        self.store.retry([1])
        row = self.store.rows()[0]
        self.assertEqual((row['attempts'], row['uncertain'], row['disposition']), (0, 1, 'pending'))
        self.assertEqual(self.store.meta()['counters']['attempts'], 4)

    def test_backoff_does_not_reorder_later_notice_units(self):
        self.ingest(work(1), work(2))
        self.store.reserve([1], 0)
        self.store.resolve('failed', 0)
        self.assertEqual(self.store.due(29), [])
        self.assertEqual(self.store.due(30), [1, 2])

    def test_success_is_not_replayed_when_a_later_unit_fails(self):
        records = [work(1), work(2)]
        self.ingest(*records)
        self.store.reserve([1], 0)
        self.store.resolve('delivered', 0)
        self.store.reserve([2], 0)
        self.store.resolve('failed', 0)
        self.ingest(*records)
        self.assertEqual([row['seq'] for row in self.store.rows()], [2])
        self.assertEqual(self.store.meta()['counters']['delivered'], 1)

    def test_acknowledgement_ends_retry_without_claiming_delivery(self):
        self.ingest(work(1))
        self.store.reserve([1], 0)
        with self.assertRaises(journal.JournalError):
            self.store.reconcile(self.snapshot(ack=1))
        self.store.resolve('unknown', 1)
        self.store.reconcile(self.snapshot(ack=1))
        self.assertEqual(self.store.rows(), [])
        counters = self.store.meta()['counters']
        self.assertEqual((counters['acknowledged'], counters['delivered'], counters['failed']), (1, 0, 0))

    def test_failed_result_commit_leaves_reservation_for_uncertain_recovery(self):
        self.ingest(work(1))
        self.store.reserve([1], 0)
        original = self.store.put
        def fail(key, value):
            if key == 'scan_through':
                raise sqlite3.OperationalError('synthetic result commit failure')
            return original(key, value)
        with mock.patch.object(self.store, 'put', side_effect=fail):
            with self.assertRaises(sqlite3.OperationalError):
                self.store.resolve('delivered', 1)
        self.assertEqual(self.store.rows()[0]['disposition'], 'reserved')
        self.assertEqual(self.store.meta()['counters']['delivered'], 0)
        self.assertTrue(self.store.recover_attempt(2))
        self.assertEqual(self.store.rows()[0]['uncertain'], 1)

    def test_notice_groups_do_not_cross_pointer_rows(self):
        self.ingest(work(1), work(2, 'memory-pointer'), work(3))
        self.assertEqual(self.store.due(0), [1])
        with self.assertRaises(journal.JournalError):
            self.store.reserve([1, 2], 0)
        self.store.reserve([1], 0)
        self.store.resolve('delivered', 0)
        self.assertEqual(self.store.due(0), [2])

    def test_seeded_pointer_below_imported_checkpoint_is_not_reseeded(self):
        self.store.close()
        self.path.unlink()
        self.identity = journal.identity('codex', 'a' * 64, 'b' * 32, 17)
        self.store = journal.Journal(self.path, self.identity, create=True)
        self.store.confirm_activation(dict(target_digest='a' * 64, nonce='b' * 32))
        snapshot = dict(records=[work(3, 'memory-pointer')])
        self.assertTrue(self.store.seed_pointers(snapshot))
        self.store.reserve([3], 0)
        self.store.resolve('delivered', 0)
        self.assertEqual(self.store.meta()['scan_through'], 17)
        self.store.close()
        self.store = journal.Journal(self.path, self.identity)
        self.assertFalse(self.store.seed_pointers(snapshot))
        self.assertEqual(self.store.rows(), [])

    def test_seeded_future_pointer_cannot_skip_unscanned_peer_messages(self):
        self.store.close()
        self.path.unlink()
        self.store = journal.Journal(self.path, self.identity, create=True)
        self.store.confirm_activation(dict(target_digest='a' * 64, nonce='b' * 32))
        self.store.seed_pointers(dict(records=[work(1000, 'memory-pointer')]))
        self.store.ingest(dict(through=128, records=[]))
        self.store.reserve([1000], 0)
        self.store.resolve('delivered', 0)
        self.assertEqual(self.store.meta()['scan_through'], 128)
        self.assertEqual(self.store.meta()['enumerated_through'], 128)
        self.assertEqual(self.store.rows()[0]['disposition'], 'delivered')
        self.store.close()
        self.store = journal.Journal(self.path, self.identity)
        self.store.ingest(dict(through=256, records=[work(200)]))
        self.assertEqual(self.store.meta()['scan_through'], 199)
        self.assertEqual(self.store.due(0), [200])
        self.store.reserve([200], 0)
        self.store.resolve('delivered', 0)
        self.assertEqual(self.store.meta()['scan_through'], 256)
        self.store.ingest(dict(through=1000, records=[work(1000, 'memory-pointer')]))
        self.assertEqual(self.store.meta()['scan_through'], 1000)
        self.assertEqual(self.store.rows(), [])
        self.assertEqual(self.store.meta()['counters']['delivered'], 2)

    def test_missing_peer_is_not_acknowledged_without_evidence(self):
        self.ingest(work(1))
        with self.assertRaises(journal.JournalError):
            self.store.reconcile(self.snapshot())
        self.assertEqual(self.store.rows()[0]['disposition'], 'pending')
        self.assertEqual(self.store.meta()['scan_through'], 0)
        self.store.reconcile(self.snapshot(ack=None))
        self.assertEqual(self.store.rows(), [])
        self.assertEqual(self.store.meta()['counters']['unknown'], 1)
        self.assertEqual(self.store.meta()['counters']['acknowledged'], 0)
        self.assertEqual(self.store.meta()['scan_through'], 1)

    def test_replacement_pointer_is_obsolete_not_acknowledged(self):
        old, new = work(1, 'memory-pointer'), work(3, 'memory-pointer')
        self.ingest(old)
        self.store.reconcile(self.snapshot(bindings={old['binding']: old['binding_instance']},
                                           pointers={old['binding']: new}))
        self.assertEqual(self.store.rows(), [])
        self.assertEqual(self.store.meta()['counters']['obsolete'], 1)
        self.assertEqual(self.store.meta()['counters']['acknowledged'], 0)
        self.ingest(new)
        self.assertEqual(self.store.due(0), [3])

    def test_unknown_binding_metadata_is_not_binding_removal(self):
        self.ingest(work(1, 'memory-pointer'), work(2))
        self.store.reconcile(self.snapshot(records=[work(2)], ack=None))
        self.assertEqual(len(self.store.rows()), 2)
        self.assertEqual(self.store.meta()['counters']['obsolete'], 0)
        self.assertEqual(self.store.due(0, memory_available=False), [2])


    def test_delivery_gate_requires_both_activation_and_seed_completion(self):
        self.ingest(work(1))
        for activated, seeded in ((False, False), (False, True), (True, False)):
            with self.subTest(activated=activated, seeded=seeded):
                with self.store.transaction():
                    self.store.put('activation_confirmed', activated)
                    self.store.put('pointers_seeded', seeded)
                with self.assertRaisesRegex(journal.JournalError, 'journal_not_ready'):
                    self.store.reserve([1], 0)
                self.assertEqual(self.store.rows()[0]['attempts'], 0)
        with self.assertRaises(journal.JournalError):
            self.store.confirm_activation(dict(target_digest='a' * 64, nonce='c' * 32))

    def test_health_acknowledgement_does_not_reset_exhausted_work(self):
        self.ingest(work(1))
        for now in (0, 30, 90):
            self.store.reserve([1], now)
            self.store.resolve('unknown', now)
        old = self.store.rows()
        cleared = self.store.acknowledge_health()
        self.assertEqual(cleared['unknown'], 1)
        self.assertEqual(self.store.rows(), old)
        self.assertEqual(self.store.status()['exhausted'], 1)
        self.assertEqual(self.store.status()['uncertain'], 1)
        self.assertFalse(any(self.store.status()['counters'].values()))
