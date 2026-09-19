import ast
from contextlib import closing
import inspect
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from database_worker import DatabaseWorker, WorkerFailure
import notification_journal as journal
import notification_migration as migration
from test_notification_journal import work


class ErrorPolicyTests(unittest.IsolatedAsyncioTestCase):
    def test_every_raised_code_has_an_explicit_policy(self):
        codes = set()
        for module in (journal, migration):
            for node in ast.walk(ast.parse(inspect.getsource(module))):
                if not isinstance(node, ast.Call) or not node.args:
                    continue
                name = node.func.id if isinstance(node.func, ast.Name) else getattr(node.func, 'attr', None)
                if name == 'JournalError' and isinstance(node.args[0], ast.Constant):
                    codes.add(node.args[0].value)
        self.assertEqual(codes, set(journal.ERROR_POLICY))
        with self.assertRaises(KeyError):
            journal.JournalError('unclassified_synthetic_error')

    async def test_worker_distinguishes_every_domain_refusal_from_internal_and_storage_faults(self):
        class Owner:
            def fail(self, code):
                raise journal.JournalError(code)
            def close(self):
                pass
        for code, (recovery, fault) in journal.ERROR_POLICY.items():
            with self.subTest(code=code):
                worker = DatabaseWorker(Owner)
                try:
                    error_type = WorkerFailure if fault == 'internal_error' else journal.JournalError
                    with self.assertRaises(error_type) as raised:
                        await worker.call('fail', code)
                    self.assertEqual(worker.fault, fault)
                    if error_type is journal.JournalError:
                        self.assertEqual(raised.exception.recovery, recovery)
                finally:
                    await worker.close()


class StateValidationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'journal.sqlite3'
        self.identity = journal.identity('codex', 'a' * 64, 'b' * 32, 0)
        self.store = journal.Journal(self.path, self.identity, create=True)
        self.addCleanup(self.store.close)
        self.store.confirm_activation(dict(target_digest='a' * 64, nonce='b' * 32))
        self.store.seed_pointers(dict(records=[]))
        self.store.ingest(dict(through=2, records=[work(1), work(2, 'memory-pointer')]))

    def test_impossible_dispositions_roll_back_without_spending_budget(self):
        for state, attempts, uncertain, retry_at in (
                ('pending', 3, 0, 0), ('reserved', 0, 0, 0), ('delivered', 0, 0, 0),
                ('failed', 2, 0, 0), ('failed', 3, 1, 0), ('unknown', 3, 0, 0),
                ('delivered', 1, 0, 5)):
            with self.subTest(state=state, attempts=attempts, uncertain=uncertain, retry_at=retry_at):
                with self.assertRaises(journal.JournalError):
                    with self.store.transaction():
                        self.store.db.execute('UPDATE work SET disposition=?,attempts=?,uncertain=?,retry_at=? WHERE seq=1',
                                              (state, attempts, uncertain, retry_at))
                self.assertEqual(self.store.rows()[0]['attempts'], 0)

    def test_mixed_kind_reservation_is_refused_on_reopen(self):
        # Simulate a damaged store using constraint-valid SQL, then reopen it.
        with self.store.transaction(validate=False):
            self.store.db.execute("UPDATE work SET disposition='reserved',attempts=1")
            self.store.db.execute('INSERT INTO attempt VALUES(1,0)')
            self.store.db.executemany('INSERT INTO attempt_member VALUES(1,?)', [(1,), (2,)])
        self.store.close()
        with self.assertRaisesRegex(journal.JournalError, 'journal_recovery_required'):
            journal.Journal(self.path, self.identity)

    def test_invalid_stored_counter_is_recovery_not_internal_api_fault(self):
        with self.store.transaction(validate=False):
            self.store.put('scan_through', -1)
        with self.assertRaises(journal.JournalError) as raised:
            self.store.validate()
        self.assertEqual(raised.exception.code, 'journal_recovery_required')
        self.assertIsNone(raised.exception.database_fault)

    def test_unexpected_sidecars_are_refused_before_sqlite_opens(self):
        self.store.close()
        for suffix in ('-shm', '-journal'):
            path = Path(str(self.path) + suffix)
            path.write_bytes(b'synthetic retained state')
            with self.subTest(suffix=suffix), mock.patch.object(journal.sqlite3, 'connect') as connect:
                with self.assertRaisesRegex(journal.JournalError, 'journal_recovery_required'):
                    journal.Journal(self.path, self.identity)
                connect.assert_not_called()
                self.assertEqual(path.read_bytes(), b'synthetic retained state')
            path.unlink()
