import asyncio
import json
import os
import random
from pathlib import Path
import signal
import sqlite3
import subprocess
import sys
import threading
import tempfile
import time
import unittest
from unittest.mock import patch

import memory
import platform_support

REPO = '0123456789abcdef'


def git_repo(parent, name='repo'):
    root = Path(parent)/name
    root.mkdir(parents=True)
    subprocess.run(['git','init','-q',str(root)],check=True,capture_output=True)
    subprocess.run(['git','-C',str(root),'commit','-q','--allow-empty','-m','base'],
                   check=True,capture_output=True,
                   env=dict(os.environ,GIT_AUTHOR_NAME='t',GIT_AUTHOR_EMAIL='t@e',
                            GIT_COMMITTER_NAME='t',GIT_COMMITTER_EMAIL='t@e'))
    return root


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.home = Path(self.tmp.name)
        self.s = memory.Store(self.home/'memory.sqlite3', REPO)
        self.addCleanup(self.s.close)
        self.svc = memory.Service(self.home, REPO, self.s)

    def call(self, **r):
        return self.svc.command(r, 4242)

    def note(self, body, **kw):
        kw.setdefault('consumer', 'writer')
        kw.setdefault('kind', 'finding')
        if kw.get('key') is not None and 'deadline' not in kw:
            kw['deadline'] = time.time() + 600
        return self.s.note(kw.pop('consumer'), kw.pop('kind'), body, **kw)['seq']

    def drain(self, consumer='reader', limit_pages=50):
        """Page a snapshot to the end and acknowledge it, as a caller must."""
        page = self.call(op='sync', consumer=consumer)
        self.assertEqual(page['kind'], 'snapshot')
        for _ in range(limit_pages):
            if not page['more']:
                break
            page = self.call(op='sync', consumer=consumer, snapshot_id=page['snapshot_id'],
                             page_token=page['page_token'])
        return self.call(op='ack', consumer=consumer, snapshot_id=page['snapshot_id'])


class IdentityTests(unittest.TestCase):
    def test_worktrees_share_one_store_and_repositories_do_not_collide(self):
        with tempfile.TemporaryDirectory() as d:
            root = git_repo(d)
            deep = root/'a'/'b'
            deep.mkdir(parents=True)
            tree = Path(d)/'linked-worktree'
            subprocess.run(['git','-C',str(root),'worktree','add','-q','--detach',str(tree)],
                           check=True,capture_output=True)
            # A real worktree, not a subdirectory: its .git is a file pointing at the
            # common directory, which is exactly the case the absolute form must handle.
            self.assertTrue((tree/'.git').is_file())
            self.assertEqual(memory.repo_identity(root), memory.repo_identity(deep))
            self.assertEqual(memory.repo_identity(root), memory.repo_identity(tree))
            self.assertNotEqual(memory.repo_identity(root), memory.repo_identity(git_repo(d,'other')))

    def test_outside_a_repository_is_refused(self):
        with tempfile.TemporaryDirectory() as d, patch('memory.subprocess.run') as run:
            run.return_value = subprocess.CompletedProcess([], 128, '', 'not a git repository')
            with self.assertRaises(memory.MemoryError_) as e:
                memory.repo_identity(d)
            self.assertEqual(e.exception.code, 'repo_unresolved')


class WriteTests(Base):
    def test_head_is_durable_and_never_moves_backwards(self):
        self.note('one')
        last = self.note('two', expires=time.time()-1)
        self.assertEqual(self.s.head(), last)
        self.s.reclaim()
        # The expired entry is gone, but the event head must not rewind, or a later
        # append would reuse a sequence a reader has already seen.
        self.assertEqual(self.s.head(), last)
        self.assertEqual(self.note('three'), last+1)

    def test_reclaiming_a_replacement_cannot_resurrect_what_it_replaced(self):
        old = self.note('superseded text')
        self.note('replacement', supersedes=old, expires=time.time()-1)
        self.assertNotIn(old, {e['seq'] for e in self.s.live()})
        self.s.reclaim()
        # The link lives on the replaced row, so deleting the replacement leaves the
        # original replaced rather than silently live again.
        self.assertNotIn(old, {e['seq'] for e in self.s.live()})

    def test_competing_revisions_are_retained_and_the_conflict_is_reported(self):
        """The contract forbids silent loss, so a second reporter is kept, not refused."""
        original = self.note('original')
        winner = self.note('one reporter replaces it', supersedes=original)
        competing = self.s.note('writer-b', 'finding', 'another reporter replaces it too',
                                supersedes=original)
        self.assertEqual(competing['conflicts_with'], winner)
        live = {e['seq']: e for e in self.s.live()}
        # Both replacements survive and remain visible. Refusing the later one would be
        # first-writer-wins with the loser discarded.
        self.assertIn(winner, live)
        self.assertIn(competing['seq'], live)
        self.assertNotIn(original, live)
        self.assertEqual(live[competing['seq']]['conflicts_with'], winner)
        self.assertIsNone(live[winner]['conflicts_with'])

    def test_same_key_with_changed_payload_is_refused_and_scope_includes_consumer(self):
        self.note('original', key='k9')
        with self.assertRaises(memory.MemoryError_) as e:
            self.note('changed', key='k9')
        self.assertEqual(e.exception.code, 'idempotency_conflict')
        self.assertEqual(len(self.s.live()), 1)
        self.assertNotEqual(self.note('other agent', consumer='writer-b', key='k9'), None)

    def test_a_failed_write_rolls_back_and_leaves_no_partial_state(self):
        good = self.note('kept')
        before = self.s.head()
        with self.assertRaises(memory.MemoryError_):
            self.note('doomed', supersedes=99999)
        # The replaced-entry check fails inside the transaction after head was bumped,
        # so a missing rollback would leak a head increment and an orphan row.
        self.assertEqual(self.s.head(), before)
        self.assertEqual([e['seq'] for e in self.s.live()], [good])
        self.assertIsNone(self.s.db.execute('SELECT seq FROM entries WHERE body=?',
                                            ('doomed',)).fetchone())

    def test_scope_target_is_required_for_narrow_scopes(self):
        with self.assertRaises(memory.MemoryError_) as e:
            self.note('task bound', scope='task')
        self.assertEqual(e.exception.code, 'invalid_request')
        self.assertTrue(self.note('task bound', scope='task', scope_target='T-1'))

    def test_a_store_declaring_another_schema_is_refused(self):
        self.s.set_meta('schema', memory.SCHEMA + 1)
        self.s.db.commit()
        # The owner releases the store first. Exclusive locking refuses a second
        # connection outright, which is a different condition asserted separately, and
        # opening one here would test the lock instead of the schema check.
        self.s.close()
        with self.assertRaises(memory.MemoryError_) as e:
            memory.Store(self.home/'memory.sqlite3', REPO)
        self.assertEqual(e.exception.code, 'schema_too_new')

    def test_a_second_owner_is_refused_as_busy_not_as_unreadable(self):
        """Exclusive locking makes a second owner routine, so it needs its own code.

        Reporting it as an unreadable file would tell an operator to suspect corruption
        when the real cause is that the service already holds the store.
        """
        with self.assertRaises(memory.MemoryError_) as e:
            memory.Store(self.home/'memory.sqlite3', REPO)
        self.assertEqual(e.exception.code, 'store_busy')
        self.assertIn('control socket', str(e.exception))

    def test_byte_width_not_character_count_bounds_a_body(self):
        wide = 'é' * (memory.MAX_BODY//2 + 1)
        self.assertLess(len(wide), memory.MAX_BODY)
        self.assertGreater(len(wide.encode()), memory.MAX_BODY)
        with self.assertRaises(memory.MemoryError_) as e:
            self.note(wide)
        self.assertEqual(e.exception.code, 'entry_too_large')

    def test_capacity_refusal_preserves_records_and_reserves_bytes_for_a_withdrawal(self):
        with patch.object(memory,'MAX_ENTRIES',8), patch.object(memory,'RESERVED_ENTRIES',3), \
             patch.object(memory,'MAX_LOGICAL_BYTES',8000), patch.object(memory,'RESERVED_BYTES',3000):
            body = 'x'*400
            for _ in range(5):
                self.note(body)
            with self.assertRaises(memory.MemoryError_) as e:
                self.note(body)
            self.assertEqual(e.exception.code,'capacity')
            self.assertIn('stored data is intact', str(e.exception))
            self.assertEqual(len(self.s.live()), 5)
            # Reserved slots and reserved bytes together must still admit a withdrawal.
            target = self.s.live()[0]['seq']
            self.assertTrue(self.note('withdrawn', kind='directive', revokes=target))

    def test_growth_stops_at_the_page_ceiling_and_space_is_recoverable(self):
        """The ceiling must stop real growth, reserve room for a withdrawal, and recover.

        A test that sets the limit below an existing file proves only that a comparison
        happens. This drives actual allocation into the ceiling.

        It asserts pages, not file bytes. The log is deliberately outside the comparison:
        including it made the outcome depend on when the last checkpoint ran and on how far
        a particular SQLite build shrank the file during recovery, which is why the earlier
        version of this test passed on five CI runners and failed on the sixth. Pages are
        the quantity the engine itself caps, so the result is the same on every build.
        """
        body = 'x' * 4000
        ceiling = self.s.pages() + 600
        reserve = 120
        written = []
        with patch.object(memory, 'MAX_PAGES', ceiling), \
             patch.object(memory, 'ORDINARY_MAX_PAGES', ceiling - reserve), \
             patch.object(memory, 'MAX_ENTRIES', 100_000), \
             patch.object(memory, 'MAX_LOGICAL_BYTES', 1 << 40):
            for _ in range(4000):
                try:
                    written.append(self.note(body, kind='decision'))
                except memory.MemoryError_ as exc:
                    self.assertEqual(exc.code, 'capacity')
                    break
            else:
                self.fail('growth was never bounded')
            self.assertGreater(len(written), 5, 'the bound must not stop growth immediately')
            # Ordinary appends stopped with the reserve substantially intact. The assertion
            # is on the reserve rather than on the exact threshold, because a commit
            # allocates pages the in-transaction count cannot yet see and that gap differs
            # between SQLite builds: it was one page on one supported build and eight on
            # another. Demanding an exact threshold would be testing the build.
            self.assertGreaterEqual(ceiling - self.s.pages(), reserve // 2,
                                    'ordinary appends consumed the reserve')
            # Every growth path is bounded, not only the append path.
            with self.assertRaises(memory.MemoryError_) as e:
                memory.freeze(self.s, 'a-reader-at-the-bound')
            self.assertEqual(e.exception.code, 'capacity')
            with self.assertRaises(memory.MemoryError_) as e:
                self.call(op='sync', consumer='another-reader-at-the-bound')
            self.assertEqual(e.exception.code, 'capacity')
            # A refused mutation leaves nothing behind.
            stored = len(self.s.live())
            # The reserve is what keeps a withdrawal possible at a full store. Without
            # reserved pages a full store would pin a directive it could never retract.
            withdrawal = self.note('withdrawn', kind='directive', revokes=written[0])
            self.assertTrue(withdrawal)
            self.assertEqual(len(self.s.live()), stored)
            # Progress must also still commit at the ceiling, drawing on the same reserve.
            self.s.progress_probe = None
            with self.s.progress():
                self.s.set_meta('probe-at-the-bound', '1')
            self.assertEqual(self.s.meta('probe-at-the-bound'), '1')

            # Recovery: expire nearly everything, reclaim, and prove the pages return and
            # are reusable under the same ceiling. Expiring almost all of it rather than
            # half keeps the assertion independent of how much a particular SQLite build
            # returns in one incremental vacuum, which differs between builds and is not
            # something this design may depend on.
            at_bound = self.s.pages()
            self.s.db.execute('UPDATE entries SET expires=? WHERE seq IN (%s)'
                              % ','.join(str(x) for x in written[:-5]),
                              (time.time()-1,))
            self.s.db.commit()
            self.s.reclaim()
            self.assertLess(self.s.pages(), at_bound, 'reclaimed pages must be returned')
            self.assertTrue(self.note(body, kind='decision'),
                            'writes must resume after recovery')

    def test_page_ceiling_also_refuses(self):
        self.note('one')
        with patch.object(memory, 'MAX_PAGES', 1), \
             patch.object(memory, 'ORDINARY_MAX_PAGES', 1):
            with self.assertRaises(memory.MemoryError_) as e:
                self.note('two')
            self.assertEqual(e.exception.code, 'capacity')
        self.assertEqual(len(self.s.live()), 1)

    def test_a_snapshot_copy_is_charged_against_the_budget(self):
        """Freezing copies every member, so it is a durable mutation like any other."""
        for i in range(4):
            self.note(f'entry {i}')
        before = self.s.usage()['logical']
        memory.freeze(self.s, 'reader')
        self.assertGreater(self.s.usage()['logical'], before)
        with patch.object(memory,'MAX_LOGICAL_BYTES', before):
            with self.assertRaises(memory.MemoryError_) as e:
                memory.freeze(self.s, 'another-reader')
            self.assertEqual(e.exception.code, 'capacity')

    def test_an_empty_snapshot_and_a_keyed_write_are_both_charged(self):
        before = self.s.usage()['logical']
        memory.freeze(self.s, 'reader')
        empty = self.s.usage()['logical']
        # An empty snapshot still writes a header row; charging zero for it would let a
        # reader grow the store without ever being accounted.
        self.assertGreater(empty, before)
        self.s.note('writer', 'finding', 'keyed', key='k1', deadline=time.time()+600)
        self.assertGreater(self.s.usage()['logical'], empty + memory.measure('keyed'))

    def test_registration_is_charged_for_its_eventual_tombstone(self):
        before = self.s.usage()['logical']
        self.call(op='sync', consumer='a-consumer-with-a-long-name')
        self.assertGreater(self.s.usage()['logical'], before)

    def test_an_in_window_idempotency_key_is_never_evicted_to_make_room(self):
        with patch.object(memory,'MAX_IDEM_ROWS', 2):
            deadline = time.time() + 600
            self.note('one', key='k1', deadline=deadline)
            self.note('two', key='k2', deadline=deadline)
            with self.assertRaises(memory.MemoryError_) as e:
                self.note('three', key='k3', deadline=deadline)
            self.assertEqual(e.exception.code, 'idem_capacity')
            # Refusing protects the safe retry; evicting would turn it into a duplicate.
            self.assertTrue(self.s.note('writer','finding','one', key='k1',
                                        deadline=deadline)['duplicate'])

    def test_the_retry_deadline_is_echoed_and_the_horizon_reported(self):
        deadline = time.time() + 600
        result = self.s.note('writer', 'finding', 'bounded retry', key='k1', deadline=deadline)
        self.assertEqual(result['deadline'], deadline)
        self.assertEqual(result['idempotency_horizon'], memory.IDEM_TTL)

    def test_lifetimes_are_enforced_at_the_request_boundary(self):
        self.note('ephemeral', kind='status', expires=time.time()-1)
        self.s.expired_at = 0
        # No capacity pressure here: a horizon enforced only when the store fills is a
        # side effect of pressure, not a lifetime a caller can reason about.
        self.call(op='status')
        self.assertEqual(self.s.live(), [])

    def test_retirement_records_expire_by_age(self):
        self.note('one')
        self.drain()
        self.s.db.execute('UPDATE cursors SET updated=?', (time.time()-memory.CONSUMER_TTL-1,))
        self.s.db.commit()
        self.s.expire()
        self.assertEqual(self.s.db.execute('SELECT count(*) FROM retired').fetchone()[0], 1)
        self.s.db.execute('UPDATE retired SET at=?', (time.time()-memory.RETIRED_TTL-1,))
        self.s.db.commit()
        self.s.expire()
        self.assertEqual(self.s.db.execute('SELECT count(*) FROM retired').fetchone()[0], 0)

    def test_registration_is_bounded_by_consumers_and_their_tombstones(self):
        """The count limit is enforced at admission, not by evicting a tombstone.

        Evicting an in-window tombstone would silently turn a returning retired consumer
        into a new one, contradicting the retention the service promises it.
        """
        with patch.object(memory, 'MAX_CONSUMERS', 2), patch.object(memory, 'MAX_RETIRED', 1):
            for name in ('r1', 'r2'):
                self.call(op='sync', consumer=name)
            with self.assertRaises(memory.MemoryError_) as e:
                self.call(op='sync', consumer='r3')
            self.assertEqual(e.exception.code, 'capacity')
            # Retire one, leaving a tombstone. The combined bound still holds and the
            # tombstone survives, so r1 is still told it was retired.
            self.s.db.execute('UPDATE cursors SET updated=? WHERE consumer=?',
                              (time.time()-memory.CONSUMER_TTL-1, 'r1'))
            self.s.db.commit()
            self.s.expire()
            self.assertEqual(self.s.db.execute('SELECT count(*) FROM retired').fetchone()[0], 1)
            with self.assertRaises(memory.MemoryError_) as e:
                self.call(op='sync', consumer='r1')
            self.assertEqual(e.exception.code, 'consumer_retired')

    def test_deduplication_state_is_retained_only_to_its_deadline(self):
        self.note('kept', key='fresh', deadline=time.time()+600)
        self.s.db.execute('UPDATE idem SET deadline=?', (time.time()-1,))
        self.s.db.commit()
        self.s.expire()
        self.assertEqual(self.s.db.execute('SELECT count(*) FROM idem').fetchone()[0], 0)

    def test_an_idempotent_write_requires_a_deadline_fixed_before_the_first_send(self):
        with self.assertRaises(memory.MemoryError_) as e:
            self.s.note('writer', 'finding', 'no deadline', key='k1')
        self.assertEqual(e.exception.code, 'invalid_request')
        with self.assertRaises(memory.MemoryError_) as e:
            self.s.note('writer', 'finding', 'too far', key='k1',
                        deadline=time.time()+memory.IDEM_TTL+60)
        self.assertEqual(e.exception.code, 'invalid_request')

    def test_a_retry_after_its_deadline_is_refused_rather_than_appended(self):
        """Real time advances past one unchanged deadline; nothing is substituted."""
        deadline = time.time() + 0.3
        first = self.s.note('writer', 'finding', 'uncertain outcome', key='k1', deadline=deadline)
        self.assertFalse(first['duplicate'])
        while time.time() <= deadline:
            time.sleep(0.05)
        # The row may still be present, because cleanup runs on an interval. Expiry is
        # decided by the clock, so the retry must be refused either way.
        with self.assertRaises(memory.MemoryError_) as e:
            self.s.note('writer', 'finding', 'uncertain outcome', key='k1', deadline=deadline)
        self.assertEqual(e.exception.code, 'retry_deadline_expired')
        self.assertEqual(len(self.s.live()), 1)

    def test_a_duplicate_carrying_a_changed_deadline_is_refused(self):
        deadline = time.time() + 600
        self.s.note('writer', 'finding', 'same content', key='k1', deadline=deadline)
        with self.assertRaises(memory.MemoryError_) as e:
            self.s.note('writer', 'finding', 'same content', key='k1', deadline=deadline + 60)
        # Accepting it would let a caller extend deduplication indefinitely while the
        # service reported a horizon it never agreed to.
        self.assertEqual(e.exception.code, 'idempotency_conflict')
        repeated = self.s.note('writer', 'finding', 'same content', key='k1', deadline=deadline)
        self.assertTrue(repeated['duplicate'])
        self.assertEqual(repeated['deadline'], deadline)

    def test_a_deadline_must_be_a_finite_number(self):
        for bad in (float('inf'), float('nan'), 'soon'):
            with self.assertRaises(memory.MemoryError_) as e:
                self.s.note('writer', 'finding', 'body', key='kx', deadline=bad)
            self.assertEqual(e.exception.code, 'invalid_request')

    def test_the_reported_author_is_part_of_the_content_fingerprint(self):
        deadline = time.time() + 600
        self.s.note('writer', 'finding', 'same body', key='k1', deadline=deadline, author='peer-a')
        with self.assertRaises(memory.MemoryError_) as e:
            self.s.note('writer', 'finding', 'same body', key='k1', deadline=deadline,
                        author='peer-b')
        self.assertEqual(e.exception.code, 'idempotency_conflict')
        # The transport pid is not content: the same write relayed by another process
        # deduplicates rather than conflicting.
        repeat = self.s.note('writer', 'finding', 'same body', key='k1', deadline=deadline,
                             author='peer-a', pid=99999)
        self.assertTrue(repeat['duplicate'])


class SnapshotTests(Base):
    def test_a_frozen_snapshot_survives_revocation_and_reclamation_during_pagination(self):
        seqs = [self.note(f'entry {i}') for i in range(4)]
        page = self.call(op='sync', consumer='reader')
        with patch.object(memory,'FRAME_BUDGET',1200):
            page = self.call(op='sync', consumer='reader')
        total, sid = page['total'], page['snapshot_id']
        # Mutate hard while the reader is mid-snapshot: revoke a member, and expire
        # plus reclaim another. Neither may change this reader's remaining pages.
        self.note('withdrawn', kind='directive', revokes=seqs[0])
        self.s.db.execute('UPDATE entries SET expires=? WHERE seq=?', (time.time()-1, seqs[1]))
        self.s.db.commit()
        self.s.reclaim()
        self.assertIsNone(self.s.db.execute('SELECT seq FROM entries WHERE seq=?',
                                            (seqs[1],)).fetchone())
        seen = list(page['entries'])
        while page['more']:
            page = self.call(op='sync', consumer='reader', snapshot_id=sid,
                             page_token=page['page_token'])
            seen.extend(page['entries'])
        self.assertEqual(page['total'], total)
        self.assertEqual(len(seen), total)
        self.assertEqual({e['seq'] for e in seen}, set(seqs))
        done = self.call(op='ack', consumer='reader', snapshot_id=sid)
        self.assertTrue(done['complete'])

    def test_revocation_after_the_head_arrives_as_a_later_delta(self):
        target = self.note('standing rule', kind='directive')
        self.drain()
        revocation = self.note('withdrawn', kind='directive', revokes=target)
        delta = self.call(op='sync', consumer='reader')
        self.assertEqual(delta['kind'], 'delta')
        self.assertEqual([e['seq'] for e in delta['entries']], [revocation])
        self.assertEqual(delta['entries'][0]['revokes'], target)

    def test_an_expired_snapshot_restarts_without_advancing_progress(self):
        self.note('one')
        page = self.call(op='sync', consumer='reader')
        stale = page['snapshot_id']
        self.s.db.execute('UPDATE snapshots SET created=? WHERE id=?',
                          (time.time()-memory.SNAPSHOT_TTL-1, stale))
        self.s.db.commit()
        # The documented recovery is to restart sync. That must actually work rather
        # than selecting the same expired snapshot forever.
        restarted = self.call(op='sync', consumer='reader')
        self.assertEqual(restarted['kind'], 'snapshot')
        self.assertNotEqual(restarted['snapshot_id'], stale)
        self.assertEqual(self.s.cursor('reader')[0], 0)
        self.assertTrue(self.call(op='ack', consumer='reader',
                                  snapshot_id=restarted['snapshot_id'])['complete'])
        self.assertEqual(self.s.cursor('reader')[0], self.s.head())

    def test_a_stale_page_token_cannot_be_applied_to_another_snapshot(self):
        self.note('one')
        first = self.call(op='sync', consumer='reader')
        with self.assertRaises(memory.MemoryError_) as e:
            self.call(op='sync', consumer='reader', snapshot_id='0'*32, page_token=0)
        self.assertEqual(e.exception.code, 'stale_page_token')
        with self.assertRaises(memory.MemoryError_) as e:
            self.call(op='sync', consumer='reader', snapshot_id=first['snapshot_id'], page_token=99)
        self.assertEqual(e.exception.code, 'stale_page_token')

    def test_completion_is_tracked_by_the_server_not_the_caller(self):
        with patch.object(memory,'FRAME_BUDGET',900):
            for i in range(4):
                self.note(f'entry {i}')
            page = self.call(op='sync', consumer='reader')
            self.assertTrue(page['more'])
            # A caller claiming it finished must not be believed.
            with self.assertRaises(memory.MemoryError_) as e:
                self.call(op='ack', consumer='reader', snapshot_id=page['snapshot_id'])
            self.assertEqual(e.exception.code, 'snapshot_incomplete')
            self.assertEqual(self.s.cursor('reader')[0], 0)

    def test_a_retained_acknowledgement_replays_after_a_lost_response(self):
        self.note('one')
        first = self.drain()
        self.assertFalse(first['replayed'])
        sid = first['snapshot']
        # The caller never saw the response and retries the identical request.
        replay = self.call(op='ack', consumer='reader', snapshot_id=sid)
        self.assertTrue(replay['complete'])
        self.assertTrue(replay['replayed'])
        self.assertEqual(replay['cursor'], first['cursor'])

    def test_an_acknowledged_empty_store_does_not_resnapshot_forever(self):
        self.drain()
        self.assertEqual(self.call(op='sync', consumer='reader')['kind'], 'delta')
        self.assertEqual(self.call(op='sync', consumer='reader')['kind'], 'delta')

    def test_a_cursor_below_the_floor_returns_to_a_snapshot(self):
        self.note('one')
        self.drain()
        self.s.set_meta('floor', self.s.head()+5)
        self.s.db.commit()
        self.assertEqual(self.call(op='sync', consumer='reader')['kind'], 'snapshot')

    def test_delta_acknowledgement_is_monotonic_and_bounded_by_issuance(self):
        self.note('one')
        self.drain()
        self.note('two')
        delta = self.call(op='sync', consumer='reader')
        with self.assertRaises(memory.MemoryError_) as e:
            self.call(op='ack', consumer='reader', through=999)
        self.assertEqual(e.exception.code, 'not_issued')
        self.call(op='ack', consumer='reader', through=delta['next_cursor'])
        replay = self.call(op='ack', consumer='reader', through=1)
        self.assertIn('ignored', replay)

    def test_a_retired_consumer_is_told_rather_than_silently_restarted(self):
        self.note('one')
        self.drain()
        self.s.db.execute('UPDATE cursors SET updated=? WHERE consumer=?',
                          (time.time()-memory.CONSUMER_TTL-1, 'reader'))
        self.s.db.commit()
        self.s.reclaim()
        with self.assertRaises(memory.MemoryError_) as e:
            self.call(op='sync', consumer='reader')
        self.assertEqual(e.exception.code, 'consumer_retired')
        self.assertIn('new consumer key', str(e.exception))

    def test_a_gap_above_the_cursor_returns_to_a_snapshot(self):
        """A removed event above the cursor must not leave a reader polling forever."""
        self.note('event one')
        self.drain()
        self.note('event two', kind='status', expires=time.time()-1)
        self.s.reclaim()
        # The boundary comes from what was removed, so it describes this tail gap. A
        # boundary taken from the lowest surviving row would sit below the cursor and
        # the reader would be told there is more while receiving nothing.
        self.assertEqual(self.s.floor(), 2)
        out = self.call(op='sync', consumer='reader')
        self.assertEqual(out['kind'], 'snapshot')
        self.assertFalse(out['more'] and not out['entries'])

    def test_an_interior_gap_also_returns_to_a_snapshot(self):
        self.note('one')
        self.note('two', kind='status', expires=time.time()-1)
        self.note('three')
        self.s.reclaim()
        self.assertEqual(self.s.floor(), 2)
        self.assertEqual(self.call(op='sync', consumer='fresh')['kind'], 'snapshot')

    def test_a_cleared_snapshot_keeps_its_obligation(self):
        self.note('one')
        self.drain()
        self.note('two')
        page = self.call(op='sync', consumer='reader')
        self.assertEqual(page['kind'], 'delta')
        self.call(op='ack', consumer='reader', through=page['next_cursor'])
        self.s.db.execute('UPDATE cursors SET snapshot=?,resnapshot=0 WHERE consumer=?',
                          ('deadbeef'*4, 'reader'))
        self.s.db.commit()
        # The snapshot is unusable and the cursor sits above the floor, so without the
        # retained obligation this bootstrapped reader would fall through to deltas and
        # skip everything the snapshot was carrying.
        self.assertEqual(self.call(op='sync', consumer='reader')['kind'], 'snapshot')

    def test_a_snapshot_is_bound_to_the_consumer_that_opened_it(self):
        self.note('one')
        page = self.call(op='sync', consumer='reader-a')
        for op in (dict(op='ack', consumer='reader-b', snapshot_id=page['snapshot_id']),
                   dict(op='sync', consumer='reader-b', snapshot_id=page['snapshot_id'],
                        page_token=0)):
            with self.assertRaises(memory.MemoryError_) as e:
                self.call(**op)
            self.assertEqual(e.exception.code, 'foreign_snapshot')

    def test_continuation_requires_the_snapshot_identity(self):
        self.note('one')
        page = self.call(op='sync', consumer='reader')
        with self.assertRaises(memory.MemoryError_) as e:
            self.call(op='sync', consumer='reader', page_token=1)
        self.assertEqual(e.exception.code, 'stale_page_token')

    def test_numeric_acknowledgement_cannot_bootstrap_or_bypass_a_snapshot(self):
        self.note('one')
        with self.assertRaises(memory.MemoryError_) as e:
            self.call(op='ack', consumer='reader', through=1)
        self.assertEqual(e.exception.code, 'not_bootstrapped')
        self.call(op='sync', consumer='reader')
        with self.assertRaises(memory.MemoryError_) as e:
            self.call(op='ack', consumer='reader', through=1)
        self.assertEqual(e.exception.code, 'snapshot_open')
        self.assertEqual(self.s.cursor('reader')[0], 0)

    def test_every_sync_refreshes_activity(self):
        self.note('one')
        self.drain()
        self.s.db.execute('UPDATE cursors SET updated=? WHERE consumer=?', (0, 'reader'))
        self.s.db.commit()
        # An idle poll returns nothing, but it is still activity; without the refresh an
        # actively polling consumer would eventually be retired underneath itself.
        self.call(op='sync', consumer='reader')
        self.assertGreater(self.s.cursor('reader')[4] if False else
                           self.s.db.execute('SELECT updated FROM cursors WHERE consumer=?',
                                             ('reader',)).fetchone()[0], 0)

    def test_directives_lead_the_snapshot(self):
        self.note('a finding')
        self.note('no option menus', kind='directive')
        page = self.call(op='sync', consumer='reader')
        self.assertEqual(page['entries'][0]['type'], 'directive')


class FramingTests(Base):
    def test_a_page_is_bounded_by_encoded_bytes(self):
        body = 'x'*(memory.MAX_BODY-1)
        count = 40
        for _ in range(count):
            # `decision` carries no snapshot tail cap, so the page limit under test is
            # the byte budget rather than the per-type cap.
            self.note(body, kind='decision')
        # 40 maximum-size entries exceed one frame, which is the case a row-count
        # limit of 50 or 200 would have produced an undeliverable response for.
        page = self.call(op='sync', consumer='reader')
        self.assertLessEqual(len(memory.encode(page['entries'])), memory.LIMIT)
        self.assertTrue(page['more'])
        self.assertLess(len(page['entries']), count)
        seen = len(page['entries'])
        while page['more']:
            page = self.call(op='sync', consumer='reader', snapshot_id=page['snapshot_id'],
                             page_token=page['page_token'])
            self.assertLessEqual(len(memory.encode(page['entries'])), memory.LIMIT)
            seen += len(page['entries'])
        self.assertEqual(seen, count)

    def test_an_undeliverable_entry_is_refused_at_admission(self):
        """Storing what can never be delivered would block its snapshot page forever."""
        with patch.object(memory,'FRAME_BUDGET',64):
            with self.assertRaises(memory.MemoryError_) as e:
                self.note('a body that cannot fit a tiny page budget')
            self.assertEqual(e.exception.code, 'entry_too_large')
        self.assertEqual(self.s.live(), [])

    def test_an_already_stored_undeliverable_entry_is_reported_not_dropped(self):
        self.note('stored while the budget was generous')
        with patch.object(memory,'FRAME_BUDGET',64):
            with self.assertRaises(memory.MemoryError_) as e:
                self.call(op='sync', consumer='reader')
            self.assertEqual(e.exception.code, 'entry_too_large')

    def test_recall_continues_and_reports_truncation_truthfully(self):
        with patch.object(memory,'ROW_WINDOW',3):
            for i in range(7):
                self.note(f'match {i}', kind='decision')
            first = self.call(op='recall', consumer='r', query='match')
            self.assertTrue(first['more'])
            self.assertIsNotNone(first['next_before'])
            seen = [e['seq'] for e in first['entries']]
            token = first['next_before']
            while token:
                page = self.call(op='recall', consumer='r', query='match', before=token)
                seen.extend(e['seq'] for e in page['entries'])
                token = page['next_before']
            # Continuation must reach every match; a window that stopped early while
            # reporting more as false would hide results behind a false ending.
            self.assertEqual(len(seen), 7)
            self.assertEqual(len(set(seen)), 7)

    def test_status_paginates_its_consumer_list(self):
        with patch.object(memory,'ROW_WINDOW',2):
            for name in ('c1','c2','c3','c4','c5'):
                self.call(op='sync', consumer=name)
            names, token = [], ''
            while True:
                page = self.call(op='status', after=token)
                names.extend(c['consumer'] for c in page['consumers'])
                token = page.get('next_after')
                if not token:
                    break
            self.assertEqual(names, ['c1','c2','c3','c4','c5'])


class SearchTests(Base):
    def paths(self):
        found = [False]
        probe = sqlite3.connect(':memory:')
        try:
            probe.execute('CREATE VIRTUAL TABLE t USING fts5(body)')
            found.append(True)
        except sqlite3.Error:
            pass
        finally:
            probe.close()
        return found

    def test_recall_applies_the_same_liveness_rule_as_sync(self):
        for fts in self.paths():
            with self.subTest(fts=fts), tempfile.TemporaryDirectory() as d:
                s = memory.Store(Path(d)/'m.sqlite3', REPO, fts=fts)
                self.addCleanup(s.close)
                svc = memory.Service(d, REPO, s)
                self.assertEqual(s.fts, fts)
                kept = s.note('w','gotcha','registry keeps this one')['seq']
                gone = s.note('w','gotcha','registry loses this one')['seq']
                s.note('w','gotcha','registry replacement', supersedes=gone)
                revoked = s.note('w','directive','registry revoked rule')['seq']
                s.note('w','directive','registry withdrawal', revokes=revoked)
                expired = s.note('w','status','registry expired note',
                                 expires=time.time()-1)['seq']
                hits = {e['seq'] for e in svc.command(dict(op='recall',query='registry'),1)['entries']}
                self.assertIn(kept, hits)
                for absent in (gone, revoked, expired):
                    self.assertNotIn(absent, hits)

    def test_fallback_escapes_wildcards_and_search_survives_reclamation(self):
        with tempfile.TemporaryDirectory() as d:
            s = memory.Store(Path(d)/'m.sqlite3', REPO, fts=False)
            self.addCleanup(s.close)
            svc = memory.Service(d, REPO, s)
            s.note('w','finding','literal percent % here')
            s.note('w','finding','no wildcard')
            self.assertEqual(len(svc.command(dict(op='recall',query='%'),1)['entries']), 1)

    def test_an_index_is_backfilled_when_a_store_gains_search_on_reopen(self):
        if True not in self.paths():
            self.skipTest('this runtime provides no FTS5')
        with tempfile.TemporaryDirectory() as d:
            path = Path(d)/'m.sqlite3'
            plain = memory.Store(path, REPO, fts=False)
            plain.note('w','finding','written before the index existed')
            plain.close()
            upgraded = memory.Store(path, REPO)
            self.addCleanup(upgraded.close)
            self.assertTrue(upgraded.fts)
            svc = memory.Service(d, REPO, upgraded)
            self.assertEqual(len(svc.command(dict(op='recall',query='before'),1)['entries']), 1)

    def test_a_partially_indexed_store_is_rebuilt_on_reopen(self):
        """Emptiness is the wrong completeness test for a search index."""
        if True not in self.paths():
            self.skipTest('this runtime provides no FTS5')
        with tempfile.TemporaryDirectory() as d:
            path = Path(d)/'m.sqlite3'
            first = memory.Store(path, REPO)
            first.note('w','finding','searchable one')
            first.close()
            # A runtime without FTS writes an entry the index never sees.
            without = memory.Store(path, REPO, fts=False)
            without.note('w','finding','searchable two')
            without.close()
            again = memory.Store(path, REPO)
            self.addCleanup(again.close)
            svc = memory.Service(d, REPO, again)
            total = again.db.execute('SELECT count(*) FROM entries').fetchone()[0]
            hits = svc.command(dict(op='recall', query='searchable'), 1)['entries']
            self.assertEqual(len(hits), total)

    def test_reclamation_maintains_the_search_index(self):
        if True not in self.paths():
            self.skipTest('this runtime provides no FTS5')
        with tempfile.TemporaryDirectory() as d:
            s = memory.Store(Path(d)/'m.sqlite3', REPO)
            self.addCleanup(s.close)
            svc = memory.Service(d, REPO, s)
            s.note('w','status','ephemeral marker', expires=time.time()-1)
            s.reclaim()
            # A contentless index keeps its rows unless they are deleted explicitly,
            # which would resurrect the entry through search alone.
            self.assertEqual(svc.command(dict(op='recall',query='ephemeral'),1)['entries'], [])


class LifecycleTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.repo_path = git_repo(self.tmp.name)
        self.state = Path(self.tmp.name)/'state'
        self.repo = memory.repo_identity(self.repo_path)
        self.home = memory.state_dir(self.state, self.repo)
        self.running = []
        self.addCleanup(self.kill_all)

    def kill_all(self):
        for p in self.running:
            if p.poll() is None:
                p.kill()
                p.wait(timeout=10)
            # Close the pipes explicitly; leaving them to the collector produces
            # unclosed-file warnings that hide real ones.
            for stream in (p.stdout, p.stderr):
                if stream and not stream.closed:
                    stream.close()

    def spawn(self, *args, env=None):
        p = subprocess.Popen([sys.executable, 'memory.py', '--state-dir', str(self.state),
                              '--repo-path', str(self.repo_path), *args],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                             env=env)
        self.running.append(p)
        return p

    def wait_for_socket(self, timeout=20, pid=None):
        """Wait until the service actually answers, not merely until a socket exists.

        A killed service leaves its socket behind, so an existence check can return
        while connections are still refused.
        """
        control = platform_support.control_socket_path(self.home)
        deadline = time.monotonic()+timeout
        while time.monotonic() < deadline:
            try:
                reply = self.client(op='hello')
                if reply.get('ok') and (pid is None or reply['result']['pid'] == pid):
                    return control
            except (ConnectionRefusedError, FileNotFoundError, OSError, ValueError):
                pass
            time.sleep(.05)
        self.fail('service did not become reachable')

    def client(self, **payload):
        return asyncio.run(memory.request(self.home, payload))

    def test_recovery_is_reachable_from_the_command_line(self):
        """The documented recovery path must exist as a subcommand, not only as an op.

        `recover` was implemented in the service and described in the contract while
        argparse offered no such subcommand, so an operator following the documentation had
        no way to take it.
        """
        server = self.spawn('serve')
        self.wait_for_socket(pid=server.pid)
        client = self.spawn('recover')
        stdout, stderr = client.communicate(timeout=20)
        self.assertEqual(client.returncode, 0, stderr)
        reply = json.loads(stdout)
        self.assertTrue(reply.get('ok'), reply)
        self.assertTrue(reply['result']['recovered'])
        self.assertIsNone(reply['result']['blocked'])
        # The service is still serving afterwards.
        self.assertTrue(self.client(op='hello')['ok'])

    def test_concurrent_first_start_elects_one_service(self):
        # Three real starts race from cold. Exactly one may serve; the others must
        # reuse it rather than bind, fail, or corrupt the state directory.
        racers = [self.spawn('serve') for _ in range(3)]
        self.wait_for_socket()
        reused, serving = [], []
        deadline = time.monotonic()+30
        while time.monotonic() < deadline and len(reused)+len(serving) < 3:
            for p in racers:
                if p in reused or p in serving:
                    continue
                if p.poll() is not None:
                    reused.append(p)
                elif self.client(op='hello')['result']['pid'] == p.pid:
                    serving.append(p)
            time.sleep(.1)
        self.assertEqual(len(serving), 1, 'exactly one service must serve')
        self.assertEqual(len(reused), 2)
        for p in reused:
            self.assertEqual(p.returncode, 0, p.stderr.read())
            self.assertEqual(json.loads(p.stdout.read())['status'], 'already_running')

    def test_an_unclean_exit_is_recovered_only_after_death_is_proved(self):
        first = self.spawn('serve')
        control = self.wait_for_socket()
        owner = memory.read_owner(self.home)
        self.assertEqual(owner['pid'], first.pid)
        self.assertFalse(memory.owner_is_dead(owner))
        # A live owner is never removed, whatever a probe says.
        with self.assertRaises(memory.MemoryError_) as e:
            memory.bind_exclusive(self.home, self.repo, 'g')
        self.assertEqual(e.exception.code, 'socket_in_use')
        self.assertTrue(control.exists())
        first.send_signal(signal.SIGKILL)
        first.wait(timeout=10)
        self.assertTrue(control.exists(), 'a killed service leaves its socket behind')
        self.assertTrue(memory.owner_is_dead(memory.read_owner(self.home)))
        second = self.spawn('serve')
        self.wait_for_socket(pid=second.pid)

    def test_a_live_unrelated_owner_blocks_recovery(self):
        memory.private_dir(self.home)
        control = platform_support.control_socket_path(self.home)
        memory.private_dir(control.parent)
        holder = asyncio.run(self._hold(control))
        self.addCleanup(holder.close)
        memory.write_owner(self.home, control, 'g', self.repo)
        self.assertFalse(memory.owner_is_dead(memory.read_owner(self.home)))
        with self.assertRaises(memory.MemoryError_):
            memory.bind_exclusive(self.home, self.repo, 'g2')
        self.assertTrue(control.exists())

    async def _hold(self, control):
        import socket as sk
        s = sk.socket(sk.AF_UNIX, sk.SOCK_STREAM)
        s.bind(str(control))
        s.listen(1)
        return s

    def test_a_crash_between_commit_and_response_is_survived_and_recovered(self):
        """The window is commit to send, and recovery runs through the real path."""
        crasher = self.spawn('serve', env=dict(os.environ, MEMORY_TEST_CRASH_AFTER_COMMIT='1'))
        self.wait_for_socket(pid=crasher.pid)
        deadline = time.time() + 600
        with self.assertRaises(memory.MemoryError_) as e:
            self.client(op='note', consumer='cli-1', type='decision',
                        body='committed, never answered', key='k1', deadline=deadline)
        self.assertEqual(e.exception.code, 'no_reply')
        crasher.wait(timeout=10)
        self.assertEqual(crasher.returncode, 70)
        control = platform_support.control_socket_path(self.home)
        # No manual cleanup: the replacement must recover through ownership evidence,
        # which is the path a real operator depends on.
        self.assertTrue(control.exists())
        survivor = self.spawn('serve')
        self.wait_for_socket(pid=survivor.pid)
        status = self.client(op='status')['result']
        self.assertEqual(status['usage']['entries'], 1, 'the committed write must survive')
        retry = self.client(op='note', consumer='cli-1', type='decision',
                            body='committed, never answered', key='k1', deadline=deadline)
        # The caller never saw a response, so its retry must deduplicate rather than
        # append a second copy of work that already happened.
        self.assertTrue(retry['result']['duplicate'])
        self.assertEqual(self.client(op='status')['result']['usage']['entries'], 1)

    def test_maximum_size_entries_round_trip_through_the_real_socket(self):
        self.spawn('serve')
        self.wait_for_socket()
        body = 'x'*(memory.MAX_BODY-1)
        for _ in range(8):
            self.assertTrue(self.client(op='note', consumer='w', type='finding', body=body)['ok'])
        seen, page = 0, self.client(op='sync', consumer='reader')['result']
        while True:
            seen += len(page['entries'])
            if not page['more']:
                break
            page = self.client(op='sync', consumer='reader', snapshot_id=page['snapshot_id'],
                               page_token=page['page_token'])['result']
        self.assertEqual(seen, 8)
        self.assertTrue(self.client(op='ack', consumer='reader',
                                    snapshot_id=page['snapshot_id'])['result']['complete'])

    def test_a_listener_without_an_ownership_record_is_refused(self):
        self.spawn('serve')
        self.wait_for_socket()
        (self.home/'owner.json').unlink()
        # Something answering on the socket is evidence that something listens, not
        # that it is this repository's healthy service.
        with self.assertRaises(memory.MemoryError_) as e:
            asyncio.run(memory.verify_running(self.home, self.repo))
        self.assertEqual(e.exception.code, 'unknown_owner')

    def test_a_record_disagreeing_with_the_running_service_is_refused(self):
        self.spawn('serve')
        self.wait_for_socket()
        record = memory.read_owner(self.home)
        for field, value in (('generation', 'not-the-running-one'),
                             ('socket', '/tmp/somewhere-else.sock'),
                             ('repo', 'f'*16)):
            tampered = dict(record, **{field: value})
            (self.home/'owner.json').write_text(json.dumps(tampered))
            with self.assertRaises(memory.MemoryError_) as e:
                asyncio.run(memory.verify_running(self.home, self.repo))
            self.assertEqual(e.exception.code, 'ownership_mismatch', field)

    def test_the_serving_process_never_holds_the_start_lock(self):
        """Invariant 1 from the design contract, asserted rather than reasoned about.

        A starting caller holds this lock while it probes, so a service that needed it
        to answer could never answer. This is the shape of the readiness deadlock the
        project already shipped once.
        """
        self.spawn('serve')
        self.wait_for_socket()
        import fcntl
        with (self.home/'start.lock').open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            # The lock is ours, and the service must still answer while we hold it.
            self.assertTrue(self.client(op='hello')['ok'])
            fcntl.flock(lock, fcntl.LOCK_UN)

    def test_cleanup_removes_only_what_the_generation_still_owns(self):
        self.spawn('serve')
        control = self.wait_for_socket()
        # A predecessor tidying up late must not remove a successor's endpoint.
        self.assertFalse(memory.release(self.home, control, 'some-older-generation'))
        self.assertTrue(control.exists())
        self.assertTrue((self.home/'owner.json').exists())
        self.assertTrue(self.client(op='hello')['ok'])

    def test_reuse_is_refused_for_another_repository(self):
        self.spawn('serve')
        self.wait_for_socket()
        with self.assertRaises(memory.MemoryError_) as e:
            asyncio.run(memory.verify_running(self.home, 'f'*16))
        self.assertEqual(e.exception.code, 'foreign_service')

    def test_cli_records_scope_provenance_and_expiry(self):
        self.spawn('serve')
        self.wait_for_socket()
        p = subprocess.run([sys.executable,'memory.py','--state-dir',str(self.state),
                            '--repo-path',str(self.repo_path),'--consumer','cli-1','note',
                            'task scoped rule','--type','directive','--scope','task',
                            '--scope-target','T-7','--author','reported by a peer',
                            '--expires',str(time.time()+3600)],capture_output=True,text=True)
        self.assertEqual(p.returncode, 0, p.stderr)
        entry = self.client(op='sync', consumer='reader')['result']['entries'][0]
        self.assertEqual(entry['scope'], 'task')
        self.assertEqual(entry['scope_target'], 'T-7')
        self.assertEqual(entry['author'], 'reported by a peer')
        self.assertIsNotNone(entry['expires'])

    def cli(self, *args):
        return subprocess.run([sys.executable,'memory.py','--state-dir',str(self.state),
                               '--repo-path',str(self.repo_path), *args],
                              capture_output=True, text=True)

    def test_stop_waits_for_the_service_to_exit_before_reporting(self):
        service = self.spawn('serve')
        control = self.wait_for_socket(pid=service.pid)
        result = self.cli('stop')
        self.assertEqual(result.returncode, 0, result.stderr)
        # Completion means the endpoint is gone and the process has exited, not merely
        # that a stop request was accepted.
        self.assertEqual(json.loads(result.stdout)['status'], 'stopped')
        self.assertFalse(control.exists())
        service.wait(timeout=10)
        self.assertEqual(service.returncode, 0)

    def test_a_request_in_flight_is_drained_before_the_store_closes(self):
        """Stop must finish accepted work, and report only once the instance has gone."""
        import threading
        service = self.spawn('serve', env=dict(os.environ, MEMORY_TEST_REPLY_DELAY='2'))
        control = self.wait_for_socket(pid=service.pid)
        generation = memory.read_owner(self.home)['generation']
        outcome = {}

        def slow_write():
            try:
                outcome['reply'] = self.client(op='note', consumer='w', type='decision',
                                               body='written while stopping')
            except Exception as exc:                      # noqa: BLE001 - recorded, asserted below
                outcome['error'] = exc

        worker = threading.Thread(target=slow_write)
        worker.start()
        time.sleep(0.5)                                   # let it reach the delay
        result = json.loads(self.cli('stop').stdout)
        worker.join(timeout=30)
        # The accepted request was answered rather than cut off by a closing store.
        self.assertNotIn('error', outcome, outcome.get('error'))
        self.assertTrue(outcome['reply']['ok'], outcome['reply'])
        self.assertEqual(result['status'], 'stopped')
        self.assertEqual(result['generation'], generation)
        self.assertFalse(control.exists())
        service.wait(timeout=10)
        # Restart: the drained write is durable, and the new instance is a new generation.
        successor = self.spawn('serve')
        self.wait_for_socket(pid=successor.pid)
        self.assertEqual(self.client(op='status')['result']['usage']['entries'], 1)
        self.assertNotEqual(memory.read_owner(self.home)['generation'], generation)

    def test_a_stop_addressed_to_another_instance_is_refused(self):
        """The target is validated where the state changes, not where it was read."""
        service = self.spawn('serve')
        self.wait_for_socket(pid=service.pid)
        record = memory.read_owner(self.home)
        refused = self.client(op='stop', repo=self.repo, generation='a-different-generation')
        self.assertFalse(refused['ok'])
        self.assertEqual(refused['code'], 'not_this_instance')
        wrong_repo = self.client(op='stop', repo='f'*16, generation=record['generation'])
        self.assertEqual(wrong_repo['code'], 'wrong_repository')
        # Still serving: neither request was allowed to stop it.
        self.assertTrue(self.client(op='hello')['ok'])
        self.assertIsNone(service.poll())

    def test_a_stale_generation_reports_superseded_and_leaves_the_successor_running(self):
        service = self.spawn('serve')
        self.wait_for_socket(pid=service.pid)
        record = memory.read_owner(self.home)
        # Simulate a successor having replaced the endpoint between the caller reading the
        # record and connecting: the record names a generation the live service does not
        # have, exactly as a stale read would.
        (self.home/'owner.json').write_text(json.dumps(dict(record, generation='stale-one')))
        result = memory.stop_service(self.home, self.repo, timeout=5)
        self.assertEqual(result['status'], 'stopped')
        self.assertTrue(result.get('superseded'))
        # The running service must be untouched.
        self.assertTrue(self.client(op='hello')['ok'])
        self.assertIsNone(service.poll())

    def test_the_cli_can_continue_recall_and_status_pages(self):
        self.spawn('serve')
        self.wait_for_socket()
        deadline = str(time.time()+600)
        for i in range(4):
            self.assertEqual(self.cli('--consumer','w','note',f'match {i}',
                                      '--type','decision','--key',f'k{i}',
                                      '--deadline',deadline).returncode, 0)
        for name in ('c1','c2'):
            self.cli('--consumer',name,'sync')
        # Both continuations must be reachable from the command line, or the tokens the
        # service returns are unusable by the interface it ships with.
        first = json.loads(self.cli('recall','match').stdout)['result']
        self.assertIn('next_before', first)
        token = first['entries'][-1]['seq']
        again = json.loads(self.cli('recall','match','--before',str(token)).stdout)['result']
        self.assertTrue(all(e['seq'] < token for e in again['entries']))
        listed = json.loads(self.cli('status','--after','c1').stdout)['result']
        self.assertEqual([c['consumer'] for c in listed['consumers']], ['c2'])

    def test_consumer_key_is_required_for_stateful_operations(self):
        p = subprocess.run([sys.executable,'memory.py','--state-dir',str(self.state),
                            '--repo-path',str(self.repo_path),'sync'],capture_output=True,text=True)
        self.assertNotEqual(p.returncode, 0)
        self.assertIn('--consumer', p.stderr)


if __name__ == '__main__':
    unittest.main()


class StorageBoundTests(Base):
    """The invariants the storage bound rests on, asserted rather than assumed.

    These deliberately assert invariants and not sizes. Different correct SQLite builds
    allocate different numbers of pages for the same content, so a test that demanded a
    particular figure would be testing the build rather than this design, and would fail
    on a correct one. See docs/STORAGE-BOUND-DERIVATION.md.
    """

    def wal_bytes(self):
        try:
            return os.stat(str(self.s.path) + '-wal').st_size
        except OSError:
            return 0

    def test_every_setting_the_bound_depends_on_reads_back_as_requested(self):
        for name, requested, expected in self.s.pragmas():
            with self.subTest(pragma=name):
                self.assertEqual(self.s.db.execute(f'PRAGMA {name}').fetchone()[0], expected,
                                 f'PRAGMA {name}={requested} did not take effect')

    def test_a_setting_that_does_not_take_effect_refuses_the_store(self):
        """A silently ignored pragma is the failure this readback exists to catch."""
        # A setting whose stated expectation cannot hold, so the readback must refuse.
        with patch.object(memory.Store, 'pragmas',
                          staticmethod(lambda: (('journal_mode', 'WAL', 'memory'),))):
            with self.assertRaises(memory.MemoryError_) as e:
                memory.Store(self.home/'refused.sqlite3', REPO)
            self.assertEqual(e.exception.code, 'unsupported_runtime')

    def test_no_shared_memory_file_is_created(self):
        """Exclusive locking holds the wal-index in memory, so the term is zero."""
        self.note('content that forces the log into use')
        self.assertFalse(Path(str(self.s.path) + '-shm').exists())

    def test_the_log_holds_nothing_before_a_commit(self):
        """This is the observation the one-frame-per-dirty-page argument rests on.

        With cache_spill=OFF the commit dirty list is the only path to the log, so a
        transaction that has not committed must not have written to it.
        """
        self.note('seed')
        self.s.reset_log()
        self.s.db.execute('BEGIN IMMEDIATE')
        try:
            for i in range(400):
                self.s.db.execute(
                    'INSERT INTO entries(seq,ts,type,scope,body,author,revision) '
                    'VALUES(?,?,?,?,?,?,?)', (10_000 + i, 1.0, 'finding', 'repo',
                                              'y' * 3000, 'a', 1))
            self.assertEqual(self.wal_bytes(), 0,
                             'a spill wrote to the log before the commit')
        finally:
            self.s.db.execute('ROLLBACK')

    def test_frames_never_exceed_dirty_pages_plus_the_padding_bound(self):
        """F <= D + P, with P bounded by the sector-size ceiling."""
        # The store's own boundary, not `with db:`. On an autocommit connection that
        # context manager wraps nothing, so each statement would commit separately and the
        # log would hold the sum of three hundred transactions rather than one.
        with self.s.transaction():
            for i in range(300):
                self.s.db.execute(
                    'INSERT INTO entries(seq,ts,type,scope,body,author,revision) '
                    'VALUES(?,?,?,?,?,?,?)', (20_000 + i, 1.0, 'finding', 'repo',
                                              'z' * 3000, 'a', 1))
        frames = (self.wal_bytes() - 32) / memory.FRAME_BYTES
        self.assertGreater(frames, 0, 'the commit wrote no frames, so nothing was measured')
        # Pages after the commit bound the pages it could have dirtied.
        self.assertLessEqual(frames, self.s.pages() + memory.PAD_FRAMES)
        self.assertLessEqual(self.wal_bytes(), memory.WAL_BUDGET_BYTES)

    def test_a_reset_that_did_not_happen_is_reported_not_assumed(self):
        """No single result proves a reset, so each part of the conjunction must refuse.

        The three cases are driven through a stub because exclusive locking makes a real
        foreign reader impossible, which is the point of exclusive locking. What is under
        test is this code's decision, not SQLite's checkpointing.
        """
        class Stub:
            def __init__(self, row):
                self.row = row

            def execute(self, *_a, **_k):
                return self
            def fetchall(self):
                return [self.row]

        real = self.s.db
        for row, why in (((1, 400, 0), 'a busy checkpoint'),
                         ((0, 400, 0), 'a passive checkpoint that moved nothing')):
            with self.subTest(case=why):
                self.s.db = Stub(row)
                try:
                    with self.assertRaises(memory.MemoryError_) as e:
                        self.s.reset_log()
                    self.assertEqual(e.exception.code, 'storage_blocked')
                finally:
                    self.s.db = real
        # A clean result with a log still on disk must also refuse: (0, 0, 0) is equally
        # what a store returns when no log has ever existed.
        self.s.db = Stub((0, 0, 0))
        try:
            with open(str(self.s.path) + '-wal', 'wb') as fh:
                fh.write(b'\x00' * 4096)
            with self.assertRaises(memory.MemoryError_) as e:
                self.s.reset_log()
            self.assertEqual(e.exception.code, 'storage_blocked')
        finally:
            self.s.db = real
            Path(str(self.s.path) + '-wal').unlink(missing_ok=True)

    def test_admission_never_admits_an_append_that_enforcement_will_refuse(self):
        """The two thresholds must be the same one, or a full store spins.

        Admission compares the pages already allocated; enforcement re-reads the count
        inside the transaction against a limit reduced by the commit-time allocation. While
        only enforcement subtracted it, a store resting in the gap admitted every append and
        rolled every one back, which reads to a caller as a store that accepts writes and
        loses them.
        """
        for body_len in (200, 4000, 8000):
            with self.subTest(body=body_len):
                # A fresh store for each size, so a size is never measured against a store
                # the previous size already filled.
                store = memory.Store(self.home/f'window-{body_len}.sqlite3', REPO)
                self.addCleanup(store.close)
                ceiling = store.pages() + 400
                with patch.object(memory, 'MAX_PAGES', ceiling), \
                     patch.object(memory, 'ORDINARY_MAX_PAGES', ceiling - 80), \
                     patch.object(memory, 'MAX_ENTRIES', 100_000), \
                     patch.object(memory, 'MAX_LOGICAL_BYTES', 1 << 40):
                    for _ in range(40_000):
                        try:
                            store.note('writer', 'decision', 'x' * body_len)
                        except memory.MemoryError_ as exc:
                            self.assertEqual(exc.code, 'capacity')
                            # Admission refused it. Enforcement refusing instead would mean
                            # the transaction ran and was rolled back for want of room.
                            self.assertNotIn('would leave', str(exc),
                                             'an admitted append was refused by enforcement')
                            break
                    else:
                        self.fail('growth was never bounded')
                    self.assertGreaterEqual(ceiling - store.pages(), 40,
                                            'ordinary appends consumed the reserve')

    def test_a_clean_reset_leaves_no_log(self):
        self.note('something to log')
        self.s.reset_log()
        self.assertEqual(self.wal_bytes(), 0)

    def test_the_engine_ceiling_rolls_back_whole_with_integrity_intact(self):
        """Past the page ceiling the engine refuses, and the refusal must be clean."""
        with patch.object(memory, 'MAX_PAGES', self.s.pages() + 40), \
             patch.object(memory, 'ORDINARY_MAX_PAGES', 1 << 30), \
             patch.object(memory, 'MAX_ENTRIES', 100_000), \
             patch.object(memory, 'MAX_LOGICAL_BYTES', 1 << 40):
            store = memory.Store(self.home/'tight.sqlite3', REPO)
            self.addCleanup(store.close)
            before = len(store.live())
            with self.assertRaises(memory.MemoryError_) as e:
                for i in range(10_000):
                    store.note('writer', 'decision', 'q' * 4000)
            self.assertEqual(e.exception.code, 'capacity')
            self.assertIn('rolled back', str(e.exception))
            self.assertEqual(store.db.execute('PRAGMA integrity_check').fetchone()[0], 'ok')
            self.assertGreater(len(store.live()), before, 'nothing was ever written')

    def test_a_build_without_in_memory_temporaries_is_refused(self):
        """On such a build the sub-journal size is unbounded and unaccounted for."""
        class Stub:
            def execute(self, sql, *_a):
                if 'compile_options' in sql:
                    return [('TEMP_STORE=0',)]
                raise AssertionError('nothing else should be reached')

        real = self.s.db
        self.s.db = Stub()
        try:
            with self.assertRaises(memory.MemoryError_) as e:
                self.s.require_temp_in_memory()
            self.assertEqual(e.exception.code, 'unsupported_runtime')
            self.assertIn('TEMP_STORE=0', str(e.exception))
        finally:
            self.s.db = real

    @unittest.skipUnless(sys.platform.startswith('linux'), 'reads /proc/self/fd')
    def test_no_sub_journal_reaches_disk(self):
        """Sub-journals open delete-on-close, so a directory listing would miss them.

        The verification is stronger on Linux than elsewhere for exactly this reason, and
        the derivation records that as a limit rather than claiming parity.
        """
        def unlinked():
            found = {}
            for fd in os.listdir('/proc/self/fd'):
                try:
                    target = os.readlink(f'/proc/self/fd/{fd}')
                    if '(deleted)' in target:
                        found[target] = os.fstat(int(fd)).st_size
                except OSError:
                    pass
            return found

        for i in range(60):
            self.note('body ' + str(i) * 200, kind='finding')
        with self.s.transaction():
            self.s.db.execute('UPDATE entries SET revision=revision+1')
            self.s.db.execute('DELETE FROM entries WHERE seq % 3 = 0')
        self.assertEqual({t: n for t, n in unlinked().items() if n}, {},
                         'a sub-journal or sorter spilled to disk')

    def test_a_known_incomplete_index_is_never_served_and_the_scan_is_complete(self):
        """Search must answer from one path, chosen on trust, not on emptiness."""
        needle = 'zqxjkv'
        seq = self.note(f'a body containing {needle} once')
        self.assertTrue(self.s.index_usable())
        found = self.call(op='recall', query=needle)
        self.assertEqual([e['seq'] for e in found['entries']], [seq])
        self.assertTrue(found['indexed'])
        # Mark the index short of the head, as a runtime without FTS would leave it.
        self.s.set_meta('indexed_through', -1)
        self.s.db.commit()
        self.assertFalse(self.s.index_usable())
        fallback = self.call(op='recall', query=needle)
        self.assertFalse(fallback['indexed'], 'the reply must say which path answered')
        self.assertEqual([e['seq'] for e in fallback['entries']], [seq],
                         'the fallback must be complete, not empty')

    def varied_body(self, body_len):
        """Distinct-token text. Repeated filler understates the index by two orders."""
        words = [f'w{i:05d}' for i in range(4000)]
        rng = random.Random(11)

        def make():
            out, size = [], 0
            while size < body_len:
                w = rng.choice(words)
                out.append(w)
                size += len(w) + 1
            return ' '.join(out)[:body_len]
        return make

    def full_store(self, name, ceiling, reserve, body_len=3000, limit=20_000):
        """A store opened UNDER the tested ceiling, filled through admission.

        Patching the constant after `setUp` opened a store left the engine holding its
        original limit, so the test exercised only this code's comparison and never the
        engine's. The store is created inside the patch instead, and the engine's limit is
        asserted rather than assumed.
        """
        words = [f'w{i:05d}' for i in range(4000)]
        rng = random.Random(11)

        def body():
            out, size = [], 0
            while size < body_len:
                w = rng.choice(words)
                out.append(w)
                size += len(w) + 1
            return ' '.join(out)[:body_len]

        store = memory.Store(self.home/name, REPO)
        self.addCleanup(store.close)
        self.assertEqual(store.db.execute('PRAGMA max_page_count').fetchone()[0], ceiling,
                         'the engine must hold the ceiling under test, not the default')
        written = []
        for _ in range(limit):
            try:
                written.append(store.note('writer', 'decision', body())['seq'])
            except memory.MemoryError_ as exc:
                self.assertEqual(exc.code, 'capacity')
                break
        else:
            self.fail('growth was never bounded')
        return store, written

    def test_the_reserve_survives_a_full_store_for_every_promised_transition(self):
        """At the threshold, every transition the contract promises must still commit.

        The contract promises a specific set: page issuance, acknowledgement, activity
        refresh, withdrawal, retirement and reclamation. It deliberately does NOT promise
        that a new consumer can register or that a fresh snapshot can be frozen at
        fullness -- those add rows a caller controls and are ordinary growth -- so this
        asserts they are refused rather than quietly treating them as covered.

        Bodies are distinct-token text, not a repeated character, because repeated filler
        produces an index two orders of magnitude smaller than real content and would let
        the index term escape the test entirely.
        """
        ceiling = 900
        reserve = memory.RESERVE_PAGES // 8
        with patch.object(memory, 'MAX_PAGES', ceiling), \
             patch.object(memory, 'ORDINARY_MAX_PAGES', ceiling - reserve), \
             patch.object(memory, 'RESERVE_PAGES', reserve), \
             patch.object(memory, 'MAX_ENTRIES', 100_000), \
             patch.object(memory, 'MAX_LOGICAL_BYTES', 1 << 40), \
             patch.object(memory, 'FRAME_BUDGET', 16_384):
            # A small frame budget so the snapshot really pages. With the production budget
            # a store this size answers in one page, and page issuance -- which the reserve
            # explicitly promises -- would never be exercised at all.
            store = memory.Store(self.home/'reserve.sqlite3', REPO)
            self.addCleanup(store.close)
            self.assertEqual(store.db.execute('PRAGMA max_page_count').fetchone()[0], ceiling,
                             'the engine must hold the ceiling under test, not the default')
            service = memory.Service(self.home, REPO, store)
            body = self.varied_body(3000)

            # A reader registers and takes a snapshot while there is still room, because
            # neither is promised at fullness.
            for _ in range(40):
                store.note('writer', 'decision', body())
            page = service.command(dict(op='sync', consumer='reader'), 4242)
            self.assertEqual(page['kind'], 'snapshot')

            written = []
            for _ in range(20_000):
                try:
                    written.append(store.note('writer', 'decision', body())['seq'])
                except memory.MemoryError_ as exc:
                    self.assertEqual(exc.code, 'capacity')
                    break
            else:
                self.fail('growth was never bounded')
            self.assertGreaterEqual(ceiling - store.pages(), reserve // 2,
                                    'ordinary appends consumed the reserve')
            self.assertLess(store.pages(), ceiling)
            self.assertIsNone(store.blocked)

            # Ordinary growth is refused: an append, a new consumer, and a fresh snapshot.
            for label, call in (
                    ('append', lambda: store.note('writer', 'decision', 'more')),
                    ('registration', lambda: service.command(
                        dict(op='sync', consumer='late-reader'), 4242)),
                    ('snapshot', lambda: memory.freeze(store, 'reader'))):
                with self.subTest(refused=label):
                    with self.assertRaises(memory.MemoryError_) as e:
                        call()
                    self.assertEqual(e.exception.code, 'capacity')

            # Page issuance on the snapshot taken earlier: promised, so it must commit.
            issued = 0
            while page.get('more'):
                page = service.command(dict(op='sync', consumer='reader',
                                            snapshot_id=page['snapshot_id'],
                                            page_token=page['page_token']), 4242)
                issued += 1
            self.assertGreater(issued, 0, 'no page was issued, so issuance was not exercised')

            # Acknowledgement: promised.
            self.assertTrue(service.command(dict(op='ack', consumer='reader',
                                                 snapshot_id=page['snapshot_id']), 4242))

            # Withdrawal: a control mutation drawing on reserved slots and pages.
            self.assertTrue(store.note('writer', 'directive', 'withdrawn', revokes=written[0]))

            # Activity refresh: promised.
            with store.progress():
                store.set_meta('probe-at-the-bound', '1')
            self.assertEqual(store.meta('probe-at-the-bound'), '1')

            # Retirement: a consumer past its lifetime leaves a tombstone, at fullness.
            store.db.execute('UPDATE cursors SET updated=? WHERE consumer=?',
                             (time.time() - memory.CONSUMER_TTL - 1, 'reader'))
            store.db.commit()
            store.expire()
            self.assertIsNotNone(store.db.execute(
                'SELECT 1 FROM retired WHERE consumer=?', ('reader',)).fetchone())

            # Reclamation must run rather than be refused.
            store.db.execute('UPDATE entries SET expires=? WHERE seq IN (%s)'
                             % ','.join(str(x) for x in written[:-3]), (time.time() - 1,))
            store.db.commit()
            at_bound = store.pages()
            store.reclaim()
            self.assertLess(store.pages(), at_bound)

            # A rebuild, once reclamation has made room for it again.
            store._reconcile_index()
            self.assertTrue(store.index_usable())
            self.assertLessEqual(store.pages(), ceiling)
            self.assertIsNone(store.blocked)


    def test_no_write_path_commits_without_resetting_the_log_first(self):
        """The bound is for one transaction after an empty log, so none may accumulate.

        Expiry and reclamation run many transactions. If any of them committed without a
        reset in between, the log would hold the sum of several transactions rather than
        the largest one, and the derived budget would describe nothing. The log is sampled
        while a real expire/reclaim runs, rather than inspected after it, because an
        accumulation that is checkpointed at the end leaves no trace afterwards.
        """
        wal = str(self.s.path) + '-wal'
        for i in range(memory.EXPIRY_BATCH * 3):
            self.note('entry ' + str(i) * 40, kind='finding')
        self.s.db.execute('UPDATE entries SET expires=?', (time.time() - 1,))
        self.s.db.commit()

        peak = [0]
        stop = [False]

        def sample():
            while not stop[0]:
                try:
                    peak[0] = max(peak[0], os.stat(wal).st_size)
                except OSError:
                    pass
                time.sleep(0.0005)

        watcher = threading.Thread(target=sample, daemon=True)
        watcher.start()
        removed = self.s.reclaim()
        stop[0] = True
        watcher.join()
        self.assertGreater(removed, memory.EXPIRY_BATCH,
                           'the run must span several batches for accumulation to be possible')
        budget = 32 + (self.s.pages() + memory.PAD_FRAMES) * memory.FRAME_BYTES
        self.assertLessEqual(peak[0], budget,
                            'the log grew beyond one transaction, so transactions accumulated')

    def test_a_refused_progress_transition_is_really_rolled_back(self):
        """The refusal must not be reported after the change has already committed."""
        self.note('seed')
        with patch.object(memory, 'MAX_PAGES', self.s.pages() - 1):
            with self.assertRaises(memory.MemoryError_) as e:
                with self.s.progress():
                    self.s.set_meta('committed-then-refused', '1')
            self.assertEqual(e.exception.code, 'capacity')
            self.assertIn('rolled back', str(e.exception))
        self.assertIsNone(self.s.meta('committed-then-refused'),
                          'the error said rolled back while the write had been applied')

    def test_a_blocked_store_still_reads_and_recovers_on_request(self):
        """Blocking writes must leave the reads, status and stop the error promises."""
        ceiling = 700
        with patch.object(memory, 'MAX_PAGES', ceiling), \
             patch.object(memory, 'ORDINARY_MAX_PAGES', ceiling - 40), \
             patch.object(memory, 'MAX_ENTRIES', 100_000), \
             patch.object(memory, 'MAX_LOGICAL_BYTES', 1 << 40):
            store, written = self.full_store('blocked.sqlite3', ceiling, 40)
            service = memory.Service(self.home, REPO, store)
            # Drive a progress transition past the engine ceiling: the case the reserve
            # exists to prevent, and the one that must block rather than be shrugged off.
            with self.assertRaises(memory.MemoryError_) as e:
                with store.progress():
                    for i in range(20_000):
                        store.db.execute(
                            'INSERT INTO entries(seq,ts,type,scope,body,author,revision) '
                            'VALUES(?,?,?,?,?,?,?)',
                            (500_000 + i, 1.0, 'finding', 'repo', 'p' * 3000, 'a', 1))
            self.assertEqual(e.exception.code, 'storage_blocked')
            self.assertIsNotNone(store.blocked)

            # Writes are held, and say why.
            with self.assertRaises(memory.MemoryError_) as w:
                store.note('writer', 'decision', 'after the block')
            self.assertEqual(w.exception.code, 'storage_blocked')

            # The operations the error promises remain available really are.
            self.assertTrue(service.command(dict(op='hello'), 4242)['blocked'])
            self.assertIsNotNone(service.command(dict(op='status'), 4242)['blocked'])
            self.assertIn('entries', service.command(dict(op='recall', query='decision'), 4242))

            # Recovery is explicit, and it works.
            self.assertTrue(service.command(dict(op='recover'), 4242)['recovered'])
            self.assertIsNone(store.blocked)
            store.db.execute('UPDATE entries SET expires=? WHERE seq IN (%s)'
                             % ','.join(str(x) for x in written[:-3]), (time.time() - 1,))
            store.db.commit()
            store.reclaim()
            self.assertTrue(store.note('writer', 'decision', 'writes resume after recovery'))

    def test_a_rebuild_that_cannot_fit_still_opens_the_store_in_scan_mode(self):
        """A near-full store written without the index must open when it gains one.

        Marking the index invalid on a store that is already running tests only which path
        search picks. This drives the recovery that matters: the rebuild happens inside
        `Store.__init__`, so a capacity failure there used to close the handle and stop the
        service starting, and the fallback could never be reached because nothing was
        serving.
        """
        ceiling = 700
        path = self.home/'fallback.sqlite3'
        with patch.object(memory, 'MAX_PAGES', ceiling), \
             patch.object(memory, 'ORDINARY_MAX_PAGES', ceiling - 40), \
             patch.object(memory, 'MAX_ENTRIES', 100_000), \
             patch.object(memory, 'MAX_LOGICAL_BYTES', 1 << 40):
            # Written by a runtime with no FTS at all, so nothing is indexed.
            store = memory.Store(path, REPO, fts=False)
            words = [f'w{i:05d}' for i in range(4000)]
            rng = random.Random(3)
            needle = 'zqxjkvneedle'
            for i in range(20_000):
                body = ' '.join(rng.choice(words) for _ in range(400))
                if i == 0:
                    body = needle + ' ' + body
                try:
                    store.note('writer', 'decision', body)
                except memory.MemoryError_:
                    break
            self.assertFalse(store.index_usable())
            store.close()

            # Reopened by a runtime that has FTS, with no room to build the index.
            with patch.object(memory, 'REBUILD_HEADROOM', 10_000):
                reopened = memory.Store(path, REPO)
                self.addCleanup(reopened.close)
            # The store opened, which is the point.
            self.assertFalse(reopened.index_usable(),
                             'an index that could not be built must stay invalid')
            self.assertIsNone(reopened.blocked)
            service = memory.Service(self.home, REPO, reopened)
            found = service.command(dict(op='recall', query=needle), 4242)
            self.assertFalse(found['indexed'], 'the reply must say the scan answered')
            self.assertEqual(len(found['entries']), 1,
                             'the fallback scan must be complete, not empty')


class InitialisationBoundaryTests(unittest.TestCase):
    """Initialisation is not exempt from the bound, and must not leave a half-store."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.home = Path(self.tmp.name)
        self.path = self.home/'memory.sqlite3'

    def write_schema_only(self, path, extra=()):
        """A store carrying the schema but no identity, with the pragmas the runtime needs.

        Without those pragmas `configure` refuses first and the test would be measuring the
        readback rather than the state it means to set up.
        """
        db = sqlite3.connect(path, isolation_level=None)
        db.execute('PRAGMA auto_vacuum=INCREMENTAL')
        db.execute('PRAGMA page_size=4096')
        db.execute('PRAGMA journal_mode=WAL')
        for statement in memory.SCHEMA_STATEMENTS:
            db.execute(statement)
        for statement in extra:
            db.execute(statement)
        db.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchall()
        db.close()

    def tables(self, path):
        db = sqlite3.connect(path)
        try:
            return sorted(r[0] for r in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"))
        finally:
            db.close()

    def test_the_schema_statements_are_one_transaction(self):
        """`executescript` would commit each statement separately.

        Demonstrated directly rather than asserted: a script that fails part way leaves the
        tables its earlier statements created, which is what made initialisation several
        unreset transactions instead of one.
        """
        loose = self.home/'loose.sqlite3'
        db = sqlite3.connect(loose)
        self.addCleanup(db.close)
        with self.assertRaises(sqlite3.Error):
            db.executescript('CREATE TABLE a(x);\nCREATE TABLE b(x);\nNOT SQL AT ALL;\n')
        self.assertEqual(self.tables(loose), ['a', 'b'],
                         'a failed script left no tables, so this premise needs revisiting')

        # The store's own initialisation must instead be atomic.
        real = self.home/'atomic.sqlite3'
        original = memory.Store.set_meta

        def fail_on_schema_key(self_, key, value):
            if key == 'schema':
                raise RuntimeError('interrupted part way through initialisation')
            return original(self_, key, value)

        with patch.object(memory.Store, 'set_meta', fail_on_schema_key):
            with self.assertRaises(RuntimeError):
                memory.Store(real, REPO)
        self.assertEqual(self.tables(real), [],
                         'an interrupted initialisation left tables behind')

        # Rolling back is only half of it. The pragmas applied at open write a database
        # header, so the file is left nonempty with no tables, and `inspect` read that as a
        # foreign store: the rollback was clean and the store could still never be reopened.
        self.assertGreater(real.stat().st_size, 0, 'the premise is a nonempty file')
        store = memory.Store(real, REPO)
        self.addCleanup(store.close)
        self.assertEqual(store.meta('repo'), REPO)
        self.assertTrue(store.note('writer', 'finding', 'usable after a rolled-back start'))

    def prepared(self, name, statements, auto_vacuum=True):
        """A database in a stated shape, with the pragmas a real store would carry."""
        path = self.home/name
        db = sqlite3.connect(path, isolation_level=None)
        if auto_vacuum:
            db.execute('PRAGMA auto_vacuum=INCREMENTAL')
        db.execute('PRAGMA page_size=4096')
        db.execute('PRAGMA journal_mode=WAL')
        for statement in statements:
            db.execute(statement)
        db.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchall()
        db.close()
        return path

    def refused(self, path, code='incompatible_store'):
        before = path.read_bytes()
        with self.assertRaises(memory.MemoryError_) as e:
            memory.Store(path, REPO)
        self.assertEqual(e.exception.code, code)
        self.assertEqual(path.read_bytes(), before,
                         'a file this store refuses must be left byte for byte alone')
        return e.exception

    def test_a_search_prefixed_table_is_not_evidence_of_an_index(self):
        """A prefix cannot prove a table is an FTS5 shadow.

        `search_history` is an ordinary table belonging to something else, and excluding
        every `search`-prefixed name from the check let a foreign database be adopted.
        """
        path = self.prepared('history.sqlite3', [
            'CREATE TABLE search_history(body TEXT)',
            "INSERT INTO search_history VALUES('someone else browsing history')"])
        self.assertIn('search_history', str(self.refused(path)))

    def test_a_shadow_table_without_its_virtual_table_is_not_owned(self):
        """The shadow set is admitted only alongside the index that creates it."""
        path = self.prepared('orphan.sqlite3', list(memory.SCHEMA_STATEMENTS) + [
            'CREATE TABLE search_data(id INTEGER PRIMARY KEY, block BLOB)'])
        self.assertIn('search_data', str(self.refused(path)))

    def test_a_table_named_search_that_is_not_the_index_is_not_owned(self):
        path = self.prepared('impostor.sqlite3', list(memory.SCHEMA_STATEMENTS) + [
            'CREATE TABLE search(whatever TEXT)'])
        self.assertIn('search', str(self.refused(path)))

    def test_data_without_identity_is_refused_whether_meta_is_empty_or_absent(self):
        """The refusal must not depend on which of the two states the file is in.

        `uninitialised` returned as soon as the metadata table was missing, so the check
        that stops data being adopted ran only when an empty `meta` table happened to exist.
        Dropping the table was enough to have an entry adopted, with head reset to zero.
        """
        entry = ("INSERT INTO entries(seq,ts,type,scope,body,author,revision) "
                 "VALUES(1,1.0,'finding','repo','someone else data','a',1)")
        empty_meta = self.prepared('empty-meta.sqlite3',
                                   list(memory.SCHEMA_STATEMENTS) + [entry])
        absent_meta = self.prepared('absent-meta.sqlite3',
                                    list(memory.SCHEMA_STATEMENTS) + [entry, 'DROP TABLE meta'])
        for label, path in (('empty', empty_meta), ('absent', absent_meta)):
            with self.subTest(meta=label):
                self.assertIn('records no repository identity', str(self.refused(path)))

    def test_a_table_defined_differently_is_a_different_table(self):
        """Names are not evidence; a familiar name with another shape is not ours."""
        path = self.prepared('malformed.sqlite3', ['CREATE TABLE entries(x TEXT)'])
        self.assertIn('differently', str(self.refused(path)))

    def test_an_unfinished_schema_without_data_is_completed(self):
        """Including one that already carries a real search index."""
        for name, extra in (('plain.sqlite3', []),
                            ('indexed.sqlite3', [memory.FTS_TABLE_SQL]),
                            ('no-meta.sqlite3', ['DROP TABLE meta'])):
            with self.subTest(shape=name):
                path = self.prepared(name, list(memory.SCHEMA_STATEMENTS) + extra)
                store = memory.Store(path, REPO)
                self.addCleanup(store.close)
                self.assertEqual(store.meta('repo'), REPO)
                self.assertTrue(store.note('writer', 'finding', 'usable'))

    def test_a_schema_that_cannot_be_read_is_an_error_not_an_empty_file(self):
        """Reading a schema as empty would initialise over whatever is really there.

        Driven with a file SQLite genuinely cannot parse, rather than a substituted error,
        because the point is what this code concludes from a real unreadable database.
        """
        path = self.home/'garbage.sqlite3'
        path.write_bytes(b'this is not a database, it is something else entirely' * 200)
        before = path.read_bytes()
        with self.assertRaises(memory.MemoryError_) as e:
            memory.Store(path, REPO)
        self.assertEqual(e.exception.code, 'incompatible_store')
        self.assertIn('could not be read', str(e.exception))
        self.assertEqual(path.read_bytes(), before,
                         'a file that could not be judged must be left alone')


    def read_failure(self, fragment):
        """Make one specific read fail while every other operation stays real SQLite.

        A subclassed connection is used because sqlite3.Connection is immutable and cannot be
        patched, and because the point is a failure of a data or metadata page read rather
        than of the catalog read, which the unreadable-file test already covers.
        """
        class Failing(sqlite3.Connection):
            def execute(self_, sql, *args):
                if fragment in sql:
                    raise sqlite3.OperationalError('simulated page read failure')
                return super().execute(sql, *args)

        real_connect = sqlite3.connect

        def connect(path, *args, **kwargs):
            kwargs['factory'] = Failing
            return real_connect(path, *args, **kwargs)

        return patch.object(memory.sqlite3, 'connect', connect)

    def test_data_that_cannot_be_read_is_not_taken_for_an_empty_table(self):
        """Reading the catalog does not prove a table read will succeed.

        The check that refuses identity-free data swallowed every read error and continued as
        though the table were absent, so a file holding a saved entry was adopted: the check
        could not see the data it exists to protect.
        """
        path = self.prepared('unreadable-data.sqlite3', list(memory.SCHEMA_STATEMENTS) + [
            "INSERT INTO entries(seq,ts,type,scope,body,author,revision) "
            "VALUES(1,1.0,'finding','repo','a saved entry','a',1)"])
        before = path.read_bytes()
        with self.read_failure('count(*) FROM entries'):
            with self.assertRaises(memory.MemoryError_) as e:
                memory.Store(path, REPO)
        self.assertEqual(e.exception.code, 'incompatible_store')
        self.assertIn('could not be read', str(e.exception))
        self.assertEqual(path.read_bytes(), before,
                         'no identity or pragma may be written to a file that was refused')
        db = sqlite3.connect(path)
        self.addCleanup(db.close)
        self.assertIsNone(db.execute("SELECT value FROM meta WHERE key='repo'").fetchone(),
                          'an identity was written over a file this store cannot judge')
        self.assertEqual(db.execute('SELECT count(*) FROM entries').fetchone()[0], 1)

    def test_an_identity_that_cannot_be_read_is_not_taken_for_an_absent_one(self):
        """This is how one repository's store could be re-identified as another's.

        An unreadable metadata table looked exactly like an absent one, so a store belonging
        to another repository, holding no entries to trip the data check, was classified as an
        unfinished start and had this repository's identity written over it.
        """
        path = self.home/'other-repo.sqlite3'
        owner = memory.Store(path, 'a-different-repository')
        owner.close()
        before = path.read_bytes()
        with self.read_failure('FROM meta WHERE key IN'):
            with self.assertRaises(memory.MemoryError_) as e:
                memory.Store(path, REPO)
        self.assertEqual(e.exception.code, 'incompatible_store')
        self.assertIn('identity is unknown', str(e.exception))
        self.assertEqual(path.read_bytes(), before)
        db = sqlite3.connect(path)
        self.addCleanup(db.close)
        self.assertEqual(
            db.execute("SELECT value FROM meta WHERE key='repo'").fetchone()[0],
            'a-different-repository',
            'the other repository\'s identity was overwritten')

    def test_a_table_the_catalog_does_not_list_is_genuinely_absent(self):
        """Absence still has to be usable, or every unfinished store would be refused.

        The rule is that only the catalog proves absence, so a schema missing some of its
        tables must still classify as unfinished rather than as unreadable.
        """
        statements = [st for st in memory.SCHEMA_STATEMENTS
                      if 'snapshot_items' not in st and 'entries_live' not in st]
        path = self.prepared('partial.sqlite3', statements)
        store = memory.Store(path, REPO)
        self.addCleanup(store.close)
        self.assertEqual(store.meta('repo'), REPO)
        self.assertTrue(store.note('writer', 'finding', 'completed from a partial schema'))

    def test_another_application_database_is_never_adopted(self):
        """Completing initialisation must not be a way to take over any nonempty file.

        The rule that lets an unfinished start be finished is "no tables this store does not
        own", not "no metadata", so a database belonging to something else stays refused.
        """
        foreign = self.home/'foreign.sqlite3'
        db = sqlite3.connect(foreign, isolation_level=None)
        db.execute('PRAGMA auto_vacuum=INCREMENTAL')
        db.execute('PRAGMA page_size=4096')
        db.execute('PRAGMA journal_mode=WAL')
        db.execute('CREATE TABLE customers(id INTEGER PRIMARY KEY, name TEXT)')
        db.execute("INSERT INTO customers(name) VALUES('someone else')")
        db.close()
        with self.assertRaises(memory.MemoryError_) as e:
            memory.Store(foreign, REPO)
        self.assertEqual(e.exception.code, 'incompatible_store')
        self.assertIn('customers', str(e.exception))
        db = sqlite3.connect(foreign)
        self.addCleanup(db.close)
        self.assertEqual(db.execute('SELECT count(*) FROM customers').fetchone()[0], 1,
                         'the refusal must leave the other database untouched')

    def test_a_store_with_tables_but_no_identity_completes_initialisation(self):
        """The state an older split initialisation could leave must be recoverable.

        Tables with no identity used to read as belonging to another repository, because
        `inspect` compared a missing value against this repository's own.
        """
        self.write_schema_only(self.path)
        self.assertIn('meta', self.tables(self.path))

        store = memory.Store(self.path, REPO)
        self.addCleanup(store.close)
        self.assertEqual(store.meta('repo'), REPO)
        self.assertEqual(int(store.meta('schema')), memory.SCHEMA)
        self.assertTrue(store.note('writer', 'finding', 'usable after completion'))

    def test_a_store_holding_entries_without_identity_is_refused(self):
        """Completing initialisation there would adopt another store's data."""
        self.write_schema_only(self.path, extra=(
            "INSERT INTO entries(seq,ts,type,scope,body,author,revision) "
            "VALUES(1,1.0,'finding','repo','someone else data','a',1)",))
        with self.assertRaises(memory.MemoryError_) as e:
            memory.Store(self.path, REPO)
        self.assertEqual(e.exception.code, 'incompatible_store')
        self.assertEqual(self.tables(self.path) and True, True)
        db = sqlite3.connect(self.path)
        self.addCleanup(db.close)
        self.assertEqual(db.execute('SELECT count(*) FROM entries').fetchone()[0], 1,
                         'the refusal must leave the data untouched')


class ResetFailureTests(Base):
    """A failed reset, not an engine-full transition, is what these exercise."""

    def failing_log(self, store, row=(0, 400, 0)):
        """Make the checkpoint report a log it did not reset."""
        return patch.object(type(store), 'log_state', lambda _self: row)

    def test_a_failed_reset_is_recorded_and_holds_later_writes(self):
        self.note('before the failure')
        with self.failing_log(self.s):
            with self.assertRaises(memory.MemoryError_) as e:
                self.note('during the failure')
            self.assertEqual(e.exception.code, 'storage_blocked')
        # The state must outlive the condition. Previously reset_log raised without
        # recording anything, because transaction() resets before its own try block, and
        # the next write was accepted as though the reset had succeeded.
        self.assertIsNotNone(self.s.blocked)
        with self.assertRaises(memory.MemoryError_) as e:
            self.note('after the condition cleared')
        self.assertEqual(e.exception.code, 'storage_blocked')

    def test_a_blocked_store_answers_status_and_search(self):
        seq = self.note('findable while blocked')
        with self.failing_log(self.s):
            with self.assertRaises(memory.MemoryError_):
                self.note('blocked')
        self.assertIsNotNone(self.s.blocked)
        self.assertIsNotNone(self.call(op='status')['blocked'])
        self.assertTrue(self.call(op='hello')['blocked'])
        found = self.call(op='recall', query='findable')
        self.assertEqual([e['seq'] for e in found['entries']], [seq])

    def test_a_failed_recovery_keeps_the_block_and_writes_nothing(self):
        self.note('seed')
        with self.failing_log(self.s):
            with self.assertRaises(memory.MemoryError_):
                self.note('blocked')
            pages_before = self.s.pages()
            with self.assertRaises(memory.MemoryError_) as e:
                self.s.recover()
            self.assertEqual(e.exception.code, 'storage_blocked')
            # Recovery is itself a write and obeys the precondition it restores. Reclaiming
            # first would have written through the very unreset log being recovered from.
            self.assertEqual(self.s.pages(), pages_before)
        self.assertIsNotNone(self.s.blocked)

    def test_recovery_clears_the_block_once_the_log_can_be_reset(self):
        self.note('seed')
        with self.failing_log(self.s):
            with self.assertRaises(memory.MemoryError_):
                self.note('blocked')
        result = self.s.recover()
        self.assertTrue(result['recovered'])
        self.assertIsNone(self.s.blocked)
        self.assertTrue(self.note('writes resume after recovery'))


class ScanModeExpiryTests(Base):
    """Expiry on a store that is deliberately serving scans, across several batches."""

    def expiring(self, count, prefix='scan'):
        seqs = []
        for i in range(count):
            seqs.append(self.note(f'{prefix} body {i} ' + str(i) * 30,
                                  kind='finding', expires=time.time() - 1))
        return seqs

    def test_a_scan_mode_store_expires_across_batches_without_touching_the_index(self):
        """Rows written while the index was invalid are not in it, so must not be deleted
        from it. A contentless FTS5 delete for a posting that was never added is wrong, and
        the index is already known wrong, so there is nothing to maintain either way."""
        with patch.object(memory, 'EXPIRY_BATCH', 5):
            self.note('written while the index was still valid')
            # Put the store into scan mode, as a runtime without FTS would leave it.
            with self.s.transaction():
                self.s.set_meta('indexed_through', -1)
            self.assertFalse(self.s.index_usable())
            self.expiring(13)          # more than two batches of five

            unindexed = []
            real = memory.Store._unindex

            def spy(self_, rows):
                unindexed.append(len(rows))
                return real(self_, rows)

            with patch.object(memory.Store, '_unindex', spy):
                removed = self.s.expire()
            self.assertEqual(removed, 13, 'every expired row must be removed')
            # `_unindex` may be called, but it must decline to issue deletes in scan mode.
            self.assertEqual(self.s.db.execute(
                'SELECT count(*) FROM entries WHERE expires IS NOT NULL').fetchone()[0], 0)
            self.assertFalse(self.s.index_usable(), 'scan mode must survive expiry')
            # Search still answers completely, from the scan.
            found = self.call(op='recall', query='still valid')
            self.assertFalse(found['indexed'])

    def test_index_maintenance_that_cannot_fit_invalidates_and_still_removes_the_rows(self):
        """The engine-full case must reach the fallback, not block the store.

        `expire` ran its index maintenance with blocking semantics, so an engine refusal set
        a blocked state and raised `storage_blocked`, while the fallback caught only
        `capacity`. The invalidate-and-delete path was therefore skipped at exactly the
        moment it existed for.
        """
        self.expiring(7)
        self.assertTrue(self.s.index_usable())
        real = memory.Store._unindex
        exhausted = []

        def full_on_first_batch(self_, rows):
            real(self_, rows)
            if not exhausted:
                exhausted.append(True)
                # A genuine engine refusal inside the index-maintenance transaction.
                with patch.object(memory, 'MAX_PAGES', self_.pages()):
                    self_.db.execute(f'PRAGMA max_page_count={self_.pages()}')
                    for i in range(50_000):
                        self_.db.execute(
                            'INSERT INTO entries(seq,ts,type,scope,body,author,revision) '
                            'VALUES(?,?,?,?,?,?,?)',
                            (900_000 + i, 1.0, 'finding', 'repo', 'f' * 3000, 'a', 1))

        with patch.object(memory, 'EXPIRY_BATCH', 3), \
             patch.object(memory.Store, '_unindex', full_on_first_batch):
            removed = self.s.expire()
        self.s.db.execute(f'PRAGMA max_page_count={memory.MAX_PAGES}')
        self.assertTrue(exhausted, 'the engine refusal never happened, so nothing was tested')
        self.assertEqual(removed, 7, 'the fallback must still remove every expired row')
        self.assertIsNone(self.s.blocked,
                          'an unaffordable index maintenance must not block the store')
        self.assertFalse(self.s.index_usable(),
                         'the index must be left explicitly invalid, not silently short')
        self.assertEqual(self.s.db.execute(
            'SELECT count(*) FROM entries WHERE expires IS NOT NULL').fetchone()[0], 0)
        # And the service still answers, from the scan.
        self.assertIn('entries', self.call(op='recall', query='scan'))


class CheckpointExceptionTests(Base):
    """A checkpoint that raises has proved nothing, and must be treated as a failed proof."""

    def raising_checkpoint(self, exc):
        def boom(_self):
            raise exc
        return patch.object(memory.Store, 'log_state', boom)

    def test_a_raising_checkpoint_holds_writes_until_recovery(self):
        """Only a returned tuple used to record the block, so an exception escaped it.

        `reset_log` let the exception propagate with `blocked` still unset, so once the
        condition cleared the next write proceeded as though the log had been proved empty
        and nobody had asked for recovery.
        """
        seq = self.note('before the failure')
        with self.raising_checkpoint(sqlite3.OperationalError('disk I/O error')):
            with self.assertRaises(memory.MemoryError_) as e:
                self.note('during the failure')
            self.assertEqual(e.exception.code, 'storage_blocked')
            self.assertIsNotNone(self.s.blocked)
            self.assertIn('OperationalError', self.s.blocked)

            # Reads stay available while writes are held.
            self.assertIsNotNone(self.call(op='status')['blocked'])
            found = self.call(op='recall', query='before')
            self.assertEqual([x['seq'] for x in found['entries']], [seq])

            # Recovery cannot succeed while the checkpoint still raises, and must not write.
            pages_before = self.s.pages()
            with self.assertRaises(memory.MemoryError_) as e:
                self.s.recover()
            self.assertEqual(e.exception.code, 'storage_blocked')
            self.assertEqual(self.s.pages(), pages_before)
            self.assertIsNotNone(self.s.blocked)

        # The condition has cleared, but the block outlives it: writes stay held until
        # recovery is asked for explicitly.
        with self.assertRaises(memory.MemoryError_) as e:
            self.note('after the condition cleared')
        self.assertEqual(e.exception.code, 'storage_blocked')

        self.assertTrue(self.s.recover()['recovered'])
        self.assertIsNone(self.s.blocked)
        self.assertTrue(self.note('writes resume after recovery'))

    def test_only_a_missing_log_proves_a_missing_log(self):
        """A stat that fails for any other reason has not shown the log to be empty."""
        with self.raising_checkpoint(PermissionError('write-ahead log not readable')):
            with self.assertRaises(memory.MemoryError_) as e:
                self.note('while the log cannot be examined')
            self.assertEqual(e.exception.code, 'storage_blocked')
            self.assertIn('PermissionError', self.s.blocked)
        self.assertTrue(self.s.recover()['recovered'])

    def test_an_unreadable_log_file_is_a_failed_proof(self):
        """Drive the `os.stat` branch inside `log_state`, not a substitute for the method.

        Replacing `log_state` and raising from it proves only that the caller handles an
        exception. What must be proved is that `log_state` itself does not read a stat
        failure as an empty log, so only the stat is replaced and the real method runs. If
        `except OSError` returned to that method, the write below would succeed and this
        test would fail.
        """
        self.note('seed')
        wal = str(self.s.path) + '-wal'
        real_stat = os.stat

        def unreadable_log_only(target, *args, **kwargs):
            if str(target) == wal:
                raise PermissionError(f'cannot stat {wal}')
            return real_stat(target, *args, **kwargs)

        with patch.object(memory.os, 'stat', unreadable_log_only):
            with self.assertRaises(memory.MemoryError_) as e:
                self.note('unverifiable')
            self.assertEqual(e.exception.code, 'storage_blocked')
            self.assertIn('PermissionError', self.s.blocked)
            # The reads the block promises are still available while the stat fails.
            self.assertIsNotNone(self.call(op='status')['blocked'])
        self.assertTrue(self.s.recover()['recovered'])
        self.assertTrue(self.note('resumed'))
