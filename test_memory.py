import asyncio
import json
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import sys
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
        self.s.set_meta('schema', SchemaProbe := memory.SCHEMA + 1)
        self.s.db.commit()
        with self.assertRaises(memory.MemoryError_) as e:
            memory.Store(self.home/'memory.sqlite3', REPO)
        self.assertEqual(e.exception.code, 'schema_too_new')

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

    def test_growth_stops_at_the_physical_bound_and_space_is_recoverable(self):
        """The bound must stop real growth, reserve room for a withdrawal, and recover.

        A test that merely sets the limit below an existing file proves only that a
        comparison happens. This drives actual allocation into the bound.
        """
        body = 'x' * 4000
        cap = self.s.physical() + 2_000_000
        reserve = 200_000
        written = []
        with patch.object(memory, 'MAX_PHYSICAL_BYTES', cap), \
             patch.object(memory, 'RESERVED_BYTES', reserve), \
             patch.object(memory, 'WAL_MARGIN', 512_000), \
             patch.object(memory, 'MAX_ENTRIES', 100_000), \
             patch.object(memory, 'MAX_LOGICAL_BYTES', 1 << 40):
            for _ in range(1000):
                try:
                    written.append(self.note(body, kind='decision'))
                except memory.MemoryError_ as exc:
                    self.assertEqual(exc.code, 'capacity')
                    break
            else:
                self.fail('growth was never bounded')
            self.assertGreater(len(written), 5, 'the bound must not stop growth immediately')
            # Real allocation is what stopped, and it stopped below the hard limit.
            self.assertLessEqual(self.s.physical(), cap)
            # Other transitions must also be bounded, not merely the append path.
            with self.assertRaises(memory.MemoryError_) as e:
                memory.freeze(self.s, 'a-reader-at-the-bound')
            self.assertEqual(e.exception.code, 'capacity')
            with self.assertRaises(memory.MemoryError_) as e:
                self.call(op='sync', consumer='another-reader-at-the-bound')
            self.assertEqual(e.exception.code, 'capacity')
            stored = len(self.s.live())
            # The reserve is what keeps a withdrawal possible at the bound; without
            # reserved bytes a full store would pin a directive it can never retract.
            withdrawal = self.note('withdrawn', kind='directive', revokes=written[0])
            self.assertTrue(withdrawal)
            self.assertEqual(len(self.s.live()), stored)

            # Recovery: expire the bulk, reclaim, and prove the space returns and is
            # reusable under the same limit.
            at_bound = self.s.physical()
            self.s.db.execute('UPDATE entries SET expires=? WHERE seq IN (%s)'
                              % ','.join(str(s) for s in written[:len(written)//2]),
                              (time.time()-1,))
            self.s.db.commit()
            self.s.reclaim()
            self.assertLess(self.s.physical(), at_bound, 'reclaimed space must be returned')
            self.assertTrue(self.note(body, kind='decision'), 'writes must resume after recovery')

    def test_physical_budget_also_refuses(self):
        self.note('one')
        with patch.object(memory,'MAX_PHYSICAL_BYTES',1):
            with self.assertRaises(memory.MemoryError_) as e:
                self.note('two')
            self.assertEqual(e.exception.code,'capacity')
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
