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

    def test_physical_budget_also_refuses(self):
        self.note('one')
        with patch.object(memory,'MAX_PHYSICAL_BYTES',1):
            with self.assertRaises(memory.MemoryError_) as e:
                self.note('two')
            self.assertEqual(e.exception.code,'capacity')
        self.assertEqual(len(self.s.live()), 1)

    def test_idempotency_rows_have_a_finite_horizon(self):
        self.note('kept', key='fresh')
        self.s.db.execute('UPDATE idem SET ts=?', (time.time()-memory.IDEM_TTL-1,))
        self.s.db.commit()
        self.s.reclaim()
        self.assertEqual(self.s.db.execute('SELECT count(*) FROM idem').fetchone()[0], 0)


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

    def test_an_entry_too_large_for_a_page_is_reported_not_silently_dropped(self):
        with patch.object(memory,'FRAME_BUDGET',64):
            self.note('a body that will not fit in a tiny page budget')
            with self.assertRaises(memory.MemoryError_) as e:
                self.call(op='sync', consumer='reader')
            self.assertEqual(e.exception.code, 'entry_too_large')


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

    def spawn(self, *args):
        p = subprocess.Popen([sys.executable, 'memory.py', '--state-dir', str(self.state),
                              '--repo-path', str(self.repo_path), *args],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
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

    def test_crash_between_commit_and_response_keeps_the_write_and_the_retry_is_idempotent(self):
        self.spawn('serve')
        self.wait_for_socket()
        first = self.client(op='note', consumer='cli-1', type='decision',
                            body='committed before the crash', key='k1')
        self.assertTrue(first['ok'])
        seq = first['result']['seq']
        # Kill the process abruptly, as a real crash would, rather than closing the
        # database cleanly. The committed write must survive and the retry must not
        # produce a second entry.
        victim = self.running[-1]
        victim.send_signal(signal.SIGKILL)
        victim.wait(timeout=10)
        platform_support.control_socket_path(self.home).unlink(missing_ok=True)
        self.spawn('serve')
        self.wait_for_socket()
        retry = self.client(op='note', consumer='cli-1', type='decision',
                            body='committed before the crash', key='k1')
        self.assertTrue(retry['result']['duplicate'])
        self.assertEqual(retry['result']['seq'], seq)
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

    def test_consumer_key_is_required_for_stateful_operations(self):
        p = subprocess.run([sys.executable,'memory.py','--state-dir',str(self.state),
                            '--repo-path',str(self.repo_path),'sync'],capture_output=True,text=True)
        self.assertNotEqual(p.returncode, 0)
        self.assertIn('--consumer', p.stderr)


if __name__ == '__main__':
    unittest.main()
