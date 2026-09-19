from copy import deepcopy
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import durable_state
import notification_health as health
from notification_journal import COUNTERS, MAX_WORK

OWNER = dict(owner='a' * 32, bridge_pid=11, notifier_pid=12, proc_start='synthetic-start')


def ready_status(**changes):
    journal = dict(imported_through=0, scan_through=1, enumerated_through=1,
                   counters=dict.fromkeys(COUNTERS, 0), pending=0, exhausted=0, uncertain=0,
                   history_lost=False, activation_confirmed=True, pointers_seeded=True)
    journal.update(changes)
    return dict(journal=journal, compatibility=False, memory_available=True)


class HealthTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.file = health.HealthFile(self.root)

    def test_public_result_separates_delivery_health_from_readiness_identity(self):
        self.file.publish(health.snapshot(OWNER, ready_status(), now=1000))
        result = health.read(self.root, OWNER, now=1001)
        self.assertEqual(result['state'], 'healthy')
        self.assertNotIn('owner', result)
        self.assertNotIn('proc_start', result)
        self.assertNotIn('notifier_pid', result)

    def test_missing_stale_future_or_different_generation_is_unknown(self):
        self.assertEqual(health.read(self.root, OWNER, now=1000)['reasons'], ['health_missing'])
        self.file.publish(health.snapshot(OWNER, ready_status(), now=1000))
        for now in (999, 1000 + health.FRESHNESS_MS + 1):
            self.assertEqual(health.read(self.root, OWNER, now=now)['state'], 'unknown')
        for key, replacement in (('owner', 'b' * 32), ('bridge_pid', 13),
                                  ('notifier_pid', 14), ('proc_start', 'other-start')):
            with self.subTest(key=key):
                result = health.read(self.root, dict(OWNER, **{key: replacement}), now=1001)
                self.assertEqual(result['reasons'], ['health_owner_mismatch'])
        self.assertEqual(health.read(self.root, None, now=1001)['state'], 'unknown')

    def test_every_degraded_reason_round_trips_without_raw_error_content(self):
        for reason in health.REASONS:
            with self.subTest(reason=reason):
                self.file.publish(health.snapshot(OWNER, ready_status(), [reason], now=1000))
                result = health.read(self.root, OWNER, now=1000)
                self.assertEqual(result['state'], 'degraded')
                self.assertEqual(result['reasons'], [reason])
        with self.assertRaises(ValueError):
            health.snapshot(OWNER, ready_status(), ['provider returned private text'], now=1000)

    def test_pending_exhaustion_uncertainty_history_and_compatibility_are_visible(self):
        status = ready_status(pending=1, exhausted=2, uncertain=2, history_lost=True)
        status['compatibility'] = True
        value = health.snapshot(OWNER, status, now=1000)
        self.assertEqual(set(value['reasons']), {'pending_delivery', 'exhausted_delivery',
            'uncertain_delivery', 'history_lost', 'version_compatibility'})
        self.assertEqual(health.snapshot(OWNER, ready_status(), now=1001)['state'], 'healthy')

    def test_journal_capacity_cannot_consume_the_health_file_allowance(self):
        value = health.snapshot(OWNER, ready_status(exhausted=MAX_WORK), ['journal_capacity'], now=1000)
        self.file.publish(value)
        result = health.read(self.root, OWNER, now=1000)
        self.assertIn('journal_capacity', result['reasons'])
        self.assertEqual(result['journal']['exhausted'], MAX_WORK)
        self.assertLessEqual(self.file.path.stat().st_size, durable_state.MAX_BYTES)

    def test_publication_failure_never_keeps_an_old_healthy_result_fresh(self):
        self.file.publish(health.snapshot(OWNER, ready_status(), now=1000))
        value = health.snapshot(OWNER, ready_status(pending=1), ['storage_error'], now=2000)
        with mock.patch.object(durable_state, 'publish', side_effect=OSError('synthetic disk full')):
            with self.assertRaises(OSError):
                self.file.publish(value)
        self.assertEqual(health.read(self.root, OWNER, now=1000 + health.FRESHNESS_MS + 1)['state'], 'unknown')
        self.file.publish(value)
        self.assertEqual(health.read(self.root, OWNER, now=2001)['state'], 'degraded')

    def test_unknown_fields_and_arbitrary_counter_text_are_refused(self):
        original = health.snapshot(OWNER, ready_status(), now=1000)
        for mutate in (lambda value: value.update(provider_error='private error'),
                       lambda value: value['journal']['counters'].update(attempts='private text'),
                       lambda value: value.update(proc_start=None)):
            value = deepcopy(original)
            mutate(value)
            with self.assertRaises(ValueError):
                self.file.publish(value)
            durable_state.publish(self.file.path, value)
            self.assertEqual(health.read(self.root, OWNER, now=1000)['state'], 'unknown')

    def test_snapshot_cannot_hide_pending_work_or_exceed_journal_capacity(self):
        value = health.snapshot(OWNER, ready_status(pending=1), now=1000)
        value.update(state='healthy', reasons=[])
        durable_state.publish(self.file.path, value)
        self.assertEqual(health.read(self.root, OWNER, now=1000)['state'], 'unknown')
        with self.assertRaises(ValueError):
            health.snapshot(OWNER, ready_status(pending=MAX_WORK, exhausted=1), now=1000)
