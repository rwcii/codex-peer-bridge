import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import usage_report as report
import usage_sources as sources
import usage_selection as selection

START = '2026-01-01T00:00:00Z'
MID = '2026-01-01T00:00:10Z'
END = '2026-01-01T00:00:20Z'
CODEX = dict(input_tokens=100, cached_input_tokens=30, cache_write_input_tokens=20,
             output_tokens=15, reasoning_output_tokens=5, total_tokens=115)
CLAUDE = dict(input_tokens=50, cache_read_input_tokens=30, cache_creation_input_tokens=20,
              output_tokens=15, output_tokens_details=dict(thinking_tokens=5))


class UsageTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.path = self.root/'session-a.jsonl'
        self.path.touch()
        self.manifest = dict(schema_version=1, agents=[dict(agent_id='agent-a', session_id='session-a',
                    provider='codex', path=str(self.path), role='main')])

    def append(self, row, path=None):
        with (path or self.path).open('a') as stream:
            stream.write(json.dumps(row) + '\n')

    def codex(self, response='response-a', at=MID, usage=None, model='model-a'):
        self.append(dict(type='session_meta', payload=dict(id='session-a', source='cli')))
        self.append(dict(type='turn_context', payload=dict(model=model)))
        self.append(dict(type='token_usage_record', timestamp=at, payload=dict(
            thread_id='session-a', response_id=response, usage=CODEX if usage is None else usage)))

    def claude(self, response='response-a', at=MID, complete=True, usage=None):
        self.manifest['agents'][0]['provider'] = 'claude'
        self.append(dict(type='assistant', sessionId='session-a', timestamp=at,
                         message=dict(id=response, model='model-a', usage=CLAUDE if usage is None else usage,
                                      stop_reason='end_turn' if complete else None)))

    def result(self, **kwargs):
        return report.report(self.manifest, 'work-a', since=START, until=END, **kwargs)

    def test_nonzero_cache_write_and_disjoint_normalization(self):
        self.codex()
        result = self.result()
        self.assertEqual(result['status'], 'complete')
        row = result['rows'][0]
        self.assertEqual([row[k] for k in sources.FIELDS], [50, 10, 20, 30, 5, 115])
        self.assertEqual(row['total'], sum(row[k] for k in sources.COMPONENTS))
        self.assertEqual(row['native_totals']['input_tokens'], 100)

    def test_cumulative_events_not_counted_and_response_duplicates_not_summed(self):
        self.codex()
        self.codex()
        self.append(dict(type='event_msg', payload=dict(type='token_count', total_token_usage=CODEX)))
        row = self.result()['rows'][0]
        self.assertEqual(row['responses'], 1)
        self.assertEqual(row['total'], 115)

    def test_claude_growing_and_identical_rows_latest_wins(self):
        partial = dict(CLAUDE, output_tokens=6)
        self.claude(usage=partial, complete=False, at='2026-01-01T00:00:05Z')
        self.claude()
        self.claude()
        result = self.result()
        self.assertEqual(result['status'], 'complete')
        self.assertEqual(result['rows'][0]['total'], 115)
        self.assertEqual(result['rows'][0]['responses'], 1)

    def test_incomplete_abandoned_response_is_not_complete(self):
        self.claude(complete=False)
        self.assertEqual(self.result()['status'], 'incomplete')

    def test_response_after_window_does_not_erase_earlier_partial(self):
        self.claude(complete=False)
        self.claude(at='2026-01-01T00:00:25Z')
        self.assertEqual(self.result()['status'], 'incomplete')
        self.assertEqual(self.result()['rows'][0]['responses'], 1)

    def test_missing_reasoning_is_not_zero_but_native_total_survives(self):
        self.codex(usage={k: v for k, v in CODEX.items() if k != 'reasoning_output_tokens'})
        row = self.result()['rows'][0]
        self.assertIsNone(row['reasoning'])
        self.assertIsNone(row['tokens_out'])
        self.assertEqual(row['total'], 115)
        self.assertEqual(row['status'], 'incomplete')

    def test_missing_claude_reasoning_preserves_derivable_total(self):
        self.claude(usage={k: v for k, v in CLAUDE.items() if k != 'output_tokens_details'})
        row = self.result()['rows'][0]
        self.assertIsNone(row['reasoning'])
        self.assertEqual(row['total'], 115)

    def test_inconsistent_total_is_retained_and_flagged(self):
        self.codex(usage=dict(CODEX, total_tokens=999))
        result = self.result()
        self.assertEqual(result['status'], 'inconsistent')
        self.assertEqual(result['rows'][0]['total'], 999)

    def test_negative_boolean_and_overlap_rejected(self):
        for change in (dict(input_tokens=-1), dict(output_tokens=True), dict(cached_input_tokens=101)):
            with self.subTest(change=change):
                _, issues = sources.normalize('codex', dict(CODEX, **change))
                self.assertTrue(any(i.startswith(('invalid:', 'inconsistent:')) for i in issues))

    def test_immutable_codex_response_conflict(self):
        self.codex()
        self.codex(usage=dict(CODEX, output_tokens=20, total_tokens=120))
        self.assertEqual(self.result()['status'], 'inconsistent')

    def test_model_changes_preserve_separate_rows(self):
        self.codex()
        self.codex(response='response-b', model='model-b')
        self.assertEqual(len(self.result()['rows']), 2)

    def test_two_agents_same_model_do_not_collapse(self):
        self.codex()
        second = self.root/'second.jsonl'
        second.write_text(self.path.read_text().replace('session-a', 'session-b').replace('\"source\": \"cli\"', '\"source\": {\"subagent\": {}}'))
        self.manifest['agents'].append(dict(self.manifest['agents'][0], agent_id='agent-b',
                                            session_id='session-b', path=str(second), role='subagent'))
        rows = self.result()['rows']
        self.assertEqual([r['agent_id'] for r in rows], ['agent-a', 'agent-b'])
        self.assertEqual([r['role'] for r in rows], ['main', 'subagent'])

    def test_duplicate_selection_and_hardlink_refused(self):
        self.codex()
        self.manifest['agents'].append(dict(self.manifest['agents'][0]))
        with self.assertRaises(ValueError):
            self.result()
        link = self.root/'link.jsonl'
        os.link(self.path, link)
        self.manifest['agents'][1].update(path=str(link), session_id='session-b')
        with self.assertRaises(ValueError):
            self.result()

    def test_wrong_session_malformed_and_partial_lines_are_not_silent(self):
        self.codex()
        with self.path.open('a') as stream:
            stream.write('{bad}\n{"unfinished":')
        self.assertEqual(self.result()['status'], 'inconsistent')
        self.path.write_text(self.path.read_text().replace('session-a', 'wrong-session'))
        self.assertEqual(self.result()['rows'], [])
        self.assertEqual(self.result()['status'], 'inconsistent')

    def test_before_work_marker_excludes_old_response(self):
        self.codex()
        marker = report.begin(self.manifest, 'work-a')
        self.codex(response='response-b', at=END)
        result = report.report(self.manifest, marker=marker, until=END)
        self.assertEqual(result['rows'][0]['responses'], 1)
        self.assertEqual(result['rows'][0]['total'], 115)

    def test_marker_refuses_truncation_and_changed_selection(self):
        self.codex()
        marker = report.begin(self.manifest, 'work-a')
        self.path.write_text('')
        with self.assertRaises(ValueError):
            report.report(self.manifest, marker=marker)
        self.manifest['agents'][0]['role'] = 'subagent'
        with self.assertRaises(ValueError):
            report.report(self.manifest, marker=marker)

    def test_time_window_excludes_response_started_before_boundary(self):
        self.claude(at=START, complete=False)
        self.claude()
        result = self.result()
        self.assertEqual(result['status'], 'incomplete')
        self.assertEqual(result['rows'], [])

    def test_empty_window_not_measured_zero(self):
        result = self.result()
        self.assertEqual(result['rows'], [])
        self.assertEqual(result['status'], 'incomplete')

    def test_unverified_provider_import_is_not_native_support(self):
        for provider in ('normalized', 'deepseek'):
            self.manifest['agents'][0]['provider'] = provider
            with self.assertRaises(ValueError):
                self.result()

    def test_inventory_only_reads_selected_registration_metadata(self):
        state = self.root/'state/sessions/synthetic'
        state.mkdir(parents=True)
        (state/'session.json').write_text(json.dumps(dict(thread='session-a', agent='codex', repo='/repo')))
        value = report.inventory(self.root/'state')
        self.assertEqual(len(value['participants']), 1)
        self.assertEqual(value['participants'][0]['thread'], 'session-a')

    def test_marker_output_private_and_never_overwritten(self):
        path = self.root/'marker.json'
        report.write_new(path, dict(schema_version=1))
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        with self.assertRaises(FileExistsError):
            report.write_new(path, {})

    def test_self_selection_uses_exact_codex_environment_and_lineage(self):
        self.codex()
        home = self.root/'codex'
        target = home/'sessions/2026/01/01/rollout-session-a.jsonl'
        target.parent.mkdir(parents=True)
        target.write_text(self.path.read_text())
        with patch.dict(os.environ, {'CODEX_THREAD_ID': 'session-a', 'CODEX_HOME': str(home)}):
            manifest = selection.self_manifest('codex')
            self.assertEqual(manifest['agents'][0]['path'], str(target))
            self.assertEqual(manifest['agents'][0]['role'], 'main')
        target.write_text(target.read_text().replace('"source": "cli"', '"source": {"subagent": {}}'))
        with patch.dict(os.environ, {'CODEX_THREAD_ID': 'session-a', 'CODEX_HOME': str(home)}):
            self.assertEqual(selection.self_manifest('codex')['agents'][0]['role'], 'subagent')

    def test_claude_self_subagents_honor_config_and_do_not_use_child_env(self):
        home = self.root/'claude'
        project = '/synthetic-project'
        main = home/'projects/-synthetic-project/session-a.jsonl'
        main.parent.mkdir(parents=True)
        self.claude()
        main.write_text(self.path.read_text())
        child = main.parent/'session-a/subagents/agent-child.jsonl'
        child.parent.mkdir(parents=True)
        child.write_text(self.path.read_text())
        with patch.dict(os.environ, {'CLAUDE_CODE_SESSION_ID': 'session-a',
                        'CLAUDE_CONFIG_DIR': str(home), 'CLAUDE_CODE_CHILD_SESSION': '1'}):
            manifest = selection.self_manifest('claude', project=project, include_subagents=True)
            rows = report.report(manifest, 'work-a', since=START, until=END)['rows']
            self.assertEqual([row['role'] for row in rows], ['main', 'subagent'])
            self.assertEqual(len({row['agent_id'] for row in rows}), 2)

    def test_source_role_conflict_is_not_silently_accepted(self):
        self.codex()
        self.manifest['agents'][0]['role'] = 'subagent'
        result = self.result()
        self.assertEqual(result['status'], 'inconsistent')
        self.assertEqual(result['rows'][0]['role'], 'main')

    def test_combine_reads_no_sources_and_rejects_duplicate_agents(self):
        self.codex()
        first = self.result()
        self.path.unlink()
        combined = report.combine([first])
        self.assertEqual(combined['rows'], first['rows'])
        with self.assertRaises(ValueError):
            report.combine([first, first])
        first['rows'][0]['reasoning'] = None
        self.assertEqual(report.combine([first])['status'], 'incomplete')

    def test_table_has_exact_eight_columns(self):
        self.codex()
        table = report.table(self.result())
        self.assertIn('| Model | Role | Tokens In | Tokens Out | Cache Write | Cache Read | Reasoning | Total |', table)
        self.assertIn('Agent: agent-a', table)

    def test_limits_and_boundary_validation(self):
        with self.assertRaises(ValueError):
            sources.timestamp('2026-01-01')
        with self.assertRaises(ValueError):
            report.report(self.manifest, 'work-a', since=END, until=START)
        with patch.object(sources, 'MAX_BYTES', 1):
            self.codex()
            with self.assertRaises(ValueError):
                self.result()


if __name__ == '__main__':
    unittest.main()
