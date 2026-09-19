import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import runtime_names as names


class RuntimeNameTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def test_fresh_selection_does_not_create_state(self):
        self.assertEqual(names.choose_default(self.root), self.root / 'koinon')
        self.assertEqual(list(self.root.iterdir()), [])

    def test_legacy_default_is_reused_without_moving_data(self):
        legacy = self.root / 'codex-peer-bridge'
        legacy.mkdir()
        cursor = legacy / 'synthetic-cursor'
        cursor.write_bytes(b'preserved')
        inode = cursor.stat().st_ino
        self.assertEqual(names.choose_default(self.root), legacy)
        self.assertEqual(cursor.stat().st_ino, inode)
        self.assertEqual(cursor.read_bytes(), b'preserved')
        self.assertFalse((self.root / 'koinon').exists())

    def test_both_defaults_refuse_and_preserve_each_location(self):
        for name in ('koinon', 'codex-peer-bridge'):
            (self.root / name).mkdir()
        with self.assertRaises(names.NameConflict) as raised:
            names.choose_default(self.root)
        self.assertEqual(raised.exception.code, 'ambiguous_default_paths')
        self.assertEqual(len(list(self.root.iterdir())), 2)

    def test_broken_legacy_link_is_not_absent(self):
        legacy = self.root / 'codex-peer-bridge'
        legacy.symlink_to(self.root / 'missing')
        with self.assertRaises(names.NameConflict) as raised:
            names.choose_default(self.root)
        self.assertEqual(raised.exception.code, 'unsafe_default_path')
        self.assertTrue(legacy.is_symlink())
        self.assertFalse((self.root / 'koinon').exists())

    def test_unreadable_probe_does_not_fall_back_to_empty_state(self):
        with mock.patch.object(Path, 'lstat', side_effect=PermissionError('synthetic refusal')):
            with self.assertRaises(names.NameConflict) as raised:
                names.choose_default(self.root)
            self.assertEqual(raised.exception.code, 'default_path_unavailable')

    def test_prefix_and_state_probe_their_own_default_bases(self):
        home, state = self.root / 'home', self.root / 'configured-state'
        with mock.patch.object(Path, 'home', return_value=home), mock.patch.dict(os.environ, XDG_STATE_HOME=str(state)):
            self.assertEqual(names.default_prefix(), home / '.local/share/koinon')
            self.assertEqual(names.default_state_root(), state / 'koinon')
            (state / 'codex-peer-bridge').mkdir(parents=True)
            self.assertEqual(names.default_state_root(), state / 'codex-peer-bridge')
            self.assertFalse(home.exists())

    def test_service_family_is_preserved_and_conflicts_refuse(self):
        for instance in (None, 'a'*16):
            with self.subTest(instance=instance), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                fresh, old = names.service_names(instance), names.service_names(instance, legacy=True)
                self.assertEqual(names.selected_service_names(root, instance), fresh)
                (root / old[0]).write_text(names.LEGACY_SERVICE_MARKER)
                self.assertEqual(names.selected_service_names(root, instance), old)
                (root / fresh[0]).write_text(names.SERVICE_MARKER)
                with self.assertRaises(names.NameConflict) as raised:
                    names.selected_service_names(root, instance)
                self.assertEqual(raised.exception.code, 'ambiguous_service_units')
                self.assertEqual(len(list(root.iterdir())), 2)

    def test_external_identifiers_and_guidance_lock_names_remain_stable(self):
        self.assertEqual(names.REGISTRY_ENTRYPOINT, 'codex-peer-bridge')
        self.assertEqual(names.MEMORY_SERVICE, 'codex-peer-memory')
        self.assertEqual(names.GUIDANCE_LOCK_NAMES,
                         dict(codex='.codex-peer-bridge.lock', deepseek='.deepseek-peer-bridge.lock'))

    def test_configuration_codes_are_closed_and_every_raise_is_classified(self):
        import ast
        tree = ast.parse(Path(names.__file__).read_text())
        raised = {node.args[0].value for node in ast.walk(tree)
                  if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                  and node.func.id == 'NameConflict'}
        self.assertEqual(raised, names.CONFIGURATION_CODES)
        with self.assertRaises(KeyError):
            names.NameConflict('unclassified-new-code', ())
