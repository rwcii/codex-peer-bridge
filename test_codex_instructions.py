from pathlib import Path
import tempfile
import unittest
import codex_instructions as guidance

class GuidanceTests(unittest.TestCase):
    def test_preserve_update_remove(self):
        with tempfile.TemporaryDirectory() as d:
            home=Path(d)
            p=home/'AGENTS.md'
            original='Existing personal instructions.\n'
            p.write_text(original)
            guidance.update(home,Path('/one'))
            first=p.read_text()
            guidance.update(home,Path('/one'))
            self.assertEqual(p.read_text(),first)
            guidance.update(home,Path('/two'))
            self.assertEqual(p.read_text().count(guidance.BEGIN),1)
            self.assertNotIn('/one/session.py',p.read_text())
            guidance.update(home,Path('/two'),remove=True)
            self.assertEqual(p.read_text(),original)
            self.assertEqual((home/'AGENTS.md.before-codex-peer-bridge').read_text(),original)

    def test_override_and_malformed(self):
        with tempfile.TemporaryDirectory() as d:
            home=Path(d)
            p=home/'AGENTS.override.md'
            p.write_text('Priority guidance\n')
            self.assertEqual(guidance.update(home,Path('/app')),p)
            self.assertFalse((home/'AGENTS.md').exists())
            p.write_text(guidance.BEGIN+'truncated')
            with self.assertRaises(ValueError):
                guidance.update(home,Path('/app'))
            self.assertEqual(p.read_text(),guidance.BEGIN+'truncated')

    def test_override_transition_cleans_old_section(self):
        with tempfile.TemporaryDirectory() as d:
            home=Path(d)
            (home/'AGENTS.md').write_text('Original base\n')
            guidance.update(home,Path('/app'))
            (home/'AGENTS.override.md').write_text('New override\n')
            guidance.update(home,Path('/app'))
            self.assertEqual((home/'AGENTS.md').read_text(),'Original base\n')
            self.assertIn(guidance.BEGIN,(home/'AGENTS.override.md').read_text())
            guidance.update(home,Path('/app'),remove=True)
            self.assertEqual((home/'AGENTS.override.md').read_text(),'New override\n')

    def test_symlink_refused(self):
        with tempfile.TemporaryDirectory() as d:
            home=Path(d)
            target=home/'real'
            target.write_text('unchanged')
            (home/'AGENTS.md').symlink_to(target)
            with self.assertRaises(ValueError):
                guidance.update(home,Path('/app'))
            self.assertEqual(target.read_text(),'unchanged')
