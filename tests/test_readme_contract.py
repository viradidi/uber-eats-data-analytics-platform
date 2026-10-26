"""Repository documentation checks that run without cloud credentials."""
import re
import unittest
from pathlib import Path
from urllib.parse import unquote

ROOT = Path(__file__).resolve().parents[1]
README = ROOT / "README.md"


class ReadmeContractTests(unittest.TestCase):
    def test_local_markdown_links_resolve_to_repository_paths(self):
        content = README.read_text(encoding="utf-8")
        missing = []
        # Only inspect ordinary Markdown links/images; external URLs and anchors are skipped.
        for target in re.findall(r"!??\[[^\]]*\]\(([^)]+)\)", content):
            target = target.strip().split()[0].strip("<>")
            if not target or target.startswith(("https://", "http://", "mailto:", "#")):
                continue
            path_part = unquote(target.split("#", 1)[0])
            if not path_part:
                continue
            resolved = (ROOT / path_part).resolve()
            if not resolved.is_relative_to(ROOT.resolve()) or not resolved.exists():
                missing.append(target)
        self.assertEqual([], missing, "Broken local README links: " + ", ".join(missing))

    def test_roadmap_does_not_list_already_committed_controls_as_future_work(self):
        content = README.read_text(encoding="utf-8")
        self.assertIn("## Remaining Engineering Work", content)
        self.assertNotIn("- Automated data-quality and schema validation tests.", content)
        self.assertNotIn("- Source-to-target reconciliation reports.", content)
        self.assertIn("do not claim live Azure or Databricks execution", content)


if __name__ == "__main__":
    unittest.main()
