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

    def test_readme_describes_completed_work_and_validation_boundaries(self):
        content = README.read_text(encoding="utf-8")
        self.assertIn("## Engineering work demonstrated", content)
        self.assertIn("## Validation status and deployment boundary", content)
        self.assertIn("do not, by themselves, execute Azure Data Factory pipelines", content)
        self.assertIn("docs/FINAL_HANDOFF.md", content)
        self.assertIn("docs/PORTFOLIO_EVIDENCE.md", content)


if __name__ == "__main__":
    unittest.main()
