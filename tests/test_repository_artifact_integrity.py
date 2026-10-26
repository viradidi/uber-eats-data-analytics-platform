"""Repository-wide integrity checks for committed JSON exports and release docs."""

import json
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class RepositoryArtifactIntegrityTests(unittest.TestCase):
    def test_every_committed_json_file_parses(self):
        """Catch malformed exported ADF/config JSON before import or review."""
        json_files = sorted(
            path for path in ROOT.rglob("*.json")
            if ".git" not in path.parts and "__pycache__" not in path.parts
        )
        self.assertTrue(json_files, "Expected committed JSON artifacts to validate")
        failures = []
        for path in json_files:
            try:
                json.loads(path.read_text(encoding="utf-8"))
            except (OSError, UnicodeError, json.JSONDecodeError) as exc:
                failures.append(f"{path.relative_to(ROOT)}: {exc}")
        self.assertEqual([], failures, "Invalid JSON artifacts:\\n" + "\\n".join(failures))

    def test_text_artifacts_have_no_unresolved_merge_markers(self):
        """Catch accidental conflict markers in committed project artifacts."""
        suffixes = {".md", ".py", ".json", ".yml", ".yaml", ".sql", ".txt", ".toml", ".svg"}
        markers = ("<<<<<<< ", ">>>>>>> ", "=======")
        failures = []
        for path in ROOT.rglob("*"):
            if not path.is_file() or path.suffix.lower() not in suffixes:
                continue
            if ".git" in path.parts or "__pycache__" in path.parts or ".venv" in path.parts:
                continue
            try:
                lines = path.read_text(encoding="utf-8").splitlines()
            except (OSError, UnicodeError) as exc:
                failures.append(f"{path.relative_to(ROOT)}: could not read: {exc}")
                continue
            for line_number, line in enumerate(lines, start=1):
                if any(line.startswith(marker) for marker in markers):
                    failures.append(f"{path.relative_to(ROOT)}:{line_number}: {line}")
        self.assertEqual([], failures, "Unresolved merge markers found:\\n" + "\\n".join(failures))

    def test_readme_links_the_single_release_gate(self):
        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        release_gate = ROOT / "docs" / "PROJECT_RELEASE_GATE.md"
        self.assertTrue(release_gate.is_file())
        self.assertIn("docs/PROJECT_RELEASE_GATE.md", readme)

    def test_release_gate_distinguishes_static_and_cloud_validation(self):
        gate = (ROOT / "docs" / "PROJECT_RELEASE_GATE.md").read_text(encoding="utf-8")
        for phrase in (
            "live-cloud release is not certified",
            "end-to-end replay safety is not yet proven",
            "No-cost finalization workflow",
            "Do not claim a currently deployed",
        ):
            self.assertIn(phrase, gate)


if __name__ == "__main__":
    unittest.main()