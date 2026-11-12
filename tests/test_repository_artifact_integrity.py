"""Repository-wide integrity checks for committed JSON exports and release docs."""

import json
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class RepositoryArtifactIntegrityTests(unittest.TestCase):
    def test_every_committed_json_file_parses(self):
        json_files = sorted(path for path in ROOT.rglob("*.json") if ".git" not in path.parts and "__pycache__" not in path.parts)
        self.assertTrue(json_files, "Expected committed JSON artifacts to validate")
        failures = []
        for path in json_files:
            try:
                json.loads(path.read_text(encoding="utf-8"))
            except (OSError, UnicodeError, json.JSONDecodeError) as exc:
                failures.append(f"{path.relative_to(ROOT)}: {exc}")
        self.assertEqual([], failures, "Invalid JSON artifacts:\n" + "\n".join(failures))

    def test_text_artifacts_have_no_unresolved_merge_markers(self):
        suffixes = {".md", ".py", ".json", ".yml", ".yaml", ".sql", ".txt", ".toml", ".svg"}
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
                if line.startswith(("<<<<<<< ", ">>>>>>> ")):
                    failures.append(f"{path.relative_to(ROOT)}:{line_number}: {line}")
        self.assertEqual([], failures, "Unresolved merge markers found:\n" + "\n".join(failures))

    def test_adf_export_resource_references_resolve(self):
        def load_exports(directory):
            exports = {}
            for path in sorted((ROOT / directory).glob("*.json")):
                document = json.loads(path.read_text(encoding="utf-8"))
                exports[document["name"]] = (path, document)
            return exports

        linked_services = load_exports("linkedService")
        datasets = load_exports("dataset")
        pipelines = load_exports("pipeline")

        missing = []
        for path, dataset in datasets.values():
            linked_service = dataset.get("properties", {}).get("linkedServiceName", {})
            reference = linked_service.get("referenceName")
            if reference and reference not in linked_services:
                missing.append(f"{path.relative_to(ROOT)} -> linked service {reference}")

        def visit(value, owner_path):
            if isinstance(value, dict):
                reference_type = value.get("type")
                reference_name = value.get("referenceName")
                if reference_name and reference_type == "LinkedServiceReference":
                    if reference_name not in linked_services:
                        missing.append(f"{owner_path} -> linked service {reference_name}")
                elif reference_name and reference_type == "DatasetReference":
                    if reference_name not in datasets:
                        missing.append(f"{owner_path} -> dataset {reference_name}")
                elif reference_name and reference_type == "PipelineReference":
                    if reference_name not in pipelines:
                        missing.append(f"{owner_path} -> child pipeline {reference_name}")
                for child in value.values():
                    visit(child, owner_path)
            elif isinstance(value, list):
                for child in value:
                    visit(child, owner_path)

        for path, pipeline in pipelines.values():
            visit(pipeline.get("properties", {}), str(path.relative_to(ROOT)))

        self.assertEqual([], missing, "Unresolved ADF resource references:\\n" + "\\n".join(missing))

    def test_workflow_parses_deployment_powershell_scripts(self):
        workflow = (ROOT / ".github/workflows/validate.yml").read_text(encoding="utf-8")
        self.assertIn("Parse PowerShell deployment scripts", workflow)
        self.assertIn("shell: pwsh", workflow)
        self.assertIn("[System.Management.Automation.Language.Parser]::ParseFile", workflow)
        self.assertIn("Get-ChildItem -Path deployment -Filter", workflow)

    def test_adf_deploy_script_reads_back_factory_and_each_child_resource(self):
        script = (
            ROOT / "deployment/azure/deploy_adf_resources.ps1"
        ).read_text(encoding="utf-8")
        self.assertIn(
            "$factoryResponse = Invoke-AzRestMethod -Path $factoryPath -Method GET",
            script,
        )
        self.assertIn(
            'Invoke-AdfGet -RelativePath "linkedservices/$([uri]::EscapeDataString($resource.name))"',
            script,
        )
        self.assertIn(
            'Invoke-AdfGet -RelativePath "datasets/$([uri]::EscapeDataString($resource.name))"',
            script,
        )
        self.assertIn(
            'Invoke-AdfGet -RelativePath "pipelines/$([uri]::EscapeDataString($resource.name))"',
            script,
        )
        self.assertIn("WhatIf completed. No Azure resources were written", script)

    def test_readme_links_the_release_gate(self):
        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        self.assertTrue((ROOT / "docs" / "PROJECT_RELEASE_GATE.md").is_file())
        self.assertIn("PROJECT_RELEASE_GATE.md", readme)

    def test_release_gate_records_completed_deployment_acceptance(self):
        gate = (ROOT / "docs" / "PROJECT_RELEASE_GATE.md").read_text(encoding="utf-8").lower()
        for phrase in (
            "deployed and executed end to end",
            "successfully deployed and executed end to end",
            "135 tests",
            "continuously hosted service",
            "no third-party audit",
        ):
            self.assertIn(phrase, gate)
        record = (ROOT / "docs" / "DEPLOYMENT_ACCEPTANCE_RECORD.md").read_text(encoding="utf-8").lower()
        self.assertIn("successfully deployed and executed end to end", record)
        self.assertIn("technical boundaries", record)
        self.assertNotIn("owner-confirmed", record)
        self.assertNotIn("screenshots", record)


if __name__ == "__main__":
    unittest.main()
