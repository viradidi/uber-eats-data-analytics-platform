"""Guard environment-dependent paths and runtime alignment in Azure exports."""
import json
import re
import unittest
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[1]


class WorkspaceConfigurationContractTests(unittest.TestCase):
    def test_silver_bronze_defaults_match_the_adls_linked_service(self):
        linked_service = json.loads(
            (ROOT / "linkedService/AzureDataLakeStorage1.json").read_text(encoding="utf-8")
        )
        storage_url = linked_service["properties"]["typeProperties"]["url"]
        account = urlparse(storage_url).hostname.split(".")[0]
        expected_root = f"abfss://bronze@{account}.dfs.core.windows.net"

        notebook_files = sorted(
            path for path in (ROOT / "notebooks").glob("Silver_*")
            if path.suffix.lower() in {".py", ".ipynb"}
        )
        self.assertTrue(notebook_files, "Silver notebook exports must be present")
        for path in notebook_files:
            with self.subTest(notebook=path.name):
                source = path.read_text(encoding="utf-8")
                if path.suffix.lower() == ".ipynb":
                    notebook = json.loads(source)
                    source = "\\n".join(
                        "".join(cell.get("source", []))
                        for cell in notebook.get("cells", [])
                    )
                self.assertIn(expected_root, source)
                self.assertNotIn("ubereaststorage", source)

    def test_adf_databricks_linked_services_use_supported_lts_runtime(self):
        linked_services = (
            "LS_db_silver.json",
            "ls_api_adb.json",
            "ls_silver_csv.json",
        )
        for file_name in linked_services:
            path = ROOT / "linkedService" / file_name
            service = json.loads(path.read_text(encoding="utf-8"))
            with self.subTest(linked_service=file_name):
                self.assertEqual(
                    service["properties"]["typeProperties"]["newClusterVersion"],
                    "16.4.x-photon-scala2.12",
                )

        requirements = (ROOT / "requirements-test.txt").read_text(encoding="utf-8")
        self.assertRegex(requirements, r"(?m)^pyspark==3\\.5\\.2$")

    def test_csv_dispatcher_does_not_depend_on_a_personal_workspace_path(self):
        source = (ROOT / "routing/dispacher_csv_notebook.py").read_text(encoding="utf-8")
        self.assertNotIn("/Workspace/Users/", source)
        self.assertNotIn("sivana", source.lower())
        self.assertIn('dbutils.widgets.text("notebook_root"', source)
        self.assertIn('dbutils.notebook.run(f"{notebook_root}/{target_notebook}"', source)


if __name__ == "__main__":
    unittest.main()
