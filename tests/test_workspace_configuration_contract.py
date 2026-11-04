"""Guard environment-dependent paths and runtime alignment in Azure exports."""
import json
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
                    source = "\n".join(
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
        self.assertRegex(requirements, r"(?m)^pyspark==3\.5\.2$")

    def test_restore_staging_scripts_are_repeatable_and_verify_inputs(self):
        adls_script = (ROOT / "deployment/azure/stage_bronze_metadata.ps1").read_text(encoding="utf-8")
        self.assertIn("api_metadata_v2 (2).json", adls_script)
        self.assertIn("csv_metadata_proper (2).json", adls_script)
        self.assertIn("az storage fs file upload", adls_script)
        self.assertIn("--auth-mode login", adls_script)
        self.assertIn("az storage fs file exists", adls_script)

        csv_script = (ROOT / "deployment/azure/stage_csv_files.ps1").read_text(encoding="utf-8")
        for file_name in ("drivers.csv", "promotions.csv", "zones.csv"):
            self.assertIn(file_name, csv_script)
        self.assertIn("Get-FileHash", csv_script)

    def test_factory_export_does_not_pin_historical_managed_identity_ids(self):
        factory = json.loads(
            (ROOT / "factory/ubereatssri.json").read_text(encoding="utf-8")
        )
        self.assertEqual("SystemAssigned", factory["identity"]["type"])
        self.assertNotIn("principalId", factory["identity"])
        self.assertNotIn("tenantId", factory["identity"])

    def test_csv_pipeline_reads_staged_adls_source_without_self_hosted_runtime(self):
        source_dataset = json.loads(
            (ROOT / "dataset/DS_source_csv.json").read_text(encoding="utf-8")
        )
        self.assertEqual(
            "AzureDataLakeStorage1",
            source_dataset["properties"]["linkedServiceName"]["referenceName"],
        )
        location = source_dataset["properties"]["typeProperties"]["location"]
        self.assertEqual("AzureBlobFSLocation", location["type"])
        self.assertEqual("bronze", location["fileSystem"])
        self.assertEqual("csv_source", location["folderPath"])

        pipeline = json.loads(
            (ROOT / "pipeline/csv_initial load.json").read_text(encoding="utf-8")
        )
        loop = next(
            activity for activity in pipeline["properties"]["activities"]
            if activity.get("type") == "ForEach"
        )
        copy = next(
            activity for activity in loop["typeProperties"]["activities"]
            if activity["name"] == "onpremise migration_copy1"
        )
        self.assertEqual(
            "AzureBlobFSReadSettings",
            copy["typeProperties"]["source"]["storeSettings"]["type"],
        )
        self.assertNotIn("connectVia", source_dataset["properties"])
        self.assertFalse((ROOT / "linkedService/FileServer1.json").exists())
        self.assertFalse((ROOT / "integrationRuntime/integrationRuntime2.json").exists())

    def test_csv_dispatcher_does_not_depend_on_a_personal_workspace_path(self):
        source = (ROOT / "routing/dispacher_csv_notebook.py").read_text(encoding="utf-8")
        self.assertNotIn("/Workspace/Users/", source)
        self.assertNotIn("sivana", source.lower())
        self.assertIn('dbutils.widgets.text("notebook_root"', source)
        self.assertIn('dbutils.notebook.run(f"{notebook_root}/{target_notebook}"', source)


if __name__ == "__main__":
    unittest.main()
