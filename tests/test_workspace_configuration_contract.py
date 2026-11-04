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

    def test_restore_configuration_script_updates_all_environment_bound_adf_services(self):
        script = (
            ROOT / "deployment/azure/configure_restore_environment.ps1"
        ).read_text(encoding="utf-8")
        for token in (
            "StorageAccountName",
            "SourceSqlServerFqdn",
            "SourceSqlDatabase",
            "ControlSqlServerFqdn",
            "ControlSqlDatabase",
            "DatabricksWorkspaceHost",
            "DatabricksWorkspaceResourceId",
            "KeyVaultUri",
            "ApiBaseUrl",
            "linkedService/AzureDataLakeStorage1.json",
            "linkedService/ls_sql_database.json",
            "linkedService/ls_sql_control.json",
            "linkedService/LS_db_silver.json",
            "linkedService/ls_api_adb.json",
            "linkedService/ls_silver_csv.json",
            "linkedService/LS_restapi.json",
            "linkedService/LS_AzureKeyVault.json",
            "pipeline/api_src_inital load.json",
            "api-version=7.5",
            "pipeline().globalParameters.api_base_url",
            "pipeline().globalParameters.key_vault_uri",
            "secureOutput = $true",
            "secretReference.Remove(\"secretVersion\")",
            "git diff",
        ):
            with self.subTest(token=token):
                self.assertIn(token, script)

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
        self.assertEqual(
            "abfss://bronze@ubereatsrisstorage.dfs.core.windows.net",
            factory["properties"]["globalParameters"]["bronze_root"]["value"],
        )
        self.assertEqual(
            "https://ubereats-mock-api.onrender.com",
            factory["properties"]["globalParameters"]["api_base_url"]["value"],
        )
        self.assertEqual(
            "https://ubereatskeyvalut.vault.azure.net",
            factory["properties"]["globalParameters"]["key_vault_uri"]["value"],
        )

    def test_adf_deploy_script_creates_factory_then_linked_services_datasets_and_pipelines(self):
        script = (ROOT / "deployment/azure/deploy_adf_resources.ps1").read_text(encoding="utf-8")
        for token in (
            "Az.Accounts",
            "Az.Resources",
            "2018-06-01",
            "Invoke-AzRestMethod",
            "Get-AzResourceGroup",
            "-WaitForCompletion",
            "$PSCmdlet.ShouldProcess",
            "linkedservices/",
            "datasets/",
            "pipelines/",
            "uber_eats_master.json",
            "lastPublishTime",
            "WhatIf selected",
        ):
            with self.subTest(token=token):
                self.assertIn(token, script)

        self.assertLess(script.index("Invoke-AzRestMethod -Path $factoryPath"), script.index("$linkedServiceFiles ="))
        self.assertLess(script.index("$linkedServiceFiles ="), script.index("$datasetFiles ="))
        self.assertLess(script.index("$datasetFiles ="), script.index("$pipelineOrder ="))
        self.assertIn("Invoke-AdfGet -RelativePath \"pipelines/uber_eats_master\"", script)
    def test_external_sql_job_export_script_collects_settings_and_workspace_sources_safely(self):
        script = (
            ROOT / "deployment/databricks/export_sql_silver_job.ps1"
        ).read_text(encoding="utf-8")
        for token in (
            "JobId = 121055824417886",
            'jobs", "get"',
            "--page-token",
            "workspace", "get-status",
            "workspace", "export",
            "--format",
            "RAW",
            "SOURCE",
            "secret_value",
            "AccountKey",
            "SharedAccessSignature",
            "potential_secret_matches",
            "external_source_references",
            "child_job_references",
            "Sanitized job definition",
            "This script does not run, edit, or reset the Databricks Job.",
        ):
            with self.subTest(token=token):
                self.assertIn(token, script)

        self.assertNotIn("jobs run-now", script)
        self.assertNotIn("jobs reset", script)
        self.assertIn("export_sql_silver_job.ps1", (
            ROOT / "docs/EXTERNAL_SQL_SILVER_JOB_REVIEW.md"
        ).read_text(encoding="utf-8"))

    def test_workspace_import_script_deploys_exact_adf_notebook_paths_and_src_modules(self):
        script = (
            ROOT / "deployment/databricks/import_workspace_assets.ps1"
        ).read_text(encoding="utf-8")
        for token in (
            "workspace",
            "\"SOURCE\"",
            "\"JUPYTER\"",
            "\"RAW\"",
            "\"--language\"",
            "\"PYTHON\"",
            "Silver_Deliveries.py",
            "Silver_Drivers.py",
            "Silver_Order_Status.py",
            "Silver_Payments.py",
            "Silver_Promotions.py",
            "Silver_Ratings.py",
            "Silver_Zones.ipynb",
            "Gold_Analytics.py",
            "api-dispacher-notebook.py",
            "dispacher_csv_notebook.py",
            "src/common/quality_events.py",
            "src/common/incremental.py",
            "src/gold/transformations.py",
            "$WorkspaceRoot = \"/Shared/UberEats\"",
            "Import-Notebook",
            "Import-PythonWorkspaceFile",
            "Silver_Zones.ipynb",
        ):
            with self.subTest(token=token):
                self.assertIn(token, script)

        master = json.loads(
            (ROOT / "pipeline/uber_eats_master.json").read_text(encoding="utf-8")
        )
        gold_activity = next(
            activity for activity in master["properties"]["activities"]
            if activity["name"] == "Publish_Gold_Analytics"
        )
        self.assertEqual(
            "/Shared/UberEats/notebooks/Gold_Analytics",
            gold_activity["typeProperties"]["notebookPath"],
        )

    def test_silver_and_gold_notebooks_add_shared_workspace_package_root_before_imports(self):
        notebook_paths = (
            "notebooks/Silver_Deliveries.py",
            "notebooks/Silver_Drivers.py",
            "notebooks/Silver_Order_Status.py",
            "notebooks/Silver_Payments.py",
            "notebooks/Silver_Promotions.py",
            "notebooks/Silver_Ratings.py",
            "notebooks/Silver_Zones.ipynb",
            "notebooks/Gold_Analytics.py",
        )
        for relative_path in notebook_paths:
            path = ROOT / relative_path
            source = path.read_text(encoding="utf-8")
            if path.suffix == ".ipynb":
                notebook = json.loads(source)
                source = "\\n".join(
                    "".join(cell.get("source", []))
                    for cell in notebook.get("cells", [])
                    if cell.get("cell_type") == "code"
                )
            bootstrap = 'dbutils.widgets.text("project_root", "/Workspace/Shared/UberEats"'
            self.assertIn(bootstrap, source, relative_path)
            self.assertIn("sys.path.insert(0, PROJECT_ROOT)", source, relative_path)
            first_src_import = source.find("from src.")
            first_sys_insert = source.find("sys.path.insert(0, PROJECT_ROOT)")
            self.assertGreaterEqual(first_src_import, 0, relative_path)
            self.assertLess(first_sys_insert, first_src_import, relative_path)

    def test_api_pipeline_fails_closed_when_no_endpoint_controls_are_active(self):
        pipeline = json.loads(
            (ROOT / "pipeline/api_src_inital load.json").read_text(encoding="utf-8")
        )
        activities = pipeline["properties"]["activities"]
        guard = next(a for a in activities if a["name"] == "Require active API endpoint rows")
        loop = next(a for a in activities if a["name"] == "foreach_api_endpoint")
        expression = guard["typeProperties"]["expression"]["value"]
        self.assertIn("length(activity('lookup_api_control').output.value)", expression)
        self.assertEqual(
            ["Require active API endpoint rows"],
            [item["activity"] for item in loop["dependsOn"]],
        )
        self.assertEqual(
            "API_NO_ACTIVE_ENDPOINTS",
            guard["typeProperties"]["ifFalseActivities"][0]["typeProperties"]["errorCode"],
        )

        child_activities = loop["typeProperties"]["activities"]
        names = {activity["name"] for activity in child_activities}
        self.assertIn("Web_Get_Api_Watermarks", names)
        script = next(a for a in child_activities if a["name"] == "Script_update_api_control")
        watermark_expression = script["typeProperties"]["scripts"][0]["text"]["value"]
        self.assertIn("activity('Web_Get_Api_Watermarks')", watermark_expression)
        self.assertNotIn("activity('Web_Get_API_Watermarks')", watermark_expression)
        watermark_activity = next(a for a in child_activities if a["name"] == "Web_Get_Api_Watermarks")
        key_activity = next(a for a in child_activities if a["name"] == "get_keyvalut_api _key")
        self.assertEqual(
            "@concat(pipeline().globalParameters.api_base_url, '/watermarks')",
            watermark_activity["typeProperties"]["url"]["value"],
        )
        self.assertEqual(
            "@concat(pipeline().globalParameters.key_vault_uri, '/secrets/ubereatsapikey?api-version=7.5')",
            key_activity["typeProperties"]["url"]["value"],
        )
        self.assertTrue(key_activity["policy"]["secureOutput"])
        self.assertTrue(watermark_activity["policy"]["secureInput"])
        rest_linked_service = json.loads(
            (ROOT / "linkedService/LS_restapi.json").read_text(encoding="utf-8")
        )
        self.assertNotIn(
            "secretVersion",
            rest_linked_service["properties"]["typeProperties"]["authHeaders"]["X-API-Key"],
        )

    def test_csv_pipeline_fails_closed_when_metadata_is_empty(self):
        pipeline = json.loads(
            (ROOT / "pipeline/csv_initial load.json").read_text(encoding="utf-8")
        )
        activities = pipeline["properties"]["activities"]
        guard = next(a for a in activities if a["name"] == "Require CSV metadata rows")
        loop = next(a for a in activities if a["name"] == "ForEach1")
        expression = guard["typeProperties"]["expression"]["value"]
        self.assertIn("length(activity('metadata lookup').output.value)", expression)
        self.assertEqual(
            ["Require CSV metadata rows"],
            [item["activity"] for item in loop["dependsOn"]],
        )
        self.assertEqual(
            "CSV_NO_SOURCE_FILES",
            guard["typeProperties"]["ifFalseActivities"][0]["typeProperties"]["errorCode"],
        )

        copy = next(a for a in loop["typeProperties"]["activities"] if a["name"] == "adls csv migration_copy1")
        self.assertEqual(".csv", copy["typeProperties"]["sink"]["formatSettings"]["fileExtension"])

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
            if activity["name"] == "adls csv migration_copy1"
        )
        self.assertEqual(
            "AzureBlobFSReadSettings",
            copy["typeProperties"]["source"]["storeSettings"]["type"],
        )
        job = next(
            activity for activity in loop["typeProperties"]["activities"]
            if activity["name"] == "silver_csv_job_copy1"
        )
        self.assertEqual("DatabricksNotebook", job["type"])
        self.assertEqual(
            "/Shared/UberEats/routing/dispacher_csv_notebook",
            job["typeProperties"]["notebookPath"],
        )
        self.assertIn("file_name", job["typeProperties"]["baseParameters"])
        self.assertIn("bronze_root", job["typeProperties"]["baseParameters"])
        self.assertNotIn("jobId", job["typeProperties"])
        self.assertEqual(
            "@pipeline().globalParameters.bronze_root",
            job["typeProperties"]["baseParameters"]["bronze_root"]["value"],
        )
        self.assertNotIn("connectVia", source_dataset["properties"])
        self.assertFalse((ROOT / "linkedService/FileServer1.json").exists())
        self.assertFalse((ROOT / "integrationRuntime/integrationRuntime2.json").exists())

    def test_api_dispatcher_matches_adf_endpoint_parameter_and_uses_portable_paths(self):
        source_path = ROOT / "routing/api-dispacher-notebook.py"
        source = source_path.read_text(encoding="utf-8")
        self.assertNotIn("/Workspace/Users/", source)
        self.assertIn('dbutils.widgets.text("endpoint_name"', source)
        self.assertIn('{"bronze_root": bronze_root}', source)
        self.assertIn('dbutils.widgets.text("notebook_root"', source)
        self.assertIn('dbutils.widgets.text("notebook_root", "/Shared/UberEats/notebooks"', source)
        for endpoint, notebook in (
            ("payments", "Silver_Payments"),
            ("deliveries", "Silver_Deliveries"),
            ("ratings", "Silver_Ratings"),
            ("order-status", "Silver_Order_Status"),
        ):
            with self.subTest(endpoint=endpoint):
                self.assertIn(f'"{endpoint}": "{notebook}"', source)

        pipeline = json.loads(
            (ROOT / "pipeline/api_src_inital load.json").read_text(encoding="utf-8")
        )
        foreach = next(
            activity for activity in pipeline["properties"]["activities"]
            if activity.get("type") == "ForEach"
        )
        job = next(
            activity for activity in foreach["typeProperties"]["activities"]
            if activity["name"] == "Job_api"
        )
        self.assertIn("endpoint_name", job["typeProperties"]["baseParameters"])
        self.assertEqual(
            "/Shared/UberEats/notebooks",
            job["typeProperties"]["baseParameters"]["notebook_root"],
        )
        self.assertEqual("DatabricksNotebook", job["type"])
        self.assertEqual(
            "/Shared/UberEats/routing/api-dispacher-notebook",
            job["typeProperties"]["notebookPath"],
        )
        self.assertIn("bronze_root", job["typeProperties"]["baseParameters"])
        self.assertEqual(
            "@pipeline().globalParameters.bronze_root",
            job["typeProperties"]["baseParameters"]["bronze_root"]["value"],
        )
        self.assertNotIn("jobId", job["typeProperties"])


    def test_csv_dispatcher_does_not_depend_on_a_personal_workspace_path(self):
        source = (ROOT / "routing/dispacher_csv_notebook.py").read_text(encoding="utf-8")
        self.assertNotIn("/Workspace/Users/", source)
        self.assertNotIn("sivana", source.lower())
        self.assertIn('dbutils.widgets.text("notebook_root"', source)
        self.assertIn('dbutils.notebook.run(f"{notebook_root}/{target_notebook}"', source)


if __name__ == "__main__":
    unittest.main()
