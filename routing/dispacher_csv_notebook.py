# Databricks notebook source
dbutils.widgets.text("file_name", "")
dbutils.widgets.text("notebook_root", "../notebooks", "Silver notebook folder")
dbutils.widgets.text("bronze_root", "abfss://bronze@ubereatsrisstorage.dfs.core.windows.net", "Bronze storage root")

file_name = dbutils.widgets.get("file_name").strip().lower()
notebook_root = dbutils.widgets.get("notebook_root").strip().rstrip("/")
bronze_root = dbutils.widgets.get("bronze_root").strip().rstrip("/")

notebook_map = {
    "drivers.csv": "Silver_Drivers",
    "promotions.csv": "Silver_Promotions",
    "zones.csv": "Silver_Zones",
}

if file_name not in notebook_map:
    raise ValueError(f"Unsupported CSV source: {file_name!r}")

target_notebook = notebook_map[file_name]
print(f"CSV source received: {file_name}")
print(f"Dispatching to: {notebook_root}/{target_notebook}")
dbutils.notebook.run(
    f"{notebook_root}/{target_notebook}",
    0,
    {"bronze_root": bronze_root},
)
